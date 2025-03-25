package local_distance_blocking

import (
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking/blocking_conf"
	"gitlab.grandhoo.com/rock/storage/storage2/database/sql"
	"gitlab.grandhoo.com/rock/storage/storage2/database/table/line"
	"gitlab.grandhoo.com/rock/storage/storage2/rock_db"
	"runtime"
	"sync"
)

/**
依次遍历元素进行分组
暂时只能在主节点运行

暂时无用 -- 2024年1月17日
*/

type GroupId = blocking_conf.TokenId

const unKnownGroupId GroupId = -1
const emptyValueGroup GroupId = -2 // 空值分入 -2 组

// GroupBy 依次一列的所有元组进行分组
// 返回的 []int32 就是行号到组号的映射
// 文件序列化到 {tab}.{col}.{sid}.ldb
func GroupBy(tableName, columnName string, similar func(string, string) float64, threshold float64) error {
	var SQL = sql.SingleTable.SELECT(columnName).FROM(tableName)
	lines, err := rock_db.DB.Query(SQL)
	if err != nil {
		logger.Error(err)
		return err
	}
	values := mapping(lines, func(e line.Line) string {
		value := e[0]
		return toString(value)
	})
	groups := doGroupBy(values, similar, threshold)

	shardId := int64(0)
	startRowId := 0
	for startRowId < len(groups) {
		endRowId := startRowId + blocking_conf.FileRowSize
		if endRowId > len(groups) {
			endRowId = len(groups)
		}
		// []id 转为 [][]id
		var tokenIdsList = make([][]blocking_conf.TokenId, endRowId-startRowId)
		for i, id := range groups[startRowId:endRowId] {
			tokenIdsList[i] = []blocking_conf.TokenId{id, id}
		}

		path := blocking_conf.FilePath(tableName, []string{columnName}, shardId, blocking_conf.LocalDistanceBlockingFileSuffix)
		err = blocking_conf.WriteDerivedColumn(path, tokenIdsList)
		if err != nil {
			logger.Error(err)
			return err
		}

		startRowId = endRowId
		shardId++
	}
	return nil
}

func doGroupBy(values []string, similar func(string, string) float64, threshold float64) (groups []GroupId) {
	var size = len(values)
	groups = make([]GroupId, size)
	for i := range groups {
		if values[i] == "" {
			groups[i] = emptyValueGroup // 空值预先分组
		} else {
			groups[i] = unKnownGroupId // 非空值暂时放入 unKnownGroupId 组
		}
	}

	var locker sync.Mutex
	for i := 0; i < size; i++ {
		var coreGroupId GroupId
		locker.Lock() // 可见性
		coreGroupId = groups[i]
		locker.Unlock()

		if coreGroupId == emptyValueGroup { // 空值已分组
			continue
		}

		if coreGroupId == unKnownGroupId {
			coreValue := values[i]
			coreGroupId = int32(i) // 自己没有被分入任何组，那组号就是 i
			groups[i] = coreGroupId
			// 笛卡尔积！！
			parallel(i+1, size, func(k int) {
				if groups[k] == unKnownGroupId {
					if similar(coreValue, values[k]) > threshold {
						groups[k] = coreGroupId
					}
				}
			})
			//for j := i + 1; j < size; j++ {
			//	other := values[j]
			//	if groups[k] == unKnownGroupId && similar(coreValue, other) > threshold {
			//		groups[j] = coreGroupId
			//	}
			//}
		}
	}

	return groups
}

func mapping[E, R any](es []E, converter func(E) R) []R {
	var rs = make([]R, len(es))
	for i, e := range es {
		rs[i] = converter(e)
	}
	return rs
}

func toString(i any) string {
	if i == nil {
		return ""
	}
	if s, ok := i.(string); ok {
		return s
	}
	if s, ok := i.(fmt.Stringer); ok {
		return s.String()
	}
	return fmt.Sprintf("%v", i)
}

func parallel(start, end int, f func(int)) {
	number := end - start
	if number < 50 {
		for i := start; i < end; i++ {
			f(i)
		}
		return
	}

	degree := runtime.NumCPU() + 1
	batchSize := number/degree + 1

	wg := sync.WaitGroup{}
	for i := 0; i < degree; i++ {
		batchStart := start + i*batchSize
		batchEnd := start + (i+1)*batchSize
		if batchEnd > end {
			batchEnd = end
		}
		//println(batchStart, batchEnd)
		wg.Add(1)
		go func(batchStart, batchEnd int) {
			defer wg.Done()
			for k := batchStart; k < batchEnd; k++ {
				f(k)
			}
		}(batchStart, batchEnd)
	}
	wg.Wait()
}
