package lsh_blocking

import (
	"bufio"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking/blocking_conf"
	"gitlab.grandhoo.com/rock/storage/storage2/rock_db"
	"os"
	"sort"
	"strconv"
	"strings"
)

/**
提前存在向量信息，即一个字段的所有单元格都变成了等长向量 []float64
 name    vectorsList
 apple   [5, 6, 4, 7]
 aim     [2, 3, 4, 5]
...

同一维度的向量拆分，成为之“列”
 name    c1   c2   c3   c4
 apple   5    6    4    7
 aim     2    3    4    5
...

对于一“列”，相同的行放一组
*/

func Blocking(leftTableName string, leftColumnNames []string, leftVectorsFile string,
	rightTableName string, rightColumnNames []string, rightVectorsFile string) error {

	{
		path := blocking_conf.FilePath(leftTableName, leftColumnNames, 0, blocking_conf.LSHBlockingFileSuffix)
		exist, err := rock_db.DFS.Exist(path)
		if err != nil {
			logger.Error(err)
			return err
		}
		path2 := blocking_conf.FilePath(rightTableName, rightColumnNames, 0, blocking_conf.LSHBlockingFileSuffix)
		exist2, err := rock_db.DFS.Exist(path2)
		if err != nil {
			logger.Error(err)
			return err
		}
		if exist && exist2 {
			logger.Info("lsh blocking ", leftTableName, "-", leftColumnNames, " ", rightTableName, "-", rightColumnNames, " 已存在，跳过")
			return nil
		}
		if exist {
			err = rock_db.DFS.Delete(path)
			if err != nil {
				logger.Warn(err)
			}
		}
		if exist2 {
			err = rock_db.DFS.Delete(path2)
			if err != nil {
				logger.Warn(err)
			}
		}
	}
	leftVectorList, err := readVectorsFile(leftVectorsFile)
	if err != nil {
		return err
	}
	rightVectorList, err := readVectorsFile(rightVectorsFile)
	if err != nil {
		return err
	}

	leftTokenIdsList, rightTokenIdsList := doBlockingCross(leftVectorList, rightVectorList)

	{
		size := len(leftTokenIdsList)
		shardId := int64(0)
		startRowId := 0
		for startRowId < size {
			endRowId := startRowId + blocking_conf.FileRowSize
			if endRowId > size {
				endRowId = size
			}
			batchTokensList := leftTokenIdsList[startRowId:endRowId]
			filePath := blocking_conf.FilePath(leftTableName, leftColumnNames, shardId, blocking_conf.LSHBlockingFileSuffix)
			err := blocking_conf.WriteDerivedColumn(filePath, batchTokensList)
			if err != nil {
				logger.Error(err)
				return err
			}

			startRowId = endRowId
			shardId++
		}
	}

	{
		size := len(rightTokenIdsList)
		shardId := int64(0)
		startRowId := 0
		for startRowId < size {
			endRowId := startRowId + blocking_conf.FileRowSize
			if endRowId > size {
				endRowId = size
			}
			batchTokensList := rightTokenIdsList[startRowId:endRowId]
			filePath := blocking_conf.FilePath(leftTableName, leftColumnNames, shardId, blocking_conf.LSHBlockingFileSuffix)
			err := blocking_conf.WriteDerivedColumn(filePath, batchTokensList)
			if err != nil {
				logger.Error(err)
				return err
			}

			startRowId = endRowId
			shardId++
		}
	}
	return nil
}

func readVectorsFile(leftVectorsFile string) ([][]float64, error) {
	file, err := os.OpenFile(leftVectorsFile, os.O_RDONLY, 0666)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	defer func() {
		err := file.Close()
		if err != nil {
			logger.Warn(err)
		}
	}()
	var vectors [][]float64
	// 暂时格式为每列换行，行类逗号分隔。和 Java 版本保持一致
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var numbers []float64
		line := scanner.Text()
		for _, number := range strings.Split(line, ",") {
			f, err := strconv.ParseFloat(strings.TrimSpace(number), 64)
			if err != nil {
				logger.Warn(err)
			}
			numbers = append(numbers, f)
		}
		vectors = append(vectors, numbers)
	}
	return vectors, nil
}

const lhsTokenTopK = 5

func doBlocking[number Number](vectorList [][]number) (tokenIdsList [][]blocking_conf.TokenId) {
	rowSize := int32(len(vectorList))
	columnNumber := len(vectorList[0])
	cntTokenIdsList := make([][]pair[cnt, blocking_conf.TokenId], rowSize)
	tokenIdsList = make([][]blocking_conf.TokenId, rowSize)

	var tid blocking_conf.TokenId = 0
	for cid := 0; cid < columnNumber; cid++ {
		set := map[number][]int32{}
		for rid := int32(0); rid < rowSize; rid++ {
			num := vectorList[rid][cid]
			set[num] = append(set[num], rid)
		}
		for _, rowIds := range set {
			size := len(rowIds)
			if size < 2 {
				continue
			}
			for _, rowId := range rowIds {
				cntTokenIdsList[rowId] = append(cntTokenIdsList[rowId], pair[cnt, blocking_conf.TokenId]{
					L: cnt(size),
					R: tid,
				})
			}
			tid++
		}
	}
	for rid, cntTokenIds := range cntTokenIdsList {
		if len(cntTokenIds) == 0 {
			tokenIdsList[rid] = append(tokenIdsList[rid], blocking_conf.InvalidTokenId)
			continue
		}
		// 逆序
		sort.Slice(cntTokenIds, func(i, j int) bool {
			return cntTokenIds[i].L > cntTokenIds[j].L
		})
		tokenIdsList[rid] = append(tokenIdsList[rid], cntTokenIds[0].R)
		if len(cntTokenIds) > lhsTokenTopK {
			cntTokenIds = cntTokenIds[:lhsTokenTopK]
		}
		for _, ct := range cntTokenIds {
			tokenIdsList[rid] = append(tokenIdsList[rid], ct.R)
		}
	}

	return
}

func doBlockingCross[number Number](leftVectorList, rightVectorList [][]number) (leftTokenIdsList, rightTokenIdsList [][]blocking_conf.TokenId) {
	leftRowSize := int32(len(leftVectorList))
	rightRowSize := int32(len(rightVectorList))
	columnNumber := len(leftVectorList[0])
	leftCntTokenIdsList := make([][]pair[cnt, blocking_conf.TokenId], leftRowSize)
	rightCntTokenIdsList := make([][]pair[cnt, blocking_conf.TokenId], rightRowSize)
	leftTokenIdsList = make([][]blocking_conf.TokenId, leftRowSize)
	rightTokenIdsList = make([][]blocking_conf.TokenId, rightRowSize)

	var tid blocking_conf.TokenId = 0
	for cid := 0; cid < columnNumber; cid++ {
		leftSet := map[number][]int32{}
		rightSet := map[number][]int32{}
		for rid := int32(0); rid < leftRowSize; rid++ {
			num := leftVectorList[rid][cid]
			leftSet[num] = append(leftSet[num], rid)
		}
		for rid := int32(0); rid < rightRowSize; rid++ {
			num := rightVectorList[rid][cid]
			rightSet[num] = append(rightSet[num], rid)
		}
		for num, leftRowIds := range leftSet {
			leftSize := len(leftRowIds)
			rightSize := len(rightSet[num])
			if rightSize == 0 {
				continue
			}
			for _, rowId := range leftRowIds {
				leftCntTokenIdsList[rowId] = append(leftCntTokenIdsList[rowId], pair[cnt, blocking_conf.TokenId]{
					L: cnt(leftSize),
					R: tid,
				})
			}
			for _, rowId := range rightSet[num] {
				rightCntTokenIdsList[rowId] = append(rightCntTokenIdsList[rowId], pair[cnt, blocking_conf.TokenId]{
					L: cnt(rightSize),
					R: tid,
				})
			}
			tid++
		}
	}

	for rid, cntTokenIds := range leftCntTokenIdsList {
		if len(cntTokenIds) == 0 {
			leftTokenIdsList[rid] = []blocking_conf.TokenId{blocking_conf.InvalidTokenId}
			continue
		}
		// 逆序
		sort.Slice(cntTokenIds, func(i, j int) bool {
			return cntTokenIds[i].L > cntTokenIds[j].L
		})
		leftTokenIdsList[rid] = append(leftTokenIdsList[rid], cntTokenIds[0].R)
		if len(cntTokenIds) > lhsTokenTopK {
			cntTokenIds = cntTokenIds[:lhsTokenTopK]
		}
		for _, ct := range cntTokenIds {
			leftTokenIdsList[rid] = append(leftTokenIdsList[rid], ct.R)
		}
	}
	for rid, cntTokenIds := range rightCntTokenIdsList {
		if len(cntTokenIds) == 0 {
			rightTokenIdsList[rid] = []blocking_conf.TokenId{blocking_conf.InvalidTokenId}
			continue
		}
		// 逆序
		sort.Slice(cntTokenIds, func(i, j int) bool {
			return cntTokenIds[i].L > cntTokenIds[j].L
		})
		rightTokenIdsList[rid] = append(rightTokenIdsList[rid], cntTokenIds[0].R)
		if len(cntTokenIds) > lhsTokenTopK {
			cntTokenIds = cntTokenIds[:lhsTokenTopK]
		}
		for _, ct := range cntTokenIds {
			rightTokenIdsList[rid] = append(rightTokenIdsList[rid], ct.R)
		}
	}

	return
}

type Number interface {
	int | float32 | float64
}

type pair[L, R any] struct {
	L L
	R R
}

type cnt int32
