package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/yourbasic/bit"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_memery"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/table_data"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"gitlab.grandhoo.com/rock/rock_v3/utils/storage_utils"
	"gitlab.grandhoo.com/rock/storage/config/cpu_busy"
	"gitlab.grandhoo.com/rock/storage/storage2/database/types/special_type/timetype"
	"gitlab.grandhoo.com/rock/storage/storage2/utils"
	"golang.org/x/exp/maps"
	"log"
	"math"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"
)

var lastTotalFreed uint64

// 由后端调用
func StartMemoryCleaner() {
	go func() {
		for true {
			time.Sleep(10 * time.Second)
			global_variables.GlobalVariableLock.Lock()
			if cpu_busy.IsCPUFree() {
				maps.Clear(global_variables.GlobalVariable)
			}
			global_variables.GlobalVariableLock.Unlock()
		}
	}()
}

func ClearMemory(taskId int64) {
	//global_variables.GlobalVariableLock.Lock()
	//global_variables.GlobalVariable[taskId] = nil
	//delete(global_variables.GlobalVariable, taskId)
	//global_variables.GlobalVariableLock.Unlock()
	table_data.DropTaskId(taskId)
	runtime.GC()
	debug.FreeOSMemory()
}

func StopTask(taskId int64) {
	go func() {
		_ = storage_utils.DeleteTaskConflict(taskId)
	}()
	ClearMemory(taskId)
	//logger.Infof("taskId:%v,收到信号,停止任务", taskId)
}

func printMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Printf("Alloc = %v TotalAlloc = %v  Just Freed = %v Sys = %v NumGC = %v\n",
		m.Alloc/1024, m.TotalAlloc/1024, ((m.TotalAlloc-m.Alloc)-lastTotalFreed)/1024, m.Sys/1024, m.NumGC)
	lastTotalFreed = m.TotalAlloc - m.Alloc
}

func GetInterfaceToString(value interface{}) string {
	// interface 转 string
	var key string
	if value == nil {
		return key
	}

	switch value.(type) {
	case float64:
		ft := value.(float64)
		key = strconv.FormatFloat(ft, 'f', -1, 64)
	case float32:
		ft := value.(float32)
		key = strconv.FormatFloat(float64(ft), 'f', -1, 64)
	case int:
		it := value.(int)
		key = strconv.Itoa(it)
	case uint:
		it := value.(uint)
		key = strconv.Itoa(int(it))
	case int8:
		it := value.(int8)
		key = strconv.Itoa(int(it))
	case uint8:
		it := value.(uint8)
		key = strconv.Itoa(int(it))
	case int16:
		it := value.(int16)
		key = strconv.Itoa(int(it))
	case uint16:
		it := value.(uint16)
		key = strconv.Itoa(int(it))
	case int32:
		it := value.(int32)
		key = strconv.Itoa(int(it))
	case uint32:
		it := value.(uint32)
		key = strconv.Itoa(int(it))
	case int64:
		it := value.(int64)
		key = strconv.FormatInt(it, 10)
	case uint64:
		it := value.(uint64)
		key = strconv.FormatUint(it, 10)
	case string:
		key = value.(string)
	case time.Time:
		t, _ := value.(time.Time)
		key = t.String()
		// 2022-11-23 11:29:07 +0800 CST  这类格式把尾巴去掉
		key = strings.Replace(key, " +0800 CST", "", 1)
		key = strings.Replace(key, " +0000 UTC", "", 1)
	case []byte:
		key = string(value.([]byte))
	default:
		newValue, _ := json.Marshal(value)
		key = string(newValue)
	}

	return key
}

type selfMap struct {
	key   string
	value float64
}

func SortMap(data map[string]float64, isAscending bool) []selfMap {
	var result []selfMap
	for k, v := range data {
		result = append(result, selfMap{k, v})
	}
	if isAscending {
		sort.Slice(result, func(i, j int) bool {
			return result[i].value < result[j].value // 升序
		})
	} else {
		sort.Slice(result, func(i, j int) bool {
			return result[i].value > result[j].value // 降序
		})
	}
	return result
}

func SortPredicates(data []rds.Predicate, isAscending bool) {
	if isAscending {
		sort.SliceStable(data, func(i, j int) bool {
			return data[i].PredicateStr < data[j].PredicateStr // 升序
		})
		sort.SliceStable(data, func(i, j int) bool {
			return data[i].Support < data[j].Support // 升序
		})
	} else {
		sort.SliceStable(data, func(i, j int) bool {
			return data[i].PredicateStr > data[j].PredicateStr // 降序
		})
		sort.SliceStable(data, func(i, j int) bool {
			return data[i].Support > data[j].Support // 降序
		})
	}
}

func SortPredicatesByCorrelation(data []rds.Predicate, table2Column2Rank map[string]map[string]int, isAscending bool) {
	if isAscending { //升序
		sort.SliceStable(data, func(i, j int) bool {
			tableI := data[i].LeftColumn.TableId
			columnI := data[i].LeftColumn.ColumnId

			tableJ := data[j].LeftColumn.TableId
			columnJ := data[j].LeftColumn.ColumnId

			if _, exist := table2Column2Rank[tableI]; !exist {
				return false
			}
			if _, exist := table2Column2Rank[tableJ]; !exist {
				return true
			}
			rankI, exist := table2Column2Rank[tableI][columnI]
			if !exist {
				return false
			}
			rankJ, exist := table2Column2Rank[tableJ][columnJ]
			if !exist {
				return true
			}
			return rankI > rankJ
		})
	} else {
		sort.SliceStable(data, func(i, j int) bool {
			tableI := data[i].LeftColumn.TableId
			columnI := data[i].LeftColumn.ColumnId

			tableJ := data[j].LeftColumn.TableId
			columnJ := data[j].LeftColumn.ColumnId

			if _, exist := table2Column2Rank[tableI]; !exist {
				return false
			}
			if _, exist := table2Column2Rank[tableJ]; !exist {
				return true
			}
			rankI, exist := table2Column2Rank[tableI][columnI]
			if !exist {
				return false
			}
			rankJ, exist := table2Column2Rank[tableJ][columnJ]
			if !exist {
				return true
			}
			return rankI < rankJ
		})
	}
}

func GenTokenChan(coefficient float64) chan struct{} {
	cpuNum := runtime.NumCPU()
	if cpuNum <= 0 {
		cpuNum = 1
	}
	tokenNum := int(coefficient * float64(cpuNum))
	if tokenNum <= 0 {
		tokenNum = 1
	}
	ch := make(chan struct{}, tokenNum)
	for i := 0; i < tokenNum; i++ {
		ch <- struct{}{}
	}
	return ch
}

func CountSupport(intersection map[interface{}]map[string][]int32, hasCrossColumn, isRds bool) int {
	if intersection == nil {
		return 0
	}
	ans := 0
	for _, value := range intersection {
		tmp := 1
		if len(value) == 1 {
			for _, rows := range value {
				if isRds {
					rdsCount := 0
					for _, row := range rows {
						if row < rds_config.RdsSize {
							rdsCount++
						}
					}
					tmp *= rdsCount
				} else {
					tmp *= len(rows)
				}
			}
		} else {
			// TODO 目前只考虑的单表的情况
			index := 1
			for key, rows := range value {
				if isRds {
					rdsCount := 0
					for _, row := range rows {
						if row < rds_config.RdsSize {
							rdsCount++
						}
					}
					if "t1" == key {
						// 存在跨列的时候多行规则的同一行也是有意义的
						if hasCrossColumn {
							tmp *= rdsCount
						} else {
							tmp *= rdsCount - 1
						}
						continue
					}
					tmp *= rdsCount
				} else {
					if "t1" == key {
						// 存在跨列的时候多行规则的同一行也是有意义的
						if hasCrossColumn {
							tmp *= len(rows)
						} else {
							tmp *= len(rows) - 1
						}
						continue
					}
					tmp *= len(rows)
				}
				index++
			}
		}
		ans += tmp
	}
	if ans < 0 {
		return 0
	}
	return ans
}

func CountSupportNew(intersection [][2][]int32, hasCrossColumn, isRds, isSingle bool) (int, int, []int32) {
	if intersection == nil {
		return 0, 0, nil
	}
	ans := 0
	relatedRowCount := 0
	var satisfyRows []int32
	for _, value := range intersection {
		tmp := 1
		if isSingle {
			rows := value[0]
			relatedRowCount += len(rows)
			satisfyRows = append(satisfyRows, rows...)
			if isRds {
				rdsCount := 0
				for _, row := range rows {
					if row < rds_config.RdsSize {
						rdsCount++
					}
				}
				tmp *= rdsCount
			} else {
				tmp *= len(rows)
			}
		} else {
			// TODO 目前只考虑的单表的情况
			index := 1
			// 多行规则，不存在跨列时，交集只有1行时，不看作satisfy
			// http://erpnext.grandhoo.com/app/bug/BUG-23061305604
			if (hasCrossColumn || len(value[1]) > 1) && len(value[0]) > 0 {
				relatedRowCount += len(value[1])
				satisfyRows = append(satisfyRows, value[1]...)
			}
			for key, rows := range value {
				if isRds {
					rdsCount := 0
					for _, row := range rows {
						if row < rds_config.RdsSize {
							rdsCount++
						}
					}
					if 1 == key {
						// 存在跨列的时候多行规则的同一行也是有意义的
						if hasCrossColumn {
							tmp *= rdsCount
						} else {
							tmp *= rdsCount - 1
						}
						continue
					}
					tmp *= rdsCount
				} else {
					if 1 == key {
						// 存在跨列的时候多行规则的同一行也是有意义的
						if hasCrossColumn {
							tmp *= len(rows)
						} else {
							tmp *= len(rows) - 1
						}
						continue
					}
					tmp *= len(rows)
				}
				index++
			}
		}
		ans += tmp
	}
	if ans < 0 {
		return 0, 0, nil
	}
	return ans, relatedRowCount, satisfyRows
}

func IsSpecificColumn(columnValues []interface{}, regex string) bool {
	// 传入的一定是string类型的列
	var positive, negative int
	for i, value := range columnValues {
		if i >= rds_config.RegexCheckNum {
			break
		}
		if value == nil {
			continue
		}
		valueStr := value.(string)
		match, err := regexp.MatchString(regex, valueStr)
		if err != nil {
			logger.Error("match regex err:", err)
			return false
		}
		if match {
			positive++
		} else {
			negative++
		}
	}
	if float64(positive)/float64(positive+negative) > rds_config.RegexRatio {
		return true
	}
	return false
}

func FindDiffer(t0, t1 []int32) []int32 {
	var ans []int32
	i, j := 0, 0
	for i < len(t0) && j < len(t1) {
		if t0[i] == t1[j] {
			i++
			j++
		} else {
			ans = append(ans, t1[j])
			j++
		}
	}
	for j < len(t1) {
		ans = append(ans, t1[j])
		j++
	}
	return ans
}

func FindDifferBits(t0, t1 []int32) *bit.Set {
	ans := bit.New()
	i, j := 0, 0
	for i < len(t0) && j < len(t1) {
		if t0[i] == t1[j] {
			i++
			j++
		} else {
			ans.Add(int(t1[j]))
			j++
		}
	}
	for j < len(t1) {
		ans.Add(int(t1[j]))
		j++
	}
	return ans
}

func IsNumberType(dataType string) bool {
	if rds_config.IntType == dataType || rds_config.FloatType == dataType {
		return true
	}
	return false
}

func IsBlank(value interface{}) bool {
	if value == nil || value == "" {
		return true
	}
	return false
}

// 大于1 等于0 小于-1
func CompareTo(val1 interface{}, val2 interface{}, valType string) (int, error) {
	if rds_config.IntType == valType {
		if val1Trans, ok := val1.(int64); ok {
			if val2Trans, ok := val2.(int64); ok {
				if val1Trans > val2Trans {
					return 1, nil
				} else if val1Trans == val2Trans {
					return 0, nil
				} else {
					return -1, nil
				}
			} else {
				msg := fmt.Sprintf("值:%v 转换INT64数据类型失败", val2)
				logger.Errorf(msg)
				return 0, errors.New(msg)
			}
		} else {
			msg := fmt.Sprintf("值:%v 转换INT64数据类型失败", val1)
			logger.Errorf(msg)
			return 0, errors.New(msg)
		}
	} else if rds_config.FloatType == valType {
		if val1Trans, ok := val1.(float64); ok {
			if val2Trans, ok := val2.(float64); ok {
				if val1Trans > val2Trans {
					return 1, nil
				} else if val1Trans == val2Trans {
					return 0, nil
				} else {
					return -1, nil
				}
			} else {
				msg := fmt.Sprintf("值:%v 转换FLOAT64数据类型失败", val2)
				logger.Errorf(msg)
				return 0, errors.New(msg)
			}
		} else {
			msg := fmt.Sprintf("值:%v 转换FLOAT64数据类型失败", val1)
			logger.Errorf(msg)
			return 0, errors.New(msg)
		}
	} else if rds_config.TimeType == valType {
		if val1Trans, ok := val1.(timetype.Time); ok {
			if val2Trans, ok := val2.(timetype.Time); ok {
				if val1Trans > val2Trans {
					return 1, nil
				} else if val1Trans == val2Trans {
					return 0, nil
				} else {
					return -1, nil
				}
			} else {
				msg := fmt.Sprintf("值:%v 转换TIME数据类型失败", val2)
				logger.Errorf(msg)
				return 0, errors.New(msg)
			}
		} else {
			msg := fmt.Sprintf("值:%v 转换TIME数据类型失败", val1)
			logger.Errorf(msg)
			return 0, errors.New(msg)
		}
	} else {
		return 0, errors.New("不支持的数据类型")
	}
}

func ChangeFloatType(arr []float32) []float64 {
	ans := make([]float64, len(arr))
	for i, f := range arr {
		f2 := float64(f)
		if f2 == math.NaN() {
			f2 = 0
		}
		ans[i] = f2
	}
	return ans
}

func isConnectedGraph(arr [][2]int) bool {
	// 构建邻接表
	graph := make(map[int][]int)
	nodes := make(map[int]bool)
	for _, edge := range arr {
		u, v := edge[0], edge[1]
		graph[u] = append(graph[u], v)
		graph[v] = append(graph[v], u)
		nodes[u] = true
		nodes[v] = true
	}

	visited := make(map[int]bool)
	dfs(arr[0][0], graph, visited)

	// 判断是否访问到所有节点
	for i := range nodes {
		if !visited[i] {
			return false // 存在未访问到的节点，说明不是连通图
		}
	}

	return true // 所有节点都被访问到，是连通图
}

func dfs(node int, graph map[int][]int, visited map[int]bool) {
	visited[node] = true

	for _, neighbor := range graph[node] {
		if !visited[neighbor] {
			dfs(neighbor, graph, visited)
		}
	}
}

func Swap(a, b *string) {
	temp := a // 使用临时变量temp存储a的值
	a = b     // 将b的值赋给a
	b = temp
}
func SwapI(a, b *int) {
	temp := a // 使用临时变量temp存储a的值
	a = b     // 将b的值赋给a
	b = temp
}

func rockSlice2String(s *rock_memery.RockSlice) string {
	ss := make([]string, s.Len())
	for i := 0; i < s.Len(); i++ {
		val, err := s.Get(i)
		ss[i] = fmt.Sprintf("%v", val)
		if err != nil {
			ss[i] += err.Error()
		}
	}
	return utils.ToString(ss)
}
