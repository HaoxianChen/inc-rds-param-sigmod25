package topk

import (
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"gitlab.grandhoo.com/rock/rock_v3/utils"
	"math"
	"strconv"
	"strings"
	"time"
)

type predicateColumnsInfo struct {
	isSingle          bool
	isNumber          bool
	constantValue     float64
	leftColumnsIndex  []int
	rightColumnsIndex []int
	symbolType        string
}

type GiniIndexInfo struct {
	xY     float64
	noXY   float64
	xNoY   float64
	noXNoY float64
}

func CalGiniIndexes(addPredicates []rds.Predicate, rhs rds.Predicate, data [][]float64, header []string, tableId2index map[string]int) []float32 {
	startTime := time.Now().UnixMilli()
	logger.Infof("cal gini index lhs:%v, rhs:%v", utils.GetLhsStr(addPredicates), rhs.PredicateStr)
	defer logger.Infof("spent time:%v, finish cal gini index lhs:%v, rhs:%v", time.Now().UnixMilli()-startTime, utils.GetLhsStr(addPredicates), rhs.PredicateStr)
	giniIndexInfos := make([]GiniIndexInfo, len(addPredicates))
	predicateColumnsInfos := generatePredicateColumnsInfos(addPredicates, header)
	columnSize := len(data[0])
	invalidPredicateIndex := getInvalidPredicateIndex(addPredicates, tableId2index)
	minPredicateIndex := make(map[int]bool)
	for _, rowData := range data {
		rhsData := rowData[columnSize-1]
		satisfyRhsFlag := satisfyRhs(rhs, rhsData)
		for i, p := range predicateColumnsInfos {
			if invalidPredicateIndex[i] {
				continue
			}
			var satisfyXFlag bool
			if p.isSingle { //单行谓词
				if p.isNumber { //数值类型
					if p.constantValue == rowData[p.leftColumnsIndex[0]] { //满足x
						satisfyXFlag = true
					}
				} else {                                     //枚举类型
					if rowData[p.leftColumnsIndex[0]] == 1 { //满足x
						satisfyXFlag = true
					}
				}
			} else {                                                         //多行谓词
				if p.leftColumnsIndex == nil || p.rightColumnsIndex == nil { //谓词中的某一项不在xy训练集中
					minPredicateIndex[i] = true
					continue
				}
				if p.isNumber {
					if rowData[p.leftColumnsIndex[0]] == rowData[p.rightColumnsIndex[0]] { //满足x
						satisfyXFlag = true
					}
				} else {
					for j := range p.leftColumnsIndex {
						if rowData[p.leftColumnsIndex[j]] == 1 && rowData[p.rightColumnsIndex[j]] == 1 { //满足x
							satisfyXFlag = true
							break
						}
					}
				}
			}
			if satisfyXFlag { //满足x
				if satisfyRhsFlag { //满足y
					giniIndexInfos[i].xY++
				} else { //不满足y
					giniIndexInfos[i].xNoY++
				}
			} else {                //不满足x
				if satisfyRhsFlag { //满足y
					giniIndexInfos[i].noXY++
				} else { //不满足y
					giniIndexInfos[i].noXNoY++
				}
			}
		}
	}
	result := make([]float32, len(giniIndexInfos))
	for i, giniIndexInfo := range giniIndexInfos {
		if invalidPredicateIndex[i] {
			result[i] = math.MaxFloat32
			continue
		}
		if minPredicateIndex[i] {
			result[i] = -1
			continue
		}
		result[i] = generateGiniIndex(giniIndexInfo)
	}
	return result
}

func generatePredicateColumnsInfos(predicates []rds.Predicate, header []string) []predicateColumnsInfo {
	result := make([]predicateColumnsInfo, len(predicates))
	for i, predicate := range predicates {
		result[i] = generatePredicateColumnsInfo(predicate, header)
	}
	return result
}

func generatePredicateColumnsInfo(predicate rds.Predicate, header []string) predicateColumnsInfo {
	result := predicateColumnsInfo{
		isSingle:          false,
		isNumber:          false,
		constantValue:     0,
		leftColumnsIndex:  nil,
		rightColumnsIndex: nil,
		symbolType:        predicate.SymbolType,
	}
	if predicate.PredicateType == 0 { //单行谓词
		result.isSingle = true
		leftColumn := predicate.PredicateStr
		switch predicate.LeftColumn.ColumnType {
		case rds_config.IntType:
			result.isNumber = true
			leftColumn = strings.Split(predicate.PredicateStr, predicate.SymbolType)[0]
			result.constantValue = float64(predicate.ConstantValue.(int64))
		case rds_config.FloatType:
			result.isNumber = true
			leftColumn = strings.Split(predicate.PredicateStr, predicate.SymbolType)[0]
			result.constantValue = predicate.ConstantValue.(float64)
		}
		for i, s := range header {
			if s == leftColumn {
				result.leftColumnsIndex = append(result.leftColumnsIndex, i)
				break
			}
		}
	} else { //多行谓词
		//leftColumn := strings.Split(predicate.PredicateStr, predicate.SymbolType)[0]
		//rightColumn := strings.Split(predicate.PredicateStr, predicate.SymbolType)[1]
		// todo 暂时适配ml直接用=
		//leftColumn := strings.Split(predicate.PredicateStr, rds_config.Equal)[0]
		//rightColumn := strings.Split(predicate.PredicateStr, rds_config.Equal)[1]
		leftColumn := "t" + strconv.Itoa(predicate.LeftColumn.ColumnIndex) + "." + predicate.LeftColumn.ColumnId
		rightColumn := "t" + strconv.Itoa(predicate.RightColumn.ColumnIndex) + "." + predicate.RightColumn.ColumnId
		switch predicate.LeftColumn.ColumnType {
		case rds_config.IntType, rds_config.FloatType:
			var hasLeft, hasRight bool
			result.isNumber = true
			for i, s := range header {
				if hasLeft && hasRight {
					break
				}
				if !hasLeft && s == leftColumn {
					result.leftColumnsIndex = append(result.leftColumnsIndex, i)
					hasLeft = true
					continue
				}
				if !hasRight && s == rightColumn {
					result.rightColumnsIndex = append(result.rightColumnsIndex, i)
					hasRight = true
				}
			}
		case rds_config.EnumType, rds_config.BoolType:
			m := make(map[string]map[string]int)
			for i, s := range header {
				if strings.Contains(s, leftColumn) {
					constantValue := strings.Split(s, rds_config.Equal)[1]
					if _, f := m[constantValue]; !f {
						m[constantValue] = make(map[string]int)
					}
					m[constantValue]["left"] = i
				} else if strings.Contains(s, rightColumn) {
					constantValue := strings.Split(s, rds_config.Equal)[1]
					if _, f := m[constantValue]; !f {
						m[constantValue] = make(map[string]int)
					}
					m[constantValue]["right"] = i
				}
			}
			for _, pair := range m {
				if len(pair) > 1 {
					result.leftColumnsIndex = append(result.leftColumnsIndex, pair["left"])
					result.rightColumnsIndex = append(result.rightColumnsIndex, pair["right"])
				}
			}
		}
	}
	return result
}

func satisfyRhs(rhs rds.Predicate, data float64) bool {
	if rhs.PredicateType != 0 {
		return data == float64(1)
	} else {
		switch rhs.LeftColumn.ColumnType {
		case rds_config.EnumType, rds_config.BoolType:
			return data == float64(1)
		case rds_config.IntType:
			return data == float64(rhs.ConstantValue.(int64))
		case rds_config.FloatType:
			return data == rhs.ConstantValue.(float64)
		}
	}
	return false
}

func generateGiniIndex(giniIndexInfo GiniIndexInfo) float32 {
	xY := giniIndexInfo.xY
	noXY := giniIndexInfo.noXY
	xNoY := giniIndexInfo.xNoY
	noXNoY := giniIndexInfo.noXNoY
	y := xY + noXY
	noY := xNoY + noXNoY
	iLeft := 1 - (xNoY/noY)*(xNoY/noY) - (noXNoY/noY)*(noXNoY/noY)
	iRight := 1 - (xY/y)*(xY/y) - (noXY/y)*(noXY/y)
	f := (noY/(y+noY))*iLeft + (y/(y+noY))*iRight
	if math.IsNaN(f) {
		return 0
	}
	i := float32(f)
	return i
}

func getInvalidPredicateIndex(predicates []rds.Predicate, tableId2index map[string]int) map[int]bool {
	result := make(map[int]bool)
	for i, predicate := range predicates {
		leftTableId := predicate.LeftColumn.TableId
		rightTableId := predicate.RightColumn.TableId
		if _, ok := tableId2index[leftTableId]; !ok {
			result[i] = true
			continue
		}
		if _, ok := tableId2index[rightTableId]; !ok {
			result[i] = true
		}
	}
	return result
}
