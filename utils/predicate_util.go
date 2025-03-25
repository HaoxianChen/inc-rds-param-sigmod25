package utils

import (
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/task_tree"
	"gitlab.grandhoo.com/rock/rock_v3/request"
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking/udf_column"
	"golang.org/x/exp/slices"
	"sort"
	"strconv"
	"strings"

	"gitlab.grandhoo.com/rock/rock-share/global/enum"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"gitlab.grandhoo.com/rock/rock_v3/utils/storage_utils"

	mapset "github.com/deckarep/golang-set"
)

// GetPredicatesStr1 将多个谓词排序并转换成str
func GetPredicatesStr1(predicates map[string]bool) string {
	s1 := mapset.NewSet() //lhs
	for kk := range predicates {
		s1.Add(kk)
	}
	lhsSorted := s1.ToSlice()
	sort.Slice(lhsSorted, func(i, j int) bool {
		return lhsSorted[i].(string) < lhsSorted[j].(string)
	})
	lhsStr := ""
	for _, vs11 := range lhsSorted {
		lhsStr += fmt.Sprintf("%v", vs11)
	}
	return lhsStr
}

func GetPredicatesStr(predicates []rds.Predicate) string {
	SortPredicates(predicates, false)
	lhsStr := ""
	for _, vs11 := range predicates {
		lhsStr += fmt.Sprintf("%v", vs11.PredicateStr)
	}
	return lhsStr
}

// GetConstantValue 获取常数谓词中的常数值
func GetConstantValue(predicate string) string {
	return strings.TrimSpace(strings.Split(predicate, "=")[1])
}

func GetSortedLhs(predicates map[string]bool) string {
	tmp := make([]string, 0, len(predicates))
	for k := range predicates {
		tmp = append(tmp, k)
	}
	sort.Slice(tmp, func(i, j int) bool {
		return tmp[i] < tmp[j]
	})
	return strings.Join(tmp, " ^ ")
}

func GetLhsStr(predicates []rds.Predicate) string {
	//SortPredicates(predicates, false)
	var tmp []string
	for _, predicate := range predicates {
		tmp = append(tmp, predicate.PredicateStr)
	}
	return strings.Join(tmp, " ^ ")
}

func GetStrWithTableName(predicates []rds.Predicate, rhs *rds.Predicate, sort bool) (string, map[string]int, string, string) {
	//predicatesCopy := make([]rds.Predicate, len(predicates))
	//copy(predicatesCopy, predicates)
	for i := range predicates {
		predicate := predicates[i]
		if predicate.PredicateType == 0 {
			predicates[i].PredicateStr = predicate.LeftColumn.TableId + "." + predicate.LeftColumn.ColumnId + predicate.SymbolType + fmt.Sprint(predicate.ConstantValue)
		} else {
			predicates[i].PredicateStr = predicate.LeftColumn.TableId + "." + predicate.LeftColumn.ColumnId + predicate.SymbolType + predicate.RightColumn.TableId + " ." + predicate.RightColumn.ColumnId
		}
	}
	if sort {
		SortPredicates(predicates, false)
	}
	var tmp []string
	var withAlias []string
	predicateStrMap := make(map[string]struct{})
	table2Index := make(map[string]int)
	i := 0

	//if rhs != nil && rhs.PredicateStr != "" { //有y谓词，先编号Y谓词
	//	leftTable := rhs.LeftColumn.TableId
	//	if _, exist := table2Index[leftTable]; !exist {
	//		table2Index[leftTable] = i
	//		i += 2
	//	}
	//	rightTable := ""
	//	if rhs.RightColumn.TableId != "" {
	//		rightTable = rhs.RightColumn.TableId
	//		if _, exist := table2Index[rightTable]; !exist {
	//			table2Index[rightTable] = i
	//			i += 2
	//		}
	//	}
	//}

	for j := range predicates {
		predicate := predicates[j]
		tmp = append(tmp, predicate.PredicateStr)
		leftTable := predicate.LeftColumn.TableId
		if _, exist := table2Index[leftTable]; !exist {
			table2Index[leftTable] = i
			i += 2
		}
		rightTable := ""
		if predicate.RightColumn.TableId != "" {
			rightTable = predicate.RightColumn.TableId
			if _, exist := table2Index[rightTable]; !exist {
				table2Index[rightTable] = i
				i += 2
			}
		}

		predicateStr := "t" + fmt.Sprint(table2Index[leftTable]) + "." + predicate.LeftColumn.ColumnId + predicate.SymbolType
		if leftTable == rightTable {
			predicateStr += "t" + fmt.Sprint(table2Index[rightTable]+1) + "." + predicate.RightColumn.ColumnId
		} else if rightTable != "" {
			predicateStr += "t" + fmt.Sprint(table2Index[rightTable]) + "." + predicate.RightColumn.ColumnId
		} else {
			predicateStr += fmt.Sprint(predicate.ConstantValue)
		}
		if _, exist := predicateStrMap[predicateStr]; !exist {
			predicateStrMap[predicateStr] = struct{}{}
		} else if predicate.PredicateType != 0 { //这种情况下才可能需要copy一份
			predicateStr = "t" + fmt.Sprint(table2Index[leftTable]+1) + "." + predicate.LeftColumn.ColumnId + predicate.SymbolType
			predicateStr += "t" + fmt.Sprint(table2Index[rightTable]+1) + "." + predicate.RightColumn.ColumnId
		}
		predicates[j].PredicateStr = predicateStr
		withAlias = append(withAlias, predicateStr)
	}

	rhsStr := ""
	if rhs != nil && rhs.PredicateStr != "" {
		rhsLeftTable := rhs.LeftColumn.TableId
		rhsRightTable := rhs.RightColumn.TableId
		if _, exist := table2Index[rhsLeftTable]; !exist {
			table2Index[rhsLeftTable] = i
			i += 2
		}
		if rhsRightTable != "" {
			if _, exist := table2Index[rhsRightTable]; !exist {
				table2Index[rhsRightTable] = i
				i += 2
			}
		}
		if rhsLeftTable == rhsRightTable {
			rhsStr = "t" + fmt.Sprint(table2Index[rhsLeftTable]) + "." + rhs.LeftColumn.ColumnId + rhs.SymbolType +
				"t" + fmt.Sprint(table2Index[rhsRightTable]+1) + "." + rhs.RightColumn.ColumnId
		} else if rhsRightTable != "" {
			rhsStr = "t" + fmt.Sprint(table2Index[rhsLeftTable]) + "." + rhs.LeftColumn.ColumnId + rhs.SymbolType +
				"t" + fmt.Sprint(table2Index[rhsRightTable]) + "." + rhs.RightColumn.ColumnId
		} else {
			rhsStr = "t" + fmt.Sprint(table2Index[rhsLeftTable]) + "." + rhs.LeftColumn.ColumnId + rhs.SymbolType + fmt.Sprint(rhs.ConstantValue)
		}
		rhs.PredicateStr = rhsStr
	}

	return strings.Join(tmp, " ^ "), table2Index, strings.Join(withAlias, " ^ "), rhsStr
}

func GetLhsStrWithTableName(predicates []rds.Predicate) string {
	predicatesCopy := make([]rds.Predicate, len(predicates))
	copy(predicatesCopy, predicates)
	for i := range predicatesCopy {
		predicate := predicatesCopy[i]
		if predicate.PredicateType == 0 {
			predicatesCopy[i].PredicateStr = predicate.LeftColumn.TableId + "." + predicate.LeftColumn.ColumnId + "=" + fmt.Sprint(predicate.ConstantValue)
		} else {
			predicatesCopy[i].PredicateStr = predicate.LeftColumn.TableId + "." + predicate.LeftColumn.ColumnId + "=" + predicate.RightColumn.TableId + "." + predicate.RightColumn.ColumnId
		}
	}
	SortPredicates(predicatesCopy, false)
	var tmp []string
	for j := range predicatesCopy {
		predicate := predicatesCopy[j]
		tmp = append(tmp, predicate.PredicateStr)
	}

	return strings.Join(tmp, " ^ ")
}

func GetPredicateType(predicate string) string {
	if strings.Contains(predicate, enum.GreaterE) {
		return enum.GreaterE
	}
	if strings.Contains(predicate, enum.Less) {
		return enum.Less
	}
	return enum.Equal
}

func DeletePredicate(predicates []rds.Predicate, predicate rds.Predicate) []rds.Predicate {
	var result []rds.Predicate
	for _, p := range predicates {
		if p.PredicateStr != predicate.PredicateStr {
			result = append(result, p)
		}
	}
	return result
}

func DeletePredicateAndGini(predicates []rds.Predicate, giniIndexes []float64, predicate rds.Predicate) ([]rds.Predicate, []float64) {
	var result []rds.Predicate
	var resultGini []float64
	for i, p := range predicates {
		if p.PredicateStr != predicate.PredicateStr {
			result = append(result, p)
			resultGini = append(resultGini, giniIndexes[i])
		}
	}
	return result, resultGini
}

func GetPredicatesColumn(predicates []rds.Predicate) []rds.Column {
	columns := make([]rds.Column, len(predicates))
	for i, predicate := range predicates {
		columns[i] = predicate.LeftColumn
	}
	return columns
}

func GetMultiTablePredicatesColumn(predicates []rds.Predicate) []rds.Column { //去重了
	existColumn := make(map[string]struct{})
	columns := make([]rds.Column, 0)
	for _, predicate := range predicates {
		key := predicate.LeftColumn.TableId + "_" + predicate.LeftColumn.ColumnId
		if key == "_" { //groupby谓词的暂时处理
			continue
		}
		if _, exist := existColumn[key]; !exist {
			columns = append(columns, predicate.LeftColumn)
			existColumn[key] = struct{}{}
		}
		if predicate.PredicateType == 0 {
			continue
		} else if predicate.RightColumn.TableId != predicate.LeftColumn.TableId || predicate.RightColumn.ColumnId != predicate.LeftColumn.ColumnId {
			key := predicate.RightColumn.TableId + "_" + predicate.RightColumn.ColumnId
			if _, exist := existColumn[key]; !exist {
				columns = append(columns, predicate.RightColumn)
				existColumn[key] = struct{}{}
			}
		}
	}
	return columns
}

// SplitLhs 将谓词组合拆分成一个规则的形式
func SplitLhs(lhs []rds.Predicate) ([]rds.Predicate, rds.Predicate) {
	var lhsTmp []rds.Predicate
	var rhsTmp rds.Predicate
	flag := true
	lhsLen := len(lhs)
	index := 1
	for _, predicate := range lhs {
		// 存在常数谓词的话,将常数谓词作为rhs
		if flag && predicate.PredicateType == 0 {
			rhsTmp = predicate
			flag = false
			index++
			continue
		}
		// 不存在常数谓词的话,将最后一个谓词作为rhs
		if flag && index == lhsLen {
			rhsTmp = predicate
			continue
		}
		index++
		lhsTmp = append(lhsTmp, predicate)
	}
	return lhsTmp, rhsTmp
}

func FilterLhs(rhs rds.Predicate, lhs []rds.Predicate) []rds.Predicate {
	var result []rds.Predicate
	for _, k := range lhs {
		if k.LeftColumn != rhs.LeftColumn {
			result = append(result, k)
		}
	}
	return result
}

func MultiTableFilterLhs(rhs rds.Predicate, lhs []rds.Predicate, table2JoinTables map[string]map[string]struct{}) []rds.Predicate {
	var result []rds.Predicate
	rhsLeftTable := rhs.LeftColumn.TableId
	rhsRightTable := rhs.RightColumn.TableId
	for _, k := range lhs {
		if k.PredicateStr == rhs.PredicateStr {
			continue
		}
		leftTableId := k.LeftColumn.TableId
		rightTableId := k.RightColumn.TableId
		if IsTableRelated(rhsLeftTable, rhsRightTable, leftTableId, rightTableId, table2JoinTables) {
			result = append(result, k)
		}
	}
	return result
}

func SingleTableFilterLhs(rhs rds.Predicate, lhs []rds.Predicate) []rds.Predicate {
	var result []rds.Predicate
	for _, k := range lhs {
		if k.PredicateStr == rhs.PredicateStr {
			continue
		}
		// 避免 similar(a,a) -> a=a
		if k.SymbolType == enum.ML || k.SymbolType == enum.Similar {
			if k.LeftColumn.TableId == rhs.LeftColumn.TableId && k.LeftColumn.ColumnId == rhs.LeftColumn.ColumnId {
				continue
			}
			if k.RightColumn.TableId == rhs.RightColumn.TableId && k.RightColumn.ColumnId == rhs.RightColumn.ColumnId {
				continue
			}
		}
		result = append(result, k)
	}
	return result
}

func GeneratePredicateStr_(predicate *rds.Predicate) string {
	leftColName := predicate.LeftColumn.ColumnId
	rightColName := predicate.RightColumn.ColumnId
	if predicate.SymbolType == enum.ML {
		return fmt.Sprintf("%s('%s', t%d.%s, t%d.%s)", predicate.SymbolType, predicate.UDFName, predicate.LeftColumn.ColumnIndex, leftColName, predicate.RightColumn.ColumnIndex, rightColName)
	} else if predicate.SymbolType == enum.Similar {
		return fmt.Sprintf("%s('%s', t%d.%s, t%d.%s, %0.2f)", predicate.SymbolType, predicate.UDFName, predicate.LeftColumn.ColumnIndex, leftColName, predicate.RightColumn.ColumnIndex, rightColName, predicate.Threshold)
	} else {
		return fmt.Sprintf("t%d.%s=t%d.%s", predicate.LeftColumn.ColumnIndex, leftColName, predicate.RightColumn.ColumnIndex, rightColName)
	}
}

func GenerateConnectPredicateNew(predicate rds.Predicate) []rds.Predicate {
	addedPredicate := CopyPredicate(predicate)
	addedPredicate.LeftColumn.ColumnIndex = predicate.LeftColumn.ColumnIndex + 1
	addedPredicate.RightColumn.ColumnIndex = predicate.RightColumn.ColumnIndex + 1
	addedPredicate.PredicateStr = GeneratePredicateStr_(&addedPredicate)
	return []rds.Predicate{addedPredicate, predicate}
}

func IsTableRelated(rhsLeftTable, rhsRightTable, leftTableId, rightTableId string, table2JoinTables map[string]map[string]struct{}) bool {
	if rhsLeftTable == leftTableId || rhsRightTable == rightTableId {
		return true
	} else if _, exist := table2JoinTables[rhsLeftTable][leftTableId]; exist {
		return true
	} else if _, exist := table2JoinTables[rhsLeftTable][rightTableId]; exist {
		return true
	} else if _, exist := table2JoinTables[rhsRightTable][leftTableId]; exist {
		return true
	} else if _, exist := table2JoinTables[rhsRightTable][rightTableId]; exist {
		return true
	}
	return false
}

func ContainsColumn(predicate rds.Predicate, lhs []rds.Predicate) bool {
	// 如果待判断的谓词为相似度谓词需要特殊处理
	if predicate.PredicateType == 1 && predicate.SymbolType == enum.Similar {
		for _, predicate2 := range lhs {
			if predicate.LeftColumn.ColumnId == predicate2.LeftColumn.ColumnId && predicate.RightColumn.ColumnId == predicate2.RightColumn.ColumnId {
				return true
			}
			if predicate.RightColumn.ColumnId == predicate2.LeftColumn.ColumnId && predicate.LeftColumn.ColumnId == predicate2.RightColumn.ColumnId {
				return true
			}
		}
	}
	for _, predicate2 := range lhs {
		if predicate.LeftColumn.ColumnId == predicate2.LeftColumn.ColumnId {
			return true
		}
	}
	return false
}

func HasCrossColumnPredicate(predicates []rds.Predicate) bool {
	for _, predicate := range predicates {
		// 常数谓词的直接跳过
		if predicate.PredicateType == 0 {
			continue
		}
		if predicate.LeftColumn.ColumnId != predicate.RightColumn.ColumnId {
			return true
		}
	}
	return false
}

func GetSimilarTypeThreshold(predicate rds.Predicate) (string, float64) {
	predicateStr := predicate.PredicateStr
	tmpStr := strings.Split(predicateStr, "(")[1]
	values := strings.Split(tmpStr, ",")
	similarType := values[0]
	threshold, _ := strconv.ParseFloat(values[1], 64)
	return similarType, threshold
}

func AnalysisPredicateStr(predicateStr string, exeTableId []string, tables []*TableInfo) (rds.Predicate, []rds.Column) {
	// 多项式规则的rhs可能就是一个true
	if predicateStr == "true" {
		predicate := rds.Predicate{
			PredicateStr:  predicateStr,
			LeftColumn:    rds.Column{},
			RightColumn:   rds.Column{},
			ConstantValue: nil,
			SymbolType:    rds_config.PolyRight,
			PredicateType: 0,
			Support:       0,
		}
		return predicate, []rds.Column{{}}
	}

	if strings.HasSuffix(predicateStr, "is true") {
		rhsColumn := strings.TrimSpace(strings.Split(predicateStr, "is true")[0])
		rhsColumn = rhsColumn[3:]
		predicate := rds.Predicate{
			PredicateStr: predicateStr,
			LeftColumn: rds.Column{
				ColumnId:   rhsColumn,
				ColumnType: rds_config.StringType,
			},
		}
		column := rds.Column{
			ColumnId:   rhsColumn,
			ColumnType: rds_config.StringType,
		}
		return predicate, []rds.Column{column}
	}

	var columns []rds.Column
	var predicate rds.Predicate
	// 多项式谓词
	if strings.Contains(predicateStr, "poly(") {
		return AnalysisPolyStr(predicateStr)
	}
	// 相似度谓词
	if strings.Contains(predicateStr, "similar(") {
		return AnalysisSimilarStr(predicateStr)
	}
	if strings.Contains(predicateStr, "decode") {
		return AnalysisDecodeStr(predicateStr)
	}
	if strings.Contains(predicateStr, "category") {
		return AnalysisCategoryStr(predicateStr)
	}
	if strings.Contains(predicateStr, "titleSimilar") {
		return AnalysisTitleSimilarStr(predicateStr)
	}
	if strings.Contains(predicateStr, "contains") {
		return AnalysisContainsStr(predicateStr)
	}
	if strings.Contains(predicateStr, "notContains") {
		return AnalysisNotContainsStr(predicateStr)
	}
	if strings.HasPrefix(predicateStr, "list(") {
		return AnalysisListStr(predicateStr, tables)
	}
	splitSymbol := ""
	if strings.Contains(predicateStr, rds_config.GreaterE) {
		splitSymbol = rds_config.GreaterE
	} else if strings.Contains(predicateStr, rds_config.Less) {
		splitSymbol = rds_config.Less
	} else if strings.Contains(predicateStr, rds_config.Equal) {
		splitSymbol = rds_config.Equal
	}
	arr := strings.Split(predicateStr, splitSymbol)
	left := strings.TrimSpace(strings.Split(arr[0], "t0.")[1])
	column2Type := make(map[string]string)
	if len(tables) > 0 {
		column2Type = tables[0].ColumnsType
	} else {
		_, _, column2Type, _ = storage_utils.GetSchemaInfo(exeTableId[0])
	}
	columnType := column2Type[left]
	leftColumn := rds.Column{
		TableId:    exeTableId[0],
		ColumnId:   left,
		ColumnType: columnType,
	}
	columns = append(columns, leftColumn)
	// 多行谓词
	if strings.Contains(predicateStr, "t1.") {
		right := strings.TrimSpace(strings.Split(arr[1], "t1.")[1])
		rightColumn := rds.Column{
			ColumnId:   right,
			ColumnType: "",
		}
		columns = append(columns, rightColumn)
		predicate = rds.Predicate{
			PredicateStr:  predicateStr,
			LeftColumn:    leftColumn,
			RightColumn:   rightColumn,
			ConstantValue: nil,
			SymbolType:    rds_config.Equal,
			PredicateType: 1,
			Support:       0,
		}
		return predicate, columns
	}
	if strings.Contains(predicateStr, "t2.") {
		//_, _, column2Type, _ := storage_utils.GetSchemaInfo(exeTableId[0])
		//if column2Type == nil{
		//	column2Type = tables[0].ColumnsType
		//}
		right := strings.TrimSpace(strings.Split(arr[1], "t2.")[1])
		rightColumn := rds.Column{
			TableId:    exeTableId[0],
			ColumnId:   right,
			ColumnType: column2Type[right],
		}
		columns = append(columns, rightColumn)
		predicate = rds.Predicate{
			PredicateStr:  predicateStr,
			LeftColumn:    leftColumn,
			RightColumn:   rightColumn,
			ConstantValue: nil,
			SymbolType:    rds_config.Equal,
			PredicateType: 1,
			Support:       0,
		}
		return predicate, columns
	}
	// 单行谓词

	var constantValue interface{} = strings.TrimSpace(arr[1])
	if columnType == rds_config.FloatType {
		constantValue, _ = strconv.ParseFloat(fmt.Sprint(constantValue), 64)
	} else if columnType == rds_config.IntType {
		constantValue, _ = strconv.ParseInt(fmt.Sprint(constantValue), 10, 64)
	}
	predicate = rds.Predicate{
		PredicateStr:  predicateStr,
		LeftColumn:    leftColumn,
		RightColumn:   rds.Column{},
		ConstantValue: constantValue,
		SymbolType:    splitSymbol,
		PredicateType: 0,
		Support:       0,
	}
	return predicate, columns
}

func AnalysisPolyStr(str string) (rds.Predicate, []rds.Column) {
	predicate := rds.Predicate{
		PredicateStr:  str,
		LeftColumn:    rds.Column{},
		RightColumn:   rds.Column{},
		ConstantValue: nil,
		SymbolType:    rds_config.Poly,
		PredicateType: 0,
		Support:       0,
	}
	return predicate, []rds.Column{{}}
}

func AnalysisSimilarStr(str string) (rds.Predicate, []rds.Column) {
	arr := strings.Split(str[8:len(str)-1], ",")
	//similarType := strings.TrimSpace(arr[0])
	//threshold, _ := strconv.ParseFloat(strings.TrimSpace(arr[1]),64)
	left := strings.TrimSpace(strings.Split(arr[0], "t0.")[1])
	leftColumn := rds.Column{
		ColumnId:   left,
		ColumnType: "",
	}
	right := strings.TrimSpace(strings.Split(arr[1], "t1.")[1])
	rightColumn := rds.Column{
		ColumnId:   right,
		ColumnType: "",
	}
	columns := []rds.Column{leftColumn, rightColumn}
	predicate := rds.Predicate{
		PredicateStr:  str,
		LeftColumn:    leftColumn,
		RightColumn:   rightColumn,
		ConstantValue: nil,
		SymbolType:    rds_config.Similar,
		PredicateType: 1,
		Support:       0,
	}
	return predicate, columns
}

func AnalysisDecodeStr(str string) (rds.Predicate, []rds.Column) {
	lhsColumn := strings.TrimSpace(strings.Split(str, "decode(")[1])
	lhsColumn = lhsColumn[3 : len(lhsColumn)-1]
	predicate := rds.Predicate{
		PredicateStr: str,
		LeftColumn: rds.Column{
			ColumnId:   lhsColumn,
			ColumnType: rds_config.StringType,
		},
		RightColumn:   rds.Column{},
		ConstantValue: nil,
		SymbolType:    rds_config.Decode,
	}
	lhs := rds.Column{
		ColumnId:   lhsColumn,
		ColumnType: rds_config.StringType,
	}
	return predicate, []rds.Column{lhs}
}

func AnalysisCategoryStr(str string) (rds.Predicate, []rds.Column) {
	lhsColumn := strings.TrimSpace(strings.Split(str, "category(")[1])
	lhsColumn = lhsColumn[3 : len(lhsColumn)-1]
	predicate := rds.Predicate{
		PredicateStr: str,
		LeftColumn: rds.Column{
			ColumnId:   lhsColumn,
			ColumnType: rds_config.StringType,
		},
		RightColumn:   rds.Column{},
		ConstantValue: nil,
		SymbolType:    rds_config.Category,
	}
	lhs := rds.Column{
		ColumnId:   lhsColumn,
		ColumnType: rds_config.StringType,
	}
	return predicate, []rds.Column{lhs}
}

func AnalysisTitleSimilarStr(str string) (rds.Predicate, []rds.Column) {
	lhsColumn := strings.TrimSpace(strings.Split(str, "titleSimilar(")[1])
	lhsColumn = lhsColumn[3 : len(lhsColumn)-1]
	predicate := rds.Predicate{
		PredicateStr: str,
		LeftColumn: rds.Column{
			ColumnId:   lhsColumn,
			ColumnType: rds_config.StringType,
		},
		RightColumn:   rds.Column{},
		ConstantValue: nil,
		SymbolType:    rds_config.TitleSimilar,
	}
	lhs := rds.Column{
		ColumnId:   lhsColumn,
		ColumnType: rds_config.StringType,
	}
	return predicate, []rds.Column{lhs}
}

func AnalysisContainsStr(str string) (rds.Predicate, []rds.Column) {
	lhsColumn := strings.TrimSpace(strings.Split(str, "contains(")[1])
	columnAndValue := strings.Split(lhsColumn, ",")
	lhsColumn = strings.TrimSpace(columnAndValue[0])
	lhsColumn = lhsColumn[3:]
	value := strings.TrimSpace(columnAndValue[1])
	value = value[0 : len(value)-1]
	predicate := rds.Predicate{
		PredicateStr: str,
		LeftColumn: rds.Column{
			ColumnId:   lhsColumn,
			ColumnType: rds_config.StringType,
		},
		RightColumn:   rds.Column{},
		ConstantValue: value,
		SymbolType:    rds_config.Contains,
	}
	lhs := rds.Column{
		ColumnId:   lhsColumn,
		ColumnType: rds_config.StringType,
	}
	return predicate, []rds.Column{lhs}
}

func AnalysisNotContainsStr(str string) (rds.Predicate, []rds.Column) {
	lhsColumn := strings.TrimSpace(strings.Split(str, "notContains(")[1])
	columnAndValue := strings.Split(lhsColumn, ",")
	lhsColumn = strings.TrimSpace(columnAndValue[0])
	lhsColumn = lhsColumn[3:]
	value := strings.TrimSpace(columnAndValue[1])
	value = value[0 : len(value)-1]
	predicate := rds.Predicate{
		PredicateStr: str,
		LeftColumn: rds.Column{
			ColumnId:   lhsColumn,
			ColumnType: rds_config.StringType,
		},
		RightColumn:   rds.Column{},
		ConstantValue: value,
		SymbolType:    rds_config.NotContains,
	}
	lhs := rds.Column{
		ColumnId:   lhsColumn,
		ColumnType: rds_config.StringType,
	}
	return predicate, []rds.Column{lhs}
}

func AnalysisListStr(str string, tables []*TableInfo) (rds.Predicate, []rds.Column) {
	columnsStr := strings.Split(str, rds_config.Equal)
	lhsColumnStr := strings.TrimSpace(columnsStr[0])
	rhsColumnStr := strings.TrimSpace(columnsStr[1])

	lhsColumns := strings.Split(lhsColumnStr, ",")
	lhsColumn := strings.TrimSpace(lhsColumns[0])
	lhsColumn = lhsColumn[8:]

	columnTypeLeft := tables[1].ColumnsType
	columnTypeRight := tables[2].ColumnsType
	var lhs2 rds.Column
	var rhs2 rds.Column
	if len(lhsColumns) == 2 {
		lhsColumn2 := strings.TrimSpace(lhsColumns[1])
		lhsColumn2 = lhsColumn2[3 : len(lhsColumn2)-1]
		lhs2 = rds.Column{
			TableId:    tables[1].TableId,
			ColumnId:   lhsColumn2,
			ColumnType: columnTypeLeft[lhsColumn2],
		}
	} else {
		lhsColumn = lhsColumn[:len(lhsColumn)-1]
	}

	rhsColumns := strings.Split(rhsColumnStr, ",")
	rhsColumn := strings.TrimSpace(rhsColumns[0])
	rhsColumn = rhsColumn[8:]
	if len(rhsColumns) == 2 {
		rhsColumn2 := strings.TrimSpace(rhsColumns[1])
		rhsColumn2 = rhsColumn2[3 : len(rhsColumn2)-1]
		rhs2 = rds.Column{
			TableId:    tables[2].TableId,
			ColumnId:   rhsColumn2,
			ColumnType: columnTypeRight[rhsColumn2],
		}
	} else {
		rhsColumn = rhsColumn[:len(rhsColumn)-1]
	}

	predicate := rds.Predicate{
		PredicateStr: str,
		LeftColumn: rds.Column{
			TableId:    tables[1].TableId,
			ColumnId:   lhsColumn,
			ColumnType: columnTypeLeft[lhsColumn],
		},
		RightColumn: rds.Column{
			TableId:    tables[2].TableId,
			ColumnId:   rhsColumn,
			ColumnType: columnTypeRight[rhsColumn],
		},
		SymbolType: rds_config.List,
	}
	lhs := rds.Column{
		TableId:    tables[1].TableId,
		ColumnId:   lhsColumn,
		ColumnType: columnTypeLeft[lhsColumn],
	}
	rhs := rds.Column{
		TableId:    tables[2].TableId,
		ColumnId:   rhsColumn,
		ColumnType: columnTypeRight[rhsColumn],
	}
	columns := make([]rds.Column, 0)
	if lhs2.ColumnId != "" && rhs2.ColumnId != "" {
		columns = []rds.Column{lhs, lhs2, rhs, rhs2}
	} else {
		columns = []rds.Column{lhs, rhs}
	}
	return predicate, columns
}

func GetTable2Columns(predicates []rds.Predicate) (table2Columns map[string][]string) {
	table2Columns = make(map[string][]string)
	keyMap := make(map[string]struct{})
	for _, predicate := range predicates {
		key := predicate.LeftColumn.TableId + "_" + predicate.LeftColumn.ColumnId
		if _, exist := keyMap[key]; !exist {
			keyMap[key] = struct{}{}
			table2Columns[predicate.LeftColumn.TableId] = append(table2Columns[predicate.LeftColumn.TableId], predicate.LeftColumn.ColumnId)
		}
		if predicate.PredicateType != 0 {
			key := predicate.RightColumn.TableId + "_" + predicate.RightColumn.ColumnId
			if _, exist := keyMap[key]; !exist {
				keyMap[key] = struct{}{}
				table2Columns[predicate.RightColumn.TableId] = append(table2Columns[predicate.RightColumn.TableId], predicate.RightColumn.ColumnId)
			}
		}
	}
	return table2Columns
}

func HasCrossTablePredicate(predicates []*rds.Predicate) bool {
	for _, predicate := range predicates {
		// 常数谓词的直接跳过
		if predicate.PredicateType == 0 {
			continue
		}
		if predicate.LeftColumn.TableId != predicate.RightColumn.TableId {
			return true
		}
	}
	return false
}

func GetMLPredicate(predicates []rds.Predicate) []rds.Predicate {
	var mlPredicates []rds.Predicate
	for _, predicate := range predicates {
		// 把ML谓词加入到ml谓词列表中
		if predicate.SymbolType == enum.Similar || predicate.SymbolType == enum.ML {
			mlPredicates = append(mlPredicates, predicate)
		}
	}
	return mlPredicates
}

func GetMLPredicateNew(predicates []rds.Predicate) (mlPredicates []rds.Predicate, notMLPredicates []rds.Predicate) {
	for _, predicate := range predicates {
		// 把ML谓词加入到ml谓词列表中
		if predicate.SymbolType == enum.Similar || predicate.SymbolType == enum.ML {
			mlPredicates = append(mlPredicates, predicate)
		} else {
			notMLPredicates = append(notMLPredicates, predicate)
		}
	}
	return mlPredicates, notMLPredicates
}

func GetPredicateColumnIndexNew(predicate rds.Predicate) (int, int) {
	isConstant := predicate.PredicateType == 0
	if isConstant {
		return predicate.LeftColumn.ColumnIndex, -1
	}
	return predicate.LeftColumn.ColumnIndex, predicate.RightColumn.ColumnIndex
}

func GetPredicateColumnIndex(predicate rds.Predicate) (int, int) {
	// todo 现在临时方案,只有=号
	columnArr := strings.Split(predicate.PredicateStr, rds_config.Equal)
	leftIndex, err := strconv.Atoi(strings.Split(strings.Split(columnArr[0], ".")[0], "t")[1])
	if err != nil {
		logger.Errorf("convert string to int err:%v", err)
		return 0, 0
	}
	rightIndex, err := strconv.Atoi(strings.Split(strings.Split(columnArr[1], ".")[0], "t")[1])
	if err != nil {
		logger.Errorf("convert string to int err:%v", err)
		return 0, 0
	}
	return leftIndex, rightIndex
}

func CopyPredicate(predicate rds.Predicate) rds.Predicate {
	predicate_ := rds.Predicate{
		PredicateStr:       predicate.PredicateStr,
		LeftColumn:         predicate.LeftColumn,
		RightColumn:        predicate.RightColumn,
		GroupByColumn:      predicate.GroupByColumn,
		ConstantValue:      predicate.ConstantValue,
		ConstantIndexValue: predicate.ConstantIndexValue,
		SymbolType:         predicate.SymbolType,
		PredicateType:      predicate.PredicateType,
		UDFName:            predicate.UDFName,
		Threshold:          predicate.Threshold,
		Support:            predicate.Support,
		Intersection:       predicate.Intersection,
	}
	return predicate_
}

func GenerateConnectPredicate(predicate rds.Predicate, tableId2index map[string]int) []rds.Predicate {
	leftTableId := predicate.LeftColumn.TableId
	rightTableId := predicate.RightColumn.TableId
	leftColumnIndex, rightColumnIndex := GetPredicateColumnIndexNew(predicate)
	if _, ok := tableId2index[leftTableId]; !ok {
		tableId2index[leftTableId] = len(tableId2index)
	}
	if _, ok := tableId2index[rightTableId]; !ok {
		tableId2index[rightTableId] = len(tableId2index)
	}
	changedLeftColumnIndex := 2*tableId2index[leftTableId] + leftColumnIndex%2
	changedRightColumnIndex := 2*tableId2index[rightTableId] + rightColumnIndex%2
	predicate.PredicateStr = fmt.Sprintf("t%d.%s%st%d.%s", changedLeftColumnIndex, predicate.LeftColumn.ColumnId, predicate.SymbolType, changedRightColumnIndex, predicate.RightColumn.ColumnId)
	addedPredicate := CopyPredicate(predicate)
	addedPredicate.PredicateStr = fmt.Sprintf("t%d.%s%st%d.%s", changedLeftColumnIndex+1, predicate.LeftColumn.ColumnId, predicate.SymbolType, changedRightColumnIndex+1, predicate.RightColumn.ColumnId)

	predicate.LeftColumn.ColumnIndex = changedLeftColumnIndex
	predicate.RightColumn.ColumnIndex = changedRightColumnIndex
	addedPredicate.LeftColumn.ColumnIndex = changedLeftColumnIndex + 1
	addedPredicate.RightColumn.ColumnIndex = changedRightColumnIndex + 1

	GeneratePredicateStrNew(&predicate)
	GeneratePredicateStrNew(&addedPredicate)
	return []rds.Predicate{predicate, addedPredicate}
}

func GenerateConnectPredicateSingle(predicate rds.Predicate, tableId2index map[string]int) rds.Predicate {
	leftTableId := predicate.LeftColumn.TableId
	rightTableId := predicate.RightColumn.TableId
	leftColumnIndex, rightColumnIndex := GetPredicateColumnIndexNew(predicate)
	if _, ok := tableId2index[leftTableId]; !ok {
		tableId2index[leftTableId] = len(tableId2index)
	}
	if _, ok := tableId2index[rightTableId]; !ok {
		tableId2index[rightTableId] = len(tableId2index)
	}
	changedLeftColumnIndex := 2*tableId2index[leftTableId] + leftColumnIndex%2
	changedRightColumnIndex := 2*tableId2index[rightTableId] + rightColumnIndex%2
	predicate.PredicateStr = fmt.Sprintf("t%d.%s%st%d.%s", changedLeftColumnIndex, predicate.LeftColumn.ColumnId, predicate.SymbolType, changedRightColumnIndex, predicate.RightColumn.ColumnId)
	predicate.LeftColumn.ColumnIndex = changedLeftColumnIndex
	predicate.RightColumn.ColumnIndex = changedRightColumnIndex

	GeneratePredicateStrNew(&predicate)
	return predicate
}

func CheckPredicateIndex(predicate *rds.Predicate, tableId2index map[string]int) {
	leftTableId := predicate.LeftColumn.TableId
	rightTableId := predicate.RightColumn.TableId
	leftColumnIndex, rightColumnIndex := GetPredicateColumnIndexNew(*predicate)
	if _, ok := tableId2index[leftTableId]; !ok {
		tableId2index[leftTableId] = len(tableId2index)
	}
	if _, ok := tableId2index[rightTableId]; !ok {
		tableId2index[rightTableId] = len(tableId2index)
	}
	changedLeftColumnIndex := 2*tableId2index[leftTableId] + leftColumnIndex%2
	changedRightColumnIndex := 2*tableId2index[rightTableId] + rightColumnIndex%2
	// todo 目前只有等于号,ml的有问题,只用用=连接
	predicate.PredicateStr = fmt.Sprintf("t%d.%s%st%d.%s", changedLeftColumnIndex, predicate.LeftColumn.ColumnId, rds_config.Equal, changedRightColumnIndex, predicate.RightColumn.ColumnId)
	predicate.LeftColumn.ColumnIndex = changedLeftColumnIndex
	predicate.RightColumn.ColumnIndex = changedRightColumnIndex

	GeneratePredicateStrNew(predicate)
}

func CheckPredicatesIndex(predicates []rds.Predicate, tableId2index map[string]int) {
	for i := range predicates {
		CheckPredicateIndex(&predicates[i], tableId2index)
	}
}

func CheckPredicatesIsConnectedGraph(predicates []rds.Predicate) bool {
	edges := make([][2]int, len(predicates))
	for i, predicate := range predicates {
		//edges[i][0], edges[i][1] = GetPredicateColumnIndexNew(predicate)
		edges[i][0], edges[i][1] = predicate.LeftColumn.ColumnIndex, predicate.RightColumn.ColumnIndex
	}
	return isConnectedGraph(edges)
}

func GenerateRhs(rhsInfos []request.PredicateInfo, predicates []rds.Predicate, tableIds []string) []rds.Predicate {
	relatedColumns := make(map[string]map[string]bool)
	result := make([]rds.Predicate, 0)
	for _, rhsInfo := range rhsInfos {
		leftTableId := tableIds[rhsInfo.LeftTableIndex]
		if _, ok := relatedColumns[leftTableId]; !ok {
			relatedColumns[leftTableId] = make(map[string]bool)
		}
		rightTableId := tableIds[rhsInfo.RightTableIndex]
		if _, ok := relatedColumns[rightTableId]; !ok {
			relatedColumns[rightTableId] = make(map[string]bool)
		}
		relatedColumns[leftTableId][rhsInfo.LeftTableColumn] = true
		relatedColumns[rightTableId][rhsInfo.RightTableColumn] = true
	}
	for _, predicate := range predicates {
		if _, ok := relatedColumns[predicate.LeftColumn.TableId]; ok {
			if relatedColumns[predicate.LeftColumn.TableId][predicate.LeftColumn.ColumnId] {
				result = append(result, predicate)
				continue
			}
		}
		if _, ok := relatedColumns[predicate.RightColumn.TableId]; ok {
			if relatedColumns[predicate.RightColumn.TableId][predicate.RightColumn.ColumnId] {
				result = append(result, predicate)
				continue
			}
		}
	}
	return result
}

func FilterRhs(predicates []rds.Predicate, joinKeyPre []rds.Predicate) []rds.Predicate {
	// 主外键表列收集，不出现在 Y
	keyTableColumnMap := make(map[string]map[string]bool)
	for _, keyPre := range joinKeyPre {
		if _, ok := keyTableColumnMap[keyPre.LeftColumn.TableId]; !ok {
			keyTableColumnMap[keyPre.LeftColumn.TableId] = map[string]bool{}
		}
		keyTableColumnMap[keyPre.LeftColumn.TableId][keyPre.LeftColumn.ColumnId] = true
		if _, ok := keyTableColumnMap[keyPre.RightColumn.TableId]; !ok {
			keyTableColumnMap[keyPre.RightColumn.TableId] = map[string]bool{}
		}
		keyTableColumnMap[keyPre.RightColumn.TableId][keyPre.RightColumn.ColumnId] = true
	}
	var ans []rds.Predicate
	for _, pre := range predicates {
		if keyTableColumnMap[pre.LeftColumn.TableId] != nil && keyTableColumnMap[pre.LeftColumn.TableId][pre.LeftColumn.ColumnId] {
			continue
		}
		if keyTableColumnMap[pre.RightColumn.TableId] != nil && keyTableColumnMap[pre.RightColumn.TableId][pre.RightColumn.ColumnId] {
			continue
		}
		ans = append(ans, pre)
	}
	return ans
}

func GetAllRelatedPres(node *task_tree.TaskTree) []rds.Predicate {
	var merged []rds.Predicate

	//merged = append(merged, node.Lhs...)
	//merged = append(merged, node.LhsCandidate...)
	//merged = append(merged, node.LhsCrossCandidate...)
	for _, pre := range node.Lhs {
		//CheckPredicateIndex(&pre, node.TableId2index)
		merged = append(merged, pre)
	}
	for _, pre := range node.LhsCandidate {
		//CheckPredicateIndex(&pre, node.TableId2index)
		merged = append(merged, pre)
	}
	for _, pre := range node.LhsCrossCandidate {
		merged = append(merged, GenerateConnectPredicate(pre, node.TableId2index)...)
	}
	//CheckPredicateIndex(&node.Rhs, node.TableId2index)
	merged = append(merged, node.Rhs)
	return merged
}

func CheckSatisfyPredicateSupp(predicates []rds.Predicate, tableIndex map[string]int, supp float64) bool {
	if len(tableIndex) < 2 {
		return true
	}
	minSupp := float64(1)
	allCrossPredicates := true
	for _, predicate := range predicates {
		if predicate.PredicateType != 2 {
			allCrossPredicates = false
			if predicate.Support < minSupp {
				minSupp = predicate.Support
			}
		}
	}
	return allCrossPredicates || minSupp < supp
}

func CheckHasML(predicates []rds.Predicate) bool {
	for _, predicate := range predicates {
		if predicate.SymbolType == enum.Similar || predicate.SymbolType == enum.ML {
			return true
		}
	}
	return false
}

func SortPredicatesRelated(lhs []rds.Predicate) []rds.Predicate {
	lhs = slices.Clone(lhs)
	if len(lhs) < 2 {
		return lhs
	}

	var sortedLhs []rds.Predicate
	var visitedTableIndexes = map[int]bool{}
	var first rds.Predicate
	first = lhs[0]
	if first.PredicateType == 1 {
		lhs = lhs[1:]
	} else {
		for i, predicate := range lhs {
			if predicate.PredicateType == 1 {
				first = predicate
				lhs = append(lhs[:i], lhs[i+1:]...)
				break
			}
		}
	}

	leftTableIndex, rightTableIndex := GetPredicateColumnIndexNew(first)
	visitedTableIndexes[leftTableIndex] = true
	visitedTableIndexes[rightTableIndex] = true

	sortedLhs = append(sortedLhs, first)

	for len(lhs) > 0 {
		var i int
		for i = range lhs {
			leftTableIndex, rightTableIndex := GetPredicateColumnIndexNew(lhs[i])
			if visitedTableIndexes[leftTableIndex] || visitedTableIndexes[rightTableIndex] {
				break
			}
		}

		sortedLhs = append(sortedLhs, lhs[i])
		lhs = append(lhs[:i], lhs[i+1:]...)
	}

	return sortedLhs
}

// SortPredicatesRelatedNew 查错、纠错使用,修改了GetPredicateColumnIndex方法，等规则发现也改了后，再调用SortPredicatesRelated方法
func SortPredicatesRelatedNew(lhs []rds.Predicate) []rds.Predicate {
	lhs = slices.Clone(lhs)
	if len(lhs) < 2 {
		return lhs
	}

	var sortedLhs []rds.Predicate
	var visitedTableIndexes = map[int]bool{}
	var first rds.Predicate
	first = lhs[0]
	if first.PredicateType == 1 {
		lhs = lhs[1:]
	} else {
		for i, predicate := range lhs {
			if predicate.PredicateType == 1 {
				first = predicate
				lhs = append(lhs[:i], lhs[i+1:]...)
				break
			}
		}
	}
	// 多表单行规则
	if first.PredicateType != 1 {
		lhs = lhs[1:]
	}

	leftTableIndex, rightTableIndex := GetPredicateColumnIndexNew(first)
	visitedTableIndexes[leftTableIndex] = true
	visitedTableIndexes[rightTableIndex] = true

	sortedLhs = append(sortedLhs, first)

	for len(lhs) > 0 {
		var i int
		for i = range lhs {
			leftTableIndex, rightTableIndex := GetPredicateColumnIndexNew(lhs[i])
			if visitedTableIndexes[leftTableIndex] || visitedTableIndexes[rightTableIndex] {
				break
			}
		}

		sortedLhs = append(sortedLhs, lhs[i])
		lhs = append(lhs[:i], lhs[i+1:]...)
	}

	return sortedLhs
}

func GeneratePredicateStr(leftColumnName, rightColumnName string, leftColumnIndex, rightColumnIndex int) string {
	return fmt.Sprintf("t%d.%s=t%d.%s", leftColumnIndex, leftColumnName, rightColumnIndex, rightColumnName)
}

func GeneratePredicateStrNew(predicate *rds.Predicate) {
	leftColName := predicate.LeftColumn.ColumnId
	rightColName := predicate.RightColumn.ColumnId
	if predicate.PredicateType == 0 { //常数谓词
		predicate.PredicateStr = fmt.Sprintf("t%d.%s%v%v", predicate.LeftColumn.ColumnIndex, leftColName, predicate.SymbolType, predicate.ConstantValue)
		return
	}
	if udf_column.IsUdfColumn(leftColName) {
		_, columnNames, _, _ := udf_column.ParseUdfColumn(leftColName)
		for i := range columnNames {
			columnNames[i] = fmt.Sprintf("t%d.%s", predicate.LeftColumn.ColumnIndex, columnNames[i])
		}
		leftColName = strings.Join(columnNames, "||")
	}
	if udf_column.IsUdfColumn(rightColName) {
		_, columnNames, _, _ := udf_column.ParseUdfColumn(rightColName)
		for i := range columnNames {
			columnNames[i] = fmt.Sprintf("t%d.%s", predicate.RightColumn.ColumnIndex, columnNames[i])
		}
		rightColName = strings.Join(columnNames, "||")
	}
	if predicate.SymbolType == enum.ML {
		predicate.PredicateStr = fmt.Sprintf("%s('%s', %s, %s)", predicate.SymbolType, predicate.UDFName, leftColName, rightColName)
	} else if predicate.SymbolType == enum.Similar {
		predicate.PredicateStr = fmt.Sprintf("%s('%s', %s, %s, %0.2f)", predicate.SymbolType, predicate.UDFName, leftColName, rightColName, predicate.Threshold)
	} else {
		predicate.PredicateStr = fmt.Sprintf("t%d.%s=t%d.%s", predicate.LeftColumn.ColumnIndex, leftColName, predicate.RightColumn.ColumnIndex, rightColName)
	}
}
