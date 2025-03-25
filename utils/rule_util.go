package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"gitlab.grandhoo.com/rock/storage/storage2/utils"
	"sort"
	"strings"

	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
)

func SplitRule(rule string) (map[string]bool, string) {
	lhs := make(map[string]bool)
	tmp := strings.Split(rule, "->")
	rhs := strings.TrimSpace(tmp[1])
	for _, predicate := range strings.Split(tmp[0], "^") {
		lhs[strings.TrimSpace(predicate)] = true
	}
	return lhs, rhs
}

func GetRuleJson(rule rds.Rule) string {
	byteBuf := bytes.NewBuffer([]byte{})
	encoder := json.NewEncoder(byteBuf)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(rule)
	if err != nil {
		fmt.Println("error: ", err)
		return ""
	}
	return byteBuf.String()
	//jsonBytes, err := json.Marshal(rule)
	//if err != nil {
	//	fmt.Println("error: ", err)
	//	return ""
	//}
	//return string(jsonBytes)
}

func GetFormatRule(ruleJsonStr string) rds.Rule {
	rule := &rds.Rule{}
	err := json.Unmarshal([]byte(ruleJsonStr), &rule)
	if err != nil {
		fmt.Println("error: ", err)
		return rds.Rule{}
	}
	return *rule
}

func IsAbandoned(ree string, abandonedRules []string) bool {
	for _, rule := range abandonedRules {
		if ree == rule {
			return true
		}
	}
	return false
}

func GenerateTableIdStr(tableId string, isSingle bool) string {
	str := tableId + "(t0)^"
	if !isSingle {
		str += tableId
		str += "(t1)^"
	}
	return str
}
func GenerateMultiTableIdStr(tableId2index map[string]int) string {
	tableIdArr := make([]string, len(tableId2index))
	for s, i := range tableId2index {
		tableIdArr[i] = s
	}
	arr := make([]string, len(tableIdArr)*2)
	for i, tableId := range tableIdArr {
		arr[i*2] = fmt.Sprintf("%s(t%d)", tableId, i*2)
		arr[i*2+1] = fmt.Sprintf("%s(t%d)", tableId, i*2+1)
	}
	return strings.Join(arr, "^")
}

func IsSingle(lhs []rds.Predicate, rhs rds.Predicate) bool {
	if rhs.PredicateType != 0 {
		return false
	}
	for _, p := range lhs {
		if p.PredicateType != 0 {
			return false
		}
	}
	return true
}

func GenerateMultiTableIdStrNew2(tableId2index map[string]int, lhs []rds.Predicate) string {
	var usedTableIndex []int
	for _, p := range lhs {
		usedTableIndex = append(usedTableIndex, p.LeftColumn.ColumnIndex)
		if p.RightColumn.ColumnId != "" {
			usedTableIndex = append(usedTableIndex, p.RightColumn.ColumnIndex)
		}
	}
	usedTableIndex = utils.Distinct(usedTableIndex)
	sort.Ints(usedTableIndex)

	var tableIndexArr []int
	tableIndex2id := make(map[int]string)
	for tableId, i := range tableId2index {
		tableIndexArr = append(tableIndexArr, i)
		tableIndex2id[i] = tableId
	}

	arr := make([]string, 0, len(tableIndexArr)*2)
	for _, tid := range usedTableIndex {
		tableId := tableIndex2id[tid/2]
		arr = append(arr, fmt.Sprintf("%s(t%d)", tableId, tid))
	}
	return strings.Join(arr, "^")
}

func GenerateTableIdStrMultiTable(table2Index map[string]int, isSingle bool) (string, map[string]string) {
	index2Table := make(map[int]string)
	alias2Table := make(map[string]string)
	for table, index := range table2Index {
		index2Table[index] = table
	}

	strList := make([]string, 0)
	for i := 0; i < 2*len(index2Table); i += 2 {
		alias := "t" + fmt.Sprint(i)
		table := index2Table[i]
		alias2Table[alias] = table
		str := table + "(" + alias + ")"
		strList = append(strList, str)
		if !isSingle {
			alias := "t" + fmt.Sprint(i+1)
			alias2Table[alias] = table
			str := table + "(" + alias + ")"
			strList = append(strList, str)
		}
	}
	return strings.Join(strList, "^") + "^", alias2Table
}

func AnalysisRee(ree string, correctTableId []string, tables []*TableInfo) rds.Rule {
	leftTableId := ""
	//rightTableId := ""
	xyArray := strings.Split(ree, "->")
	y := strings.TrimSpace(xyArray[1])
	xArray := strings.Split(strings.TrimSpace(xyArray[0]), "^")
	var lhsPredicates []rds.Predicate
	var lhsColumns []rds.Column
	lhsColumnsMap := make(map[string]bool)
	for _, predicateStr := range xArray {
		predicateStr = strings.TrimSpace(predicateStr)
		// 获取左表id
		if strings.Contains(predicateStr, "(t0)") {
			leftTableId = strings.Split(predicateStr, "(t0)")[0]
			continue
		}
		// 获取右表id
		if strings.Contains(predicateStr, "(t1)") {
			//rightTableId = strings.Split(predicateStr, "(t1)")[0]
			continue
		}
		predicate, columns := AnalysisPredicateStr(predicateStr, correctTableId, tables)
		lhsPredicates = append(lhsPredicates, predicate)
		for _, column := range columns {
			if !lhsColumnsMap[column.TableId+"_"+column.ColumnId] {
				lhsColumns = append(lhsColumns, column)
				lhsColumnsMap[column.TableId+"_"+column.ColumnId] = true
			}
		}
	}
	rhsPredicate, rhsColumns := AnalysisPredicateStr(y, correctTableId, tables)
	rule := rds.Rule{
		TableId:       leftTableId,
		Ree:           ree,
		LhsPredicates: lhsPredicates,
		LhsColumns:    lhsColumns,
		Rhs:           rhsPredicate,
		RhsColumn:     rhsColumns[0],
		CR:            0,
		FTR:           0,
	}
	return rule
}

func GenerateTableIdStrByIndex2Table(index2Table map[string]string) string {
	indexes := make([]string, 0)
	for index := range index2Table {
		indexes = append(indexes, index)
	}
	sort.Strings(indexes)

	list := make([]string, len(indexes))
	for i, index := range indexes {
		list[i] = fmt.Sprintf("%s(%s)", index2Table[index], index)
	}
	return strings.Join(list, "^")
}

func GetRelatedTable(r rds.Rule) []string {
	m := make(map[string]bool)
	for _, pre := range r.LhsPredicates {
		m[pre.LeftColumn.TableId] = true
		rightTableId := pre.RightColumn.TableId
		if rightTableId != "" {
			m[rightTableId] = true
		}
	}
	m[r.Rhs.LeftColumn.TableId] = true

	rhsRightTid := r.Rhs.RightColumn.TableId
	if rhsRightTid != "" {
		m[rhsRightTid] = true
	}
	result := make([]string, len(m))
	i := 0
	for id := range m {
		result[i] = id
		i++
	}
	return result
}
