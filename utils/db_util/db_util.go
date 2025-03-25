package db_util

import (
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/enum"
	"strconv"
	"strings"

	"gitlab.grandhoo.com/rock/rock-share/global/db"
	"gitlab.grandhoo.com/rock/rock-share/global/model/po"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/utils"
)

func WriteRule(taskId string, tableId string, data rds.Rule) int {
	taskIdi, _ := strconv.ParseInt(taskId, 10, 64)
	tableIdi, _ := strconv.ParseInt(tableId, 10, 64)
	pgRule := po.Rule{
		Id:            0,
		TaskId:        taskIdi,
		ConflictCount: 0,
		Ree:           data.Ree,
		ReeJson:       utils.GetRuleJson(data),
		ReeType:       data.Rhs.PredicateType,
		Status:        enum.RULE_UNUSED,
	}
	err := po.CreateRule(&pgRule, db.DB)
	if err != nil {
		fmt.Println(err)
		return 0
	}
	ruleId := int(pgRule.Id)

	var ruleColumns []po.RuleColumn
	for _, column := range data.LhsColumns {
		columnId, _ := strconv.ParseInt(column.ColumnId, 10, 64)
		ruleColumn := po.RuleColumn{
			RuleId:   pgRule.Id,
			BindId:   tableIdi,
			ColumnId: columnId,
		}
		ruleColumns = append(ruleColumns, ruleColumn)
	}
	columnId, _ := strconv.ParseInt(data.RhsColumn.ColumnId, 10, 64)
	ruleColumn := po.RuleColumn{
		RuleId:   pgRule.Id,
		BindId:   tableIdi,
		ColumnId: columnId,
	}
	ruleColumns = append(ruleColumns, ruleColumn)
	err = po.CreateRuleColumns(&ruleColumns, db.DB)
	if err != nil {
		fmt.Println(err)
		return 0
	}

	return ruleId
}

func WriteRule2DB(r rds.Rule, taskId, tableId int64, conflictCount int) int {
	yColumnId := int64(0)
	yColumnId2 := int64(0)
	if r.RuleType != 2 { //正则的没有Y列
		yColumnId, _ = strconv.ParseInt(r.Rhs.LeftColumn.ColumnId, 10, 64)
		yColumnId2, _ = strconv.ParseInt(r.Rhs.RightColumn.ColumnId, 10, 64)
	}
	tidStr := strings.Join(utils.GetRelatedTable(r), ",")
	pgRule := po.Rule{
		Id:            0,
		TaskId:        taskId,
		BindIdList:    tidStr,
		YColumnId:     yColumnId,
		YColumnId2:    yColumnId2,
		ConflictCount: conflictCount,
		Ree:           r.Ree,
		ReeJson:       utils.GetRuleJson(r),
		ReeType:       r.RuleType,
		Support:       r.CR,
		Confidence:    r.FTR,
		Status:        enum.RULE_UNUSED,
	}
	err := po.CreateRule(&pgRule, db.DB)
	if err != nil {
		logger.Error("insert rule error:%v", err)
		return 0
	}

	var ruleColumns []po.RuleColumn
	columns := make(map[string]string)
	for _, predicate := range r.LhsPredicates {
		columns[predicate.LeftColumn.ColumnId] = predicate.LeftColumn.TableId
		columns[predicate.RightColumn.ColumnId] = predicate.RightColumn.TableId
	}
	for columnId, tId := range columns {
		if columnId == "" {
			continue
		}
		columnIdI, _ := strconv.ParseInt(columnId, 10, 64)
		tIdI, _ := strconv.ParseInt(tId, 10, 64)
		ruleColumn := po.RuleColumn{
			RuleId:   pgRule.Id,
			BindId:   tIdI,
			ColumnId: columnIdI,
			LabelY:   0,
			TaskId:   taskId,
		}
		ruleColumns = append(ruleColumns, ruleColumn)
	}
	rhsColumnIds := []string{r.Rhs.LeftColumn.ColumnId, r.Rhs.RightColumn.ColumnId}
	rhsTableId := []string{r.Rhs.LeftColumn.TableId, r.Rhs.RightColumn.TableId}
	for i, columnId := range rhsColumnIds {
		if columnId == "" {
			continue
		}
		if _, ok := columns[columnId]; !ok {
			columnIdI, _ := strconv.ParseInt(columnId, 10, 64)
			tIdI, _ := strconv.ParseInt(rhsTableId[i], 10, 64)
			ruleColumn := po.RuleColumn{
				RuleId:   pgRule.Id,
				BindId:   tIdI,
				ColumnId: columnIdI,
				LabelY:   1,
				TaskId:   taskId,
			}
			ruleColumns = append(ruleColumns, ruleColumn)
			columns[columnId] = rhsTableId[i]
		}
	}
	err = po.CreateRuleColumns(&ruleColumns, db.DB)
	if err != nil {
		logger.Error("insert column error:%v", err)
		return 0
	}

	return int(pgRule.Id)
}

func UpdateConflictCount(ruleId int, count int) {
	po.UpdateConflictCount(int64(ruleId), count, db.DB)
}

func GetAbandonedRules() []string {
	return po.GetAbandonedRules()
}

func UpdateConfidence(ruleId int, support, confidence float64, reeJson string) {
	po.UpdateConfidence(int64(ruleId), support, confidence, reeJson, db.DB)
}

func UpdateRuleStatus(taskId int64, ruleId int64, status int) {
	err := po.UpdateStatus(taskId, ruleId, status, db.DB)
	if err != nil {
		logger.Error("error:%v", err)
		return
	}
}

var _GetBindNameCache = map[string]string{}
var _GetFieldBindNameCache = map[string]string{}

func GetBindName(id string) string {
	name, ok := _GetBindNameCache[id]
	if !ok {
		if id == "" {
			name = ""
		} else {
			id2, err := strconv.ParseInt(id, 10, 64)
			if err != nil {
				name = err.Error()
			} else {
				name, err = po.FindBindNameByID(id2)
				if err != nil {
					name = err.Error()
				}
			}
		}
		_GetBindNameCache[id] = name
		logger.Info("GetBindName cache ", id, " ", name)
	}
	return name
}

func GetFieldBindName(id string) string {
	name, ok := _GetFieldBindNameCache[id]
	if !ok {
		if id == "" {
			name = ""
		} else {
			id2, err := strconv.ParseInt(id, 10, 64)
			if err != nil {
				name = err.Error()
			} else {
				fieldBind, err := po.GetFieldBindById(id2)
				if err != nil {
					name = err.Error()
				} else {
					name = fieldBind.FieldName
				}
			}
		}
		_GetFieldBindNameCache[id] = name
		logger.Info("GetFieldBindNameCache cache ", id, " ", name)
	}
	return name
}
