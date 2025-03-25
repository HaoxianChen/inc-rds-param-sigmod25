package storage_utils

import (
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/storage/storage2/database/sql"
	"gitlab.grandhoo.com/rock/storage/storage2/database/sql/condition"
	"gitlab.grandhoo.com/rock/storage/storage2/database/table/table_impl/row_id"
	"gitlab.grandhoo.com/rock/storage/storage2/rock_db"
)

type CellValue = any

type Reader struct {
	data       map[tableName]map[columnName][]CellValue
	offsetInfo map[tableName]int64 // 每张表扫描范围，起始位置。如果表不存在则视为从 0 开始扫描。默认为 nil 表示所有表都从 0 开始扫描
	limitInfo  map[tableName]int64 // 每张表表扫描范围，扫描行数目。如果值为 0 或者表不存在，表示无限制。默认为 nil 表示所有表都无数目限制
}

func new(offsetInfo, limitInfo map[tableName]int64) *Reader {
	if offsetInfo == nil {
		offsetInfo = map[tableName]int64{}
	}
	if limitInfo == nil {
		limitInfo = map[tableName]int64{}
	}
	return &Reader{
		data:       map[tableName]map[columnName][]CellValue{},
		offsetInfo: offsetInfo,
		limitInfo:  limitInfo,
	}
}

func (r *Reader) GetCell(tableName tableName, columnName columnName, rowId int32) (value CellValue, err error) {
	columns := r.data[tableName]
	offset := r.offsetInfo[tableName]
	if columns == nil {
		columns, err = loadTable(tableName, offset, r.limitInfo[tableName])
		if err != nil {
			return nil, err
		}
		r.data[tableName] = columns
	}
	return columns[columnName][int64(rowId)+offset], err
}

func loadTable(tableName tableName, offset, limit int64) (map[columnName][]CellValue, error) {
	columnInfos, err := rock_db.DB.TableColumnInfos(tableName) // 不包括删除标记列和仅逻辑的行号列
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	if limit == 0 {
		length, err := TableLengthNotRegradingDeleted(tableName)
		if err != nil {
			return nil, err
		}
		limit = length - offset
	}
	var columns = make(map[columnName][]CellValue, len(columnInfos))
	var columnNames []columnName
	for _, info := range columnInfos {
		columnName := info.ColumnName
		columnNames = append(columnNames, columnName)
		columns[columnName] = make([]CellValue, limit)
	}

	SQL := sql.SingleTable.SELECT(columnNames...).FROM(tableName).WHERE(condition.GreaterEq(row_id.ColumnName, offset))
	if limit > 0 {
		SQL = SQL.WHERE(condition.Less(row_id.ColumnName, offset+limit))
	}
	lines, err := rock_db.DB.Query(SQL)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	for i, row := range lines {
		for j, value := range row {
			columnName := columnNames[j]
			columns[columnName][i] = value
		}
	}
	return columns, nil
}
