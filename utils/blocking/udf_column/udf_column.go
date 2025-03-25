package udf_column

import (
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"strconv"
	"strings"
)

const udfColPrefix = rds_config.UdfColumnPrefix
const udfColConn = rds_config.UdfColumnConn

// MakeUdfColumn for reader
// New(tableName string, columnNames []string, fileSuffix string, threshold float64) *Reader
func MakeUdfColumn(tableName string, columnNames []string, modelName string, threshold float64) string {
	// ${tableName}@{fileSuffix}@{threshold}@col[@col]
	thresholdStr := strconv.FormatFloat(threshold, 'f', 2, 64)
	var infos []string
	infos = append(infos, modelName)
	infos = append(infos, thresholdStr)
	infos = append(infos, columnNames...)
	return udfColPrefix + tableName + udfColConn + strings.Join(infos, udfColConn)
}

func IsUdfColumn(column string) bool {
	return len(column) >= len("$t@n@1@c") && column[0] == udfColPrefix[0]
}

func ParseUdfColumn(column string) (tableName string, columnNames []string, modelName string, threshold float64) {
	if !IsUdfColumn(column) {
		panic(column)
	}
	var err error
	ss := strings.Split(column, udfColConn)
	{
		// table
		if len(ss[0]) < 2 {
			panic(column)
		}
		tableName = ss[0][1:]
	}
	modelName = ss[1]
	threshold, err = strconv.ParseFloat(ss[2], 64)
	if err != nil {
		panic(column + err.Error())
	}
	columnNames = ss[3:]
	return
}
