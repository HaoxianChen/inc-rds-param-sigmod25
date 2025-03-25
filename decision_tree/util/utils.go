package util

import (
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/util/predicate"
	"regexp"
)

func GetDeriveColumnType(columnName string) int {
	// 匹配 t0.column=t1.column
	re := regexp.MustCompile(`^t\d+\..*=t\d+\..*$`)
	match := re.MatchString(columnName)
	if match {
		//logger.Debugf("columnName:%s, 类型:%v", columnName, predicate.MultiDeriveColumn)
		return predicate.MultiDeriveColumn
	}

	// 匹配 t0.column=a
	re = regexp.MustCompile(`^t\d+\..*=.*$`)
	match = re.MatchString(columnName)
	if match {
		//logger.Debugf("columnName:%s, 类型:%v", columnName, predicate.SingleDeriveColumn)
		return predicate.SingleDeriveColumn
	}
	//logger.Debugf("columnName:%s, 类型:%v", columnName, predicate.RealColumn)
	return predicate.RealColumn
}
