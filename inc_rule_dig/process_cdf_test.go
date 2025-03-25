package inc_rule_dig

import (
	"gitlab.grandhoo.com/rock/rock-share/base/config"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"testing"
)

func Test_AddConfidence2CDF(t *testing.T) {
	cdf := make([]int, len(CdfValues))

	confidence := 0.85
	AddConfidence2CDF(confidence, cdf)

	confidence1 := 0.75
	AddConfidence2CDF(confidence1, cdf)

	confidence2 := 0.65
	AddConfidence2CDF(confidence2, cdf)

	confidence3 := 0.00001
	AddConfidence2CDF(confidence3, cdf)
	t.Logf("cdf:%v", cdf)
}

func Test_GetValidAndTotalCount(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)

	cdf := make([]int, len(CdfValues))

	confidence := 0.00001
	AddConfidence2CDF(confidence, cdf)

	confidence0 := 0.1
	AddConfidence2CDF(confidence0, cdf)

	confidence1 := 0.2
	AddConfidence2CDF(confidence1, cdf)

	confidence2 := 0.3
	AddConfidence2CDF(confidence2, cdf)

	confidence4 := 0.4
	AddConfidence2CDF(confidence4, cdf)

	confidence5 := 0.5
	AddConfidence2CDF(confidence5, cdf)

	confidence6 := 0.55
	AddConfidence2CDF(confidence6, cdf)

	confidence7 := 0.6
	AddConfidence2CDF(confidence7, cdf)

	confidence8 := 0.65
	AddConfidence2CDF(confidence8, cdf)

	confidence9 := 0.7
	AddConfidence2CDF(confidence9, cdf)

	confidence10 := 0.75
	AddConfidence2CDF(confidence10, cdf)

	confidence11 := 0.8
	AddConfidence2CDF(confidence11, cdf)

	confidence12 := 0.85
	AddConfidence2CDF(confidence12, cdf)

	confidence13 := 0.9
	AddConfidence2CDF(confidence13, cdf)

	confidence14 := 0.95
	AddConfidence2CDF(confidence14, cdf)

	t.Logf("cdf:%v", cdf)

	valid, invalid := GetValidAndInvalidCount(0.65, cdf)
	t.Logf("valid:%v, invalid:%v", valid, invalid)
}

func Test_MergeCDF(t *testing.T) {
	cdf1 := make([]int, len(CdfValues))
	//cdf1 := []int{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
	cdf2 := make([]int, len(CdfValues))
	//cdf2 := []int{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}

	cdf3 := MergeCDF(cdf1, cdf2, true)
	t.Logf("cdf3:%v", cdf3)
}
