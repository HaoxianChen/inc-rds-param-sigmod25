package dcfinder

import (
	"gitlab.grandhoo.com/rock/rock-share/base/config"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/db"
	"testing"
)

func Test_cover_suppDec_confInc(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	taskId := int64(1003485)
	dcRulePath := "rules/Guidelines(S-C+)_output.txt"

	getIncMinerCoverDCFinder(taskId, dcRulePath)

	getDCFinderCoverIncMiner(taskId, dcRulePath)
}

func Test_cover_suppDec_confDec(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	taskId := int64(1003415)
	dcRulePath := "rules/Guidelines(S-C-)_output.txt"

	getIncMinerCoverDCFinder(taskId, dcRulePath)

	getDCFinderCoverIncMiner(taskId, dcRulePath)
}
