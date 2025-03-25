package dao

import "gitlab.grandhoo.com/rock/rock-share/global/db"

type IncPruneRule struct {
	Id         int64   `gorm:"column:id"`
	TaskId     int64   `gorm:"task_id"`
	NodeJson   string  `gorm:"node_json"`
	RuleType   int     `gorm:"rule_type"`
	Support    float64 `gorm:"support"`
	Confidence float64 `gorm:"confidence"`
	CreateTime int64   `gorm:"create_time"`
	UpdateTime int64   `gorm:"update_time"`
}

func (IncPruneRule) TableName() string {
	return "inc_prune_rule"
}

func GetPrunedIncRulesByTaskId(taskId int64) (incPruneRules []IncPruneRule, err error) {
	err = db.DB.Debug().Where("task_id=?", taskId).Find(&incPruneRules).Error
	return
}

func CreateIncPruneRule(rule *IncPruneRule) (err error) {
	err = db.DB.Create(rule).Error
	return
}

// GetNeedExpandIncRules 测试使用
func GetNeedExpandIncRules(taskId int64, support float64, confidence float64) (incPruneRules []IncPruneRule, err error) {
	err = db.DB.Debug().Where("task_id=? and support>? and confidence<?", taskId, support, confidence).Find(&incPruneRules).Error
	return
}
