package dao

import "gitlab.grandhoo.com/rock/rock-share/global/db"

type IncMinimalRule struct {
	Id         int64   `gorm:"column:id"`
	TaskId     int64   `gorm:"task_id"`
	Ree        string  `gorm:"ree"`
	ReeJson    string  `gorm:"ree_json"`
	NodeJson   string  `gorm:"node_json"`
	ReeType    int     `gorm:"ree_type"`
	Support    float64 `gorm:"support"`
	Confidence float64 `gorm:"confidence"`
	CreateTime int64   `gorm:"create_time"`
	UpdateTime int64   `gorm:"update_time"`
}

func (IncMinimalRule) TableName() string {
	return "inc_minimal_rule"
}

func GetIncMinimalRulesByTaskId(taskId int64) (rules []IncMinimalRule, err error) {
	err = db.DB.Debug().Where("task_id=?", taskId).Find(&rules).Error
	return
}

func CreateIncMinimalRule(rule *IncMinimalRule) (err error) {
	err = db.DB.Debug().Create(rule).Error
	return
}
