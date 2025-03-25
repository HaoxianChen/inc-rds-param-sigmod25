package dao

import "gitlab.grandhoo.com/rock/rock-share/global/db"

type IncRoot struct {
	Id       int64  `gorm:"column:id"`
	TaskId   int64  `gorm:"task_id"`
	NodeJson string `gorm:"node_json"`
}

func (IncRoot) TableName() string {
	return "inc_root"
}

func GetIncRoot(taskId int64) (roots []IncRoot, err error) {
	err = db.DB.Debug().Where("task_id=?", taskId).Find(&roots).Error
	return
}

func CreateIncRoot(root *IncRoot) (err error) {
	err = db.DB.Debug().Create(root).Error
	return
}
