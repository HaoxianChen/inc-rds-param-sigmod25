package dao

import "gitlab.grandhoo.com/rock/rock-share/global/db"

type IncRdsTaskExtend struct {
	Id         int64  `gorm:"column:id"`
	TaskId     int64  `gorm:"task_id"`
	ExtendJson string `gorm:"extend_json"` // 做成一个可序列化的结构体，方便添加额外信息而不用修改表结构
	CreateTime int64  `gorm:"create_time"`
	UpdateTime int64  `gorm:"update_time"`
}

func (IncRdsTaskExtend) TableName() string {
	return "inc_rds_task_extend"
}

func CreateIncRdsTaskExtend(extend *IncRdsTaskExtend) (err error) {
	err = db.DB.Create(extend).Error
	return
}

func GetLatestExtendByTaskId(taskId int64) (extend IncRdsTaskExtend, err error) {
	var sql = "select * from inc_rds_task_extend t0 where task_id = ? order by update_time desc limit 1"
	err = db.DB.Debug().Raw(sql, taskId).Scan(&extend).Error
	return
}
