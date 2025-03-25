package dao

import "gitlab.grandhoo.com/rock/rock-share/global/db"

type IncRdsTask struct {
	Id         int64   `gorm:"column:id"`
	Support    float64 `gorm:"support"`
	Confidence float64 `gorm:"confidence"`
	IsDeleted  int     `gorm:"is_deleted"` // 0:正常 1:删除
	CreateTime int64   `gorm:"create_time"`
	UpdateTime int64   `gorm:"update_time"`
}

func (IncRdsTask) TableName() string {
	return "inc_rds_task"
}

func GetIncRdsTaskById(taskId int64) (task IncRdsTask, err error) {
	err = db.DB.Debug().Where("id=? and is_deleted=0", taskId).Find(&task).Error
	return
}

func CreateIncRdsTask(task *IncRdsTask) (taskId int64, err error) {
	err = db.DB.Create(task).Error
	if err != nil {
		return 0, err
	}
	return task.Id, nil
}

func DeleteIncRdsTask(taskId int64) (err error) {
	err = db.DB.Debug().Model(&IncRdsTask{}).Where("id = ?", taskId).Update("is_deleted", 1).Error
	return
}

func GetLatestIncRdsTaskByIndicator(support float64, confidence float64) (task IncRdsTask, err error) {
	var sql = "select t1.* from (select * from inc_rds_task t0 where t0.support = ? and t0.confidence = ? and is_deleted = 0 order by update_time desc limit 1) as t1"
	err = db.DB.Debug().Raw(sql, support, confidence).Scan(&task).Error
	return
}

func GetLatestIncRdsTask() (task IncRdsTask, err error) {
	var sql = "select * from inc_rds_task t0 where is_deleted = 0 order by update_time desc limit 1"
	err = db.DB.Debug().Raw(sql).Scan(&task).Error
	return
}

func DeleteIncRdsTaskByIndicator(support float64, confidence float64) (err error) {
	err = db.DB.Debug().Model(&IncRdsTask{}).Where("support = ? and confidence = ?", support, confidence).Update("is_deleted", 1).Error
	return
}
