package dao

import (
	"gitlab.grandhoo.com/rock/rock-share/global/db"
)

type IncSampleNode struct {
	Id                   int64   `gorm:"id"`
	TaskId               int64   `gorm:"task_id"`
	CoverRadius          int     `gorm:"cover_radius"`
	CurrentNodeJson      string  `gorm:"current_node_json"`
	PredecessorNodesJson string  `gorm:"predecessor_nodes_json"`
	NeighborNodesJson    string  `gorm:"neighbor_nodes_json"`
	MinConfidence        float64 `gorm:"min_confidence"`
	MaxConfidence        float64 `gorm:"max_confidence"`
	NeighborConfsJson    string  `gorm:"neighbor_confs_json"`
	NeighborCDFJson      string  `gorm:"neighbor_cdf_json"`
	CreateTime           int64   `gorm:"create_time"`
	UpdateTime           int64   `gorm:"update_time"`
}

func (IncSampleNode) TableName() string {
	return "inc_sample_node"
}

func GetSampleNodesByTaskId(taskId int64) (sampleNodes []IncSampleNode, err error) {
	err = db.DB.Debug().Where("task_id=?", taskId).Find(&sampleNodes).Error
	return
}

func CreateIncSampleNode(sampleNode *IncSampleNode) (err error) {
	err = db.DB.Create(sampleNode).Error
	return
}

func GetSampleNodesById(id int64) (sampleNodes IncSampleNode, err error) {
	err = db.DB.Debug().Where("id=?", id).Find(&sampleNodes).Error
	return
}
