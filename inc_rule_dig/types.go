package inc_rule_dig

import (
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/task_tree"
)

type Indicator struct {
	Support    float64
	Confidence float64
}

type IncMinerSupportInput struct {
	//PreIndicator Indicator
	PreTaskId       int64
	UpdateIndicator Indicator // 更新后的support和confidence
	UpdateTaskId    int64
	SupportChange   float64 // 表示支持度阈值的变化，可以是正数或负数
	Gv              *global_variables.GlobalV
	TableName2Id    map[string]string
}

type IncMinerSupportOutput struct {
	MinimalIncRules []*IncRule
	PrunedIncRules  []*IncRule
	SampleNodes     []*SampleNode
	TotalTime       int64
}

type IncExpandSupportInput struct {
	Rhs                rds.Predicate
	NeedExpandIncRules map[int][]*IncRule
	Gv                 *global_variables.GlobalV
}

type IncExpandSupportOutput struct {
	MinimalIncRules []*IncRule
	PrunedIncRules  []*IncRule
}

type IncMinerConfidenceInput struct {
	//PreIndicator Indicator
	PreTaskId        int64
	UpdateIndicator  Indicator // 更新后的support和confidence
	UpdateTaskId     int64
	ConfidenceChange float64 // 表示置信度阈值的变化，可以是正数或负数
	RecallBound      float64 // 召回率界限β
	Gv               *global_variables.GlobalV
	CoverRadius      int
	UseCDF           bool
	WithoutSampling  bool
	TableName2Id     map[string]string
}

type IncMinerConfidenceOutput struct {
	MinimalIncRules    []*IncRule
	UpdatedSampleNodes []*SampleNode
	PrunedIncRules     []*IncRule
	TotalTime          int64
}

type IncExpandConfidenceInput struct {
	Rhs                rds.Predicate
	NeedExpandIncRules map[int][]*IncRule
	SampleNodes        []*SampleNode
	Gv                 *global_variables.GlobalV
	CoverRadius        int
	WithoutSampling    bool
}

type IncExpandConfidenceOutput struct {
	MinimalIncRules   []*IncRule
	UpdateSampleNodes []*SampleNode
}

type SampleNode struct {
	CoverRadius      int        // 样本覆盖半径K
	CurrentNode      *IncRule   // 当前采样节点
	PredecessorNodes []*IncRule // 前驱节点(有K个)
	NeighborNodes    []*IncRule // 相邻节点
	MinConf          float64    // 最小confidence
	MaxConf          float64    // 最大confidence
	NeighborConfs    []float64  // 计算CDF时使用
	NeighborCDF      []int      // 计算CDF时使用
}

type IncRule struct {
	Node     *task_tree.TaskTree
	RuleType int
	CR       float64
	FTR      float64
}

type IncExpandInput struct {
	Rhs                rds.Predicate
	NeedExpandIncRules map[int][]*IncRule // layer -> IncRules
	//MinimalIncRules    []*IncRule
	//PrunedIncRules     []*IncRule         // 包含上一轮的规则,如果上一轮的剪枝集合被加到NeedExpandIncRules中,这里需要初始化一个空集合
	SampleNodes     []*SampleNode
	Gv              *global_variables.GlobalV
	CoverRadius     int
	WithoutSampling bool
}

type IncExpandOutput struct {
	MinimalIncRules    []*IncRule
	PrunedIncRules     []*IncRule
	UpdatedSampleNodes []*SampleNode
}

type IncMinerInput struct {
	PreTaskId        int64
	UpdateIndicator  Indicator // 更新后的support和confidence
	UpdateTaskId     int64
	SupportChange    float64
	ConfidenceChange float64
	Gv               *global_variables.GlobalV
	CoverRadius      int
	RecallBound      float64
	UseCDF           bool
	WithoutSampling  bool
	TableName2Id     map[string]string
}

type IncMinerOutput struct {
	MinimalIncRules    []*IncRule
	PrunedIncRules     []*IncRule
	UpdatedSampleNodes []*SampleNode
	TotalTime          int64
}

type CalcRulesOutput struct {
	HasRules    []bool
	Prunes      []bool
	IsDeleted   []bool
	RuleTypes   []int
	Supports    []float64
	Confidences []float64
}

type BatchMinerOutput struct {
	MinimalIncRules []*IncRule
	PrunedIncRules  []*IncRule
	SampleNodes     []*SampleNode
	TotalTime       int64
}

type CalcRuleResult struct {
	Rule      *IncRule
	HasRule   bool
	Prune     bool
	IsDeleted bool
}

const (
	ContainAll      = iota + 1 // 全包含(左边包含右边 或者 右边包含左边)
	ContainAllLeft             // 左边包含右边
	ContainAllRight            // 右边包含左边
	ContainPart                // 部分包含
	ContainNot                 // 全不包含
)

type IncRdsTaskExtendField struct {
	TableId2Name map[string]string // tableId -> tableName
}
