package inc_rule_dig

import "gitlab.grandhoo.com/rock/rock_v3/request"

type IncRdsRequest struct {
	DataSources                []request.DataSource    `json:"dataSources"`
	PreTaskID                  int64                   `json:"preTaskID"`
	TaskID                     int64                   `json:"taskID"`
	PreSupport                 float64                 `json:"preSupport"`
	PreConfidence              float64                 `json:"preConfidence"`
	UpdatedSupport             float64                 `json:"updatedSupport"`
	UpdatedConfidence          float64                 `json:"updatedConfidence"`
	CoverRadius                int                     `json:"coverRadius"`                // 覆盖半径K
	UseCDF                     bool                    `json:"useCDF"`                     // 是否启用近似算法
	RecallBound                float64                 `json:"recallBound"`                // 召回率界限β
	WithoutSampling            bool                    `json:"withoutSampling"`            // true 当confidence变小的时候不执行采样，执行batchMiner
	RecallRateBatchMinerTaskId int64                   `json:"recallRateBatchMinerTaskId"` // 计算召回率的batchMiner的taskId
	ChanSize                   int                     `json:"chanSize"`
	TreeLevel                  int                     `json:"TreeLevel"`
	TopKLayer                  int                     `json:"topKLayer"`
	TopKSize                   int                     `json:"topKSize"`
	TreeChanSize               int                     `json:"treeChanSize"`
	PredicateSupportLimit      float64                 `json:"predicateSupportLimit"`
	CrossTablePredicateSupp    float64                 `json:"crossTablePredicateSupp"`
	UseNeighborConfs           bool                    `json:"useNeighborConfs"`
	UsePruneVersion            bool                    `json:"usePruneVersion"`
	UseDecisionTree            bool                    `json:"useDecisionTree"`
	CubeSize                   int                     `json:"cubeSize"`
	DecisionTreeMaxDepth       int                     `json:"decisionTreeMaxDepth"`
	DecisionTreeMaxRowSize     int                     `json:"decisionTreeMaxRowSize"`
	EnumSize                   int                     `json:"enumSize"`
	GoldenRules                []string                `json:"goldenRules"`
	Rhs                        []request.PredicateInfo `json:"rhs"`
}

type IncRdsResponse struct {
	TotalTime            int64
	PreTaskId            int64
	UpdatedTaskId        int64
	MinimalREEsSize      int     // 符合阈值的REE的数量
	PruneREEsSize        int     // 剪枝的REE的数量
	SampleSize           int     // 采样的数量
	RecallRate           float64 // 召回率
	PreSupport           float64
	PreConfidence        float64
	UpdatedSupport       float64
	UpdatedConfidence    float64
	OutputSize           float64
	AuxiliarySize        float64
	MinimalSize          float64
	DecisionTreeREEsSize int     // 决策树挖掘的规则数量
	DecisionTreeSize     float64 // 决策树挖掘的规则size
	GoldenRecallRate     float64 // golden规则的召回率
}

type DeleteTaskReq struct {
	Support    float64 `json:"support"`
	Confidence float64 `json:"confidence"`
}
