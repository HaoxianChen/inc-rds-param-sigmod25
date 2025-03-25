package request

import (
	"gitlab.grandhoo.com/module/igeenum"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/task_tree"
	"gitlab.grandhoo.com/rock/rock_v3/request/udf"
)

type RuleFinderRequest struct {
	TaskId          string     `json:"taskId" binding:"required"`
	DataSource      DataSource `json:"dataSource" binding:"required"`
	CR              float64    `json:"cr"`
	FTR             float64    `json:"ftr"`
	SingleTableLine int        `json:"singleTableLine"`
	SkipColumn      []string   `json:"skipColumn"`
	ConstantColumn  []string   `json:"constantColumn"`
	Rhs             []string   `json:"rhs"`
	ExecuteRules    bool       `json:"executeRules"`
	UseStorage      bool       `json:"useStorage"`
	TreeMaxLevel    int        `json:"treeMaxLevel"`
}

type RuleExecuteRequest struct {
	TaskId         string     `json:"taskId" binding:"required"`
	DataSource     DataSource `json:"dataSource" binding:"required"`
	ConstantColumn []string   `json:"constantColumn"`
	Rules          []string   `json:"rules" binding:"required"`
}

type TaskIdTableId struct {
	TaskId  int64 `json:"taskId"`
	TableId int64 `json:"tableId"`
}

type DataSource struct {
	Database     string `json:"database" binding:"required"` // mysql etc.文件传csv
	Host         string `json:"host"`
	Port         int    `json:"port"`
	DatabaseName string `json:"databaseName"`
	TableName    string `json:"tableName" binding:"required"` // csv的话写本地完整路径,如/data/test.csv
	TableId      string `json:"tableId"`                      // 自研存储版本都使用tableId。如果不为空，则说明不用导入数据
	User         string `json:"user"`
	Password     string `json:"password"`
	TableType    int    `json:"tableType"` // 0:规则发现表;1:查错表;2:验证集;3:可信数据;4:ER标注数据
	DataLimit    int    `json:"dataLimit"` // 数据抽样
}

type PriorityRequest struct {
	Columns []string `json:"columns" binding:"required"`
	TaskId  string   `json:"taskId" binding:"required"`
}

type KillRequest struct {
	TaskID string `json:"taskID" binding:"required"`
}

type TaskRequest struct {
	TaskID  int64  `json:"taskID" binding:"required"`
	TableId string `json:"tableId" binding:"required"`
}

type AutoTestRequest struct {
	Tables []DataSource `json:"tables" binding:"required"`
	CR     float64      `json:"cr"`
	FTR    float64      `json:"ftr"`
	Rhs    []string     `json:"rhs"`
}

type DeleteConflictRequest struct {
	TaskId     int64   `json:"taskId"`
	StartRowId int64   `json:"startRowId"`
	EndRowId   int64   `json:"endRowId"`
	Threshold  float64 `json:"threshold"`
}

type QueryConflictRequest struct {
	TaskId  int64  `json:"taskId"`
	TableId string `json:"tableId"`
}

type ModifyCellRequest struct {
	TaskId   int64 `json:"taskId"`
	TableId  int64 `json:"tableId"`
	RowId    int64 `json:"rowId"`
	ColumnId int64 `json:"columnId"`
	OpLogId  int64 `json:"opLogId"`
	Value    any   `json:"value"`
}

type AggregateRequest struct {
	DataSource         DataSource `json:"dataSource"`
	TaskID             string     `json:"taskID" binding:"required"`
	Confidence         float64    `json:"confidence"`
	YColumn            []string   `json:"yColumn"`
	AggregateColumn    []string   `json:"aggregateColumn"`
	TimeColumn         string     `json:"timeColumn"`
	TimePeriod         string     `json:"timePeriod"` // 可选值:month,season,year
	SampleCount        int        `json:"sampleCount"`
	PolyMaxLength      int        `json:"polyMaxLength"`
	PolyAllowableError float64    `json:"polyAllowableError"`
}

type CsvInfo struct {
	TableName  string            `json:"tableName"`
	Path       string            `json:"path"`
	ColumnType map[string]string `json:"columnType"`
	Columns    []string          `json:"columns"` //可指定导入的列，减少内存占用
}

type TableJoin struct {
	Table          DataSource `json:"table"`
	File           CsvInfo    `json:"file"`
	Columns        []string   `json:"columns"`
	JoinKeys       []string   `json:"joinKeys"`
	Child          *TableJoin `json:"child"`
	ResultJoinKeys []string   `json:"resultJoinKeys"`
}

type CrossTableCheckRequest struct {
	Table0              TableJoin `json:"table0"`
	Table1              TableJoin `json:"table1"`
	Table2              TableJoin `json:"table2"`
	Rules               []string  `json:"rules"`
	LeftIdColumn        string    `json:"leftIdColumn"`
	RightIdColumn       string    `json:"rightIdColumn"`
	LeftAuthorIdColumn  string    `json:"leftAuthorIdColumn"`
	RightAuthorIdColumn string    `json:"rightAuthorIdColumn"`
}

type PolyRequest struct {
	DataSource         DataSource `json:"dataSource"`
	TaskID             string     `json:"taskID" binding:"required"`
	Confidence         float64    `json:"confidence"`
	SampleCount        int        `json:"sampleCount"`
	PolyMaxLength      int        `json:"polyMaxLength"`
	PolyAllowableError float64    `json:"polyAllowableError"`
}

type MultiRDSRequest struct {
	DataSources         []DataSource              `json:"dataSources"`
	TaskID              int64                     `json:"taskID" binding:"required"`
	JoinKeys            []JoinKey                 `json:"joinKeys"`
	OtherMappingKeys    []JoinKey                 `json:"otherMappingKeys"`
	Rhs                 []PredicateInfo           `json:"rhs"`
	UDFTabCols          []udf.UDFTabCol           `json:"UDFTabCols"`
	ColumnsRole         map[string]igeenum.DtRole `json:"columnsRole"` // 数据角色
	SkipColumns         []string                  `json:"skipColumns"`
	SkipYColumns        []string                  `json:"skipYColumns"` // 不作为Y的列
	Eids                map[string]string         `json:"eids"`         // 实体列id
	MutexGroup          [][]string                `json:"mutexGroup"`   // 一组中只有一列可以出现在规则中
	ErRuleTableMapping  map[string]string         `json:"erRuleTableMapping"`
	ErRuleColumnMapping map[string]string         `json:"erRuleColumnMapping"`
	RdsConf             RdsConfRequest            `json:"rdsConf"`
}

type ConnectKey struct { // 兼容后端
	LeftTableId       string   `json:"leftTableId"`
	RightTableId      string   `json:"rightTableId"`
	LeftTableColumns  []string `json:"leftTableColumns"`  //目前单一
	RightTableColumns []string `json:"rightTableColumns"` //目前单一
}

type UDFTabCol struct { // 兼容后端
	LeftTableId               string      `json:"leftTableId,omitempty"`
	LeftColumnName            string      `json:"leftColumnName,omitempty"`
	RightTableId              string      `json:"rightTableId,omitempty"`              // 左右列相同时，不传即可
	RightColumnName           string      `json:"rightColumnName,omitempty"`           // 左右列相同时，不传即可
	Type                      string      `json:"type,omitempty"`                      // similar/ML
	Name                      string      `json:"name,omitempty"`                      // 比如 jaccard sentence-bert
	Threshold                 float64     `json:"threshold,omitempty"`                 // 阈值，只在相似度时有用
	LeftColumnVectorFilePath  string      `json:"leftColumnVectorFilePath,omitempty"`  // 向量文件地址。只有 ML 时才有用
	RightColumnVectorFilePath string      `json:"rightColumnVectorFilePath,omitempty"` // 向量文件地址。左右列相同时，不传即可
	LeftVectorList            [][]float64 `json:"-"`                                   // 从 LeftColumnVectorFilePath 读取
	RightVectorList           [][]float64 `json:"-"`                                   // 从 RightColumnVectorFilePath ? LeftColumnVectorFilePath 读取
}

type RdsConfRequest struct {
	Confidence                        float64          `json:"confidence"`
	Support                           float64          `json:"support"`
	FPSupport                         float64          `json:"fpSupport"`
	MultiRuleDig                      bool             `json:"multiRuleDig"`
	FilterArr                         []float64        `json:"filterArr"`
	SampleCount                       int              `json:"sampleCount"`
	PolyMaxLength                     int              `json:"polyMaxLength"`
	PolyAllowableError                float64          `json:"polyAllowableError"`
	MultiPolySampleGroup              int              `json:"multiPolySampleGroup"`
	DecisionTreeSampleThreshold2Ratio map[int]float64  `json:"decisionTreeSampleThreshold2Ratio"`
	ChanSize                          int              `json:"chanSize"`
	PredicateSupportLimit             float64          `json:"predicateSupportLimit"`
	TreeLevel                         int              `json:"treeLevel"`
	TopKLayer                         int              `json:"topKLayer"`
	TopKSize                          int              `json:"topKSize"`
	DecisionTreeMaxDepth              int              `json:"decisionTreeMaxDepth"`
	CrossTablePredicateSupp           float64          `json:"crossTablePredicateSupp"`
	CubeSize                          int              `json:"cubeSize"`
	NoMultiRule                       bool             `json:"multiRuleFlag"`
	TreeChanSize                      int              `json:"treeChanSize"`
	IsNeedRE                          bool             `json:"isNeedRE"` // 是否调用规则执行
	REParam                           RuleExecuteParam `json:"reParam"`  // 规则执行使用
	FrequentSize                      int              `json:"frequentSize"`
	IsLocal                           bool             `json:"isLocal"`
	NumericalConf                     string           `json:"numericalConf"`

	DecisionTreeMaxRowSize int     `json:"decisionTreeMaxRowSize"`
	EnumSize               int     `json:"enumSize"`
	SimilarThreshold       float64 `json:"similarThreshold"`
	TableRuleLimit         int     `json:"tableRuleLimit"`
	RdsSize                int     `json:"rdsSize"`
	EnableErRule           bool    `json:"enableErRule"`
	EnableTimeRule         bool    `json:"enableTimeRule"`
	EnableDecisionTree     bool    `json:"enableDecisionTree"`
	EnableEmbedding        bool    `json:"enableEmbedding"`
	EnableSimilar          bool    `json:"enableSimilar"`
	EnableEnum             bool    `json:"enableEnum"`
}

type RuleExecuteParam struct {
	RuleIdsOfDB        []int    `json:"ruleIdsOfDB"`    // 如果ruleIdsOfDB为空，则taskId下的所有规则
	Rules              []string `json:"rules"`          // 外部输入的规则(规则的json形式)
	HashBucketSize     int      `json:"hashBucketSize"` // 指定hypercube的分桶数
	BatchNum           int      `json:"batchNum"`       // 规则分批跑，一批包含多少条规则
	HistogramThreshold int      `json:"histogramThreshold"`
	BucketBatchNum     int      `json:"bucketBatchNum"` // 桶分批跑，一批包含多少个桶
}

type PredicateInfo struct {
	LeftTableIndex   int    `json:"leftTableIndex"`
	RightTableIndex  int    `json:"rightTableIndex"`
	LeftTableColumn  string `json:"leftTableColumn"`
	RightTableColumn string `json:"rightTableColumn"`
}

type JoinKey struct {
	LeftTableIndex    int      `json:"leftTableIndex"`
	RightTableIndex   int      `json:"rightTableIndex"`
	LeftTableColumns  []string `json:"leftTableColumns"`
	RightTableColumns []string `json:"rightTableColumns"`
}

type RuleDiscoverTask struct {
	TaskId            int64 `json:"taskId"`
	BindId            int64 `json:"bindId"`
	DistriDigRuleMode int   `json:"distriDigRuleMode"`
}

type ShouldPruneReq struct {
	// used to init gv
	ColumnsType map[string]string
	TableName   string
	TaskId      int64
	TableId     string
	RowSize     int
	CR          float64
	FTR         float64
	StopTask    bool

	TreeNode           *task_tree.TaskTree
	ShouldPruneRowSize int
	IsSingle           bool
	IsMultiTable       bool
}
type ShouldPruneResp struct {
	RuleSize int
	HasRule  bool
	Prune    bool
	IsDelete bool
}

type ChildrenShouldPruneReq struct {
	// used to init gv
	TableName string
	TaskId    int64
	CR        float64
	FTR       float64
	StopTask  bool

	Children           []*task_tree.TaskTree
	ShouldPruneRowSize int
	IsSingle           bool
	IsMultiTable       bool
}
type ChildrenShouldPruneResp struct {
	RuleSize []int32
	HasRule  []bool
	Prune    []bool
	IsDelete []bool
}

type CalNotSatisfyNodeReq struct {
	NodeId                 int
	TaskId                 int64
	CR                     float64
	FTR                    float64
	Node                   *task_tree.TaskTree
	CalGiniFlag            bool
	TableColumnType        map[string]map[string]string
	FilterArr              []float64
	DecisionTreeMaxDepth   int
	CubeSize               int
	ChanSize               int
	DecisionTreeMaxRowSize int
}

type CalNotSatisfyNodeResp struct {
	RuleSize                   int
	LhsCandidateGiniIndex      []float32
	LhsCrossCandidateGiniIndex []float32
}

type SingleRowDecisionTreeReq struct {
	TableName    string
	TaskId       int64
	CR           float64
	FTR          float64
	MaxTreeDepth int
	StopTask     bool

	YColumn                string
	ColumnType             map[string]string
	TableLength            int
	SampleThreshold2Ratio  map[int]float64
	DecisionTreeMaxRowSize int
}

type SingleRowDecisionTreeResp struct {
	RuleSize int
	Success  bool
}

type GetTableSampleDataReq struct {
	TaskId     int64
	TableName  string
	ColumnType map[string]string
	Offset     int
	Limit      int
	Ratio      float64
}

type GetTableSampleDataResp struct {
	SampleData map[string][]interface{}
}

type HashJoinReq struct {
	TaskId   int64    `json:"taskId" binding:"required"`
	TableIds []string `json:"tableIds" binding:"required"`
	Rule     string   `json:"rule" binding:"required"`
	ChanSize int      `json:"chanSize"`
}

type HashJoinResp struct {
	RowSize      int
	XSupp        int
	XySupp       int
	HashJoinTime string
}

type RdsTimeResp struct {
	LoadDataTime int64
	RdsTime      int64
	TotalTime    int64
}

type DecisionTreeTaskReq struct {
	TaskId           int64
	Support          float64
	Confidence       float64
	MaxDepth         int
	TableIds         []string
	YColumn          string
	YTable           string
	ConnectedColumns map[string]string
	TableIndex       map[string]int
	IsSingle         bool
	IsCross          bool
}

// TaskStatusRequest 分布式任务状态request，发送给后端
type TaskStatusRequest struct {
	Method  string
	Path    string
	Body    []byte
	CsvPath string
}

// TaskStatusResponse 分布式任务状态response
type TaskStatusResponse struct {
	Status     string
	StatusCode int
	Body       []byte
}
