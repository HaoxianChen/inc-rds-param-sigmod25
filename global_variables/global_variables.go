package global_variables

import (
	"gitlab.grandhoo.com/module/igeenum"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/task_tree"
	"runtime"
	"sync"

	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
)

type GlobalV struct {
	TaskId            int64
	TableName         string
	TableId           string
	TableIds          []string
	ColumnsType       map[string]string
	RowSize           int
	DelRowSize        int
	SingleRuleSize    int32
	MultiRuleSize     int32
	TreeMaxLevel      int // 树的最大层数
	CR                float64
	FTR               float64
	IntersectionIndex map[string][][2][]int32
	IntersectionLock  *sync.RWMutex //交集缓存的读写锁

	DecisionTreeIndex     map[string]bool //记录哪些组合调用过决策树,防止重复调用
	DecisionTreeLock      *sync.Mutex
	Predicates            []rds.Predicate
	RegularPredicates     []rds.Predicate
	SingleCrossPredicates map[string][]rds.Predicate //tableId -> predicates
	StopTask              bool                       //任务是否终止
	Ree2RuleId            map[string]int             // 规则ree表达式到规则id的映射
	Ree2IdLock            *sync.RWMutex
	AbandonedRules        []string

	TableValues         map[string][]interface{}
	TableValueIndexes   map[string][]int32
	Index2Value         map[string]map[int32]interface{}
	Value2Index         map[string]map[interface{}]int32
	RowConflictSize     [0]int32
	RowConflictSizeLock *sync.RWMutex
	SimilarPairs        []rds.SimilarPair
	SimilarPredicates   []rds.Predicate
	TempPLI             map[string]map[interface{}][]int32

	WriteRulesChan   chan rds.Rule
	NeedExeRulesChan chan rds.Rule
	FinishedWriteDB  bool
	FinishedExe      bool
	HasError         bool

	LeftExpandTableValues  map[string][]interface{}
	RightExpandTableValues map[string][]interface{}
	LeftIdColumns          []string
	RightIdColumns         []string
	ListSatisfy            [][6]interface{}
	ListNotSatisfy         [][6]interface{}
	RuleConflictCount      map[string][]interface{} // 记录规则的冲突树
	PolyMaxLength          int
	PolyMinConfidence      float64
	PolySampleCount        int
	PolyAllowableError     float64
	MultiPolySampleGroup   int
	HasCheckTable          bool
	MultiRuleDig           bool
	Rules                  []rds.Rule  //规则
	RulesLock              *sync.Mutex //对rules的写操作都需要加锁
	MultiTable             bool
	DistriDigRuleMode      int // 1--queue; 2--rpc
	//Tables                 map[string]map[string][]interface{} //tableId -> tableValues
	TableId2Name         map[string]string
	CrossTablePredicates [2][]rds.Predicate
	Table2Predicates     map[string][]rds.Predicate
	//ColumnsIsForeigner     map[string]map[string]struct{}
	Table2JoinTables                  map[string]map[string]struct{}
	Table2ColumnType                  map[string]map[string]string
	Table2DecisionY                   map[string][]string
	DecisionTreeSampleThreshold2Ratio map[int]float64
	RdsChanSize                       int
	FilterArr                         []float64
	PredicateSupportLimit             float64
	TopKLayer                         int
	TopKSize                          int
	DecisionTreeMaxDepth              int
	CrossTablePredicateSupp           float64
	CubeSize                          int
	TreeChanSize                      int

	DecisionTreeNodes []*task_tree.TaskTree
	TableIndex        map[string]int

	SkipColumns  map[string]bool
	SkipYColumns map[string]bool
	ColumnsRole  map[string]igeenum.DtRole

	Eids          map[string]string
	EidPredicates []rds.Predicate

	DecisionTreeMaxRowSize int
	EnumSize               int
	SimilarThreshold       float64
	MLThreshold            float64
	TableRuleLimit         int
	RdsSize                int

	EnableErRule       bool
	EnableTimeRule     bool
	EnableDecisionTree bool
	EnableEmbedding    bool
	EnableML           bool
	EnableSimilar      bool
	EnableEnum         bool

	MutexGroup map[string][]int
	GroupSize  int

	ErRuleTableMapping  map[string]string `json:"erRuleTableMapping"`
	ErRuleColumnMapping map[string]string `json:"erRuleColumnMapping"`

	TimeRules        []rds.Rule
	ChEndpoint       chan string
	UseNeighborConfs bool
	UsePruneVersion  bool
	UseDecisionTree  bool
}

type PredicateScore struct {
	Pred  rds.Predicate
	Score float64
}

// GlobalVariable ************************************************************************
var GlobalVariable = make(map[int64]*GlobalV) //[taskID,GlobalV]
var GlobalVariableLock sync.RWMutex           //并发写入操作,加锁

var (
	//RuleScores    = make(map[rds.Predicate]map[rds.Predicate]float64)
	MultiTopKLock sync.RWMutex //并发写入操作,加锁
)

// IntersectionChan 限制交集计算的最大并发协程数量
var IntersectionChan chan int //带缓冲

func InitGlobalV(taskId int64) *GlobalV {
	GlobalVariableLock.Lock()
	gv := &GlobalV{
		taskId,
		"",
		"",
		[]string{},
		make(map[string]string),
		0,
		0,
		0,
		0,
		rds_config.TreeLevel,
		rds_config.Support,
		rds_config.Confidence,
		make(map[string][][2][]int32),
		&sync.RWMutex{},
		make(map[string]bool),
		&sync.Mutex{},
		nil,
		nil,
		make(map[string][]rds.Predicate),
		false,
		make(map[string]int), // 规则ree表达式到规则id的映射
		&sync.RWMutex{},
		nil,
		nil,
		nil,
		nil,
		nil,
		[0]int32{},
		&sync.RWMutex{},
		nil,
		nil,
		make(map[string]map[interface{}][]int32),
		make(chan rds.Rule, 1000),
		make(chan rds.Rule, 1000),
		false,
		false,
		false,
		make(map[string][]interface{}),
		make(map[string][]interface{}),
		make([]string, 0),
		make([]string, 0),
		make([][6]interface{}, 0),
		make([][6]interface{}, 0),
		make(map[string][]interface{}),
		rds_config.PolyMaxLength,
		rds_config.PolyMinConfidence,
		rds_config.PolySampleCount,
		rds_config.AllowableError,
		rds_config.MultiPolySampleGroup,
		false,
		false,
		nil,
		&sync.Mutex{},
		false,
		2,
		//make(map[string]map[string][]interface{}),
		make(map[string]string),
		[2][]rds.Predicate{},
		make(map[string][]rds.Predicate),
		//make(map[string]map[string]struct{}),
		make(map[string]map[string]struct{}),
		map[string]map[string]string{},
		make(map[string][]string),
		make(map[int]float64),
		1,
		[]float64{},
		rds_config.PredicateSupportLimit,
		rds_config.StartTopKLayer,
		rds_config.TopK,
		rds_config.DecisionTreeMaxDepth,
		rds_config.CrossTablePredicateSupp,
		0,
		1,
		[]*task_tree.TaskTree{},
		map[string]int{},
		map[string]bool{},
		map[string]bool{},
		map[string]igeenum.DtRole{},
		map[string]string{},
		[]rds.Predicate{},
		rds_config.DecisionTreeMaxRowSize,
		rds_config.EnumSize,
		rds_config.SimilarThreshold,
		rds_config.MLThreshold,
		rds_config.TableRuleLimit,
		rds_config.RdsSize,
		rds_config.EnableErRule,
		rds_config.EnableTimeRule,
		rds_config.EnableDecisionTree,
		rds_config.EnableEmbedding,
		rds_config.EnableML,
		rds_config.EnableSimilar,
		rds_config.EnableEnum,
		map[string][]int{},
		0,
		map[string]string{},
		map[string]string{},
		nil,
		nil,
		false,
		false,
		false,
	}
	GlobalVariable[taskId] = gv
	GlobalVariableLock.Unlock()
	return gv
}

func DeleteGV(taskId int64) {
	GlobalVariableLock.Lock()
	GlobalVariable[taskId] = nil
	GlobalVariableLock.Unlock()
}

var Ch = genTokenChan(1.5)

func genTokenChan(coefficient float64) chan struct{} {
	// 后端那边直接限制了cpu的核数要留1个
	cpuNum := runtime.NumCPU() - 1
	if cpuNum <= 0 {
		cpuNum = 1
	}
	tokenNum := int(coefficient * float64(cpuNum))
	if tokenNum <= 0 {
		tokenNum = 1
	}

	if rds_config.UseStorage {
		tokenNum = 1
	}

	ch := make(chan struct{}, tokenNum)
	for i := 0; i < tokenNum; i++ {
		ch <- struct{}{}
	}
	return ch
}

var IsJobRunning = false
