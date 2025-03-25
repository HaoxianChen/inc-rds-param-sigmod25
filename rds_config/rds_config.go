package rds_config

import (
	"gitlab.grandhoo.com/rock/rock-share/global/utils/similarity/similarity_enum"
	"math"
)

const MAXCpuNum = 32

const GinPort = 19123

var GinPorts = []uint32{19123, 19124, 19125, 19126, 19127, 19128, 19129, 19130, 19131, 19132, 19133, 19134, 19135, 19136, 19137, 19138, 19139, 19140, 19141, 19142}

const PositiveSize = 100

const StartTopKLayer = 2

// 相似度谓词相关配置
const (
	RegexCheckNum    = 100
	RegexRatio       = 0.75
	SimilarThreshold = 0.9
	MLThreshold      = 0.9
	JaroWinkler      = similarity_enum.JaroWinkler
)

// 匹配列类型的正则表达式
const (
	AddressRegex = "^.+(市|区|镇|县|屯|).+(路|街|组|苑|村|小区|巷|区).+(栋|大厦|号|号楼|单元|楼|室|户).*$"
	CompanyRegex = "^.+(有限|公司|中心|大学|学校|医院|合作社|协会).*$"
)

const (
	RatioOfColumn = 0.1 //常数谓词频率
	TreeLevel     = 4   //所有种类树的最大层数，测试用，可取消
	TopK          = 50  //Topk谓词数
	RuleCoverage  = 0.8 // 当规则最大覆盖率不满足时,加入相似度谓词
)

// 开关
const (
	SaveIntersection = false
	UseStorage       = true
)

const (
	MultiFilterRowSize = 1000
	FilterCondition    = 7

	/*
		对于省份->区域和市->区域两条规则，区域按照中国七大区划分可以视为枚举类型，但无法在常数谓词->常数谓词被挖掘
		因此这两条规则暂时需用非常数谓词表述成

		t0^t1^t0.province=t1.province->t0.region=t1.region
		t0^t1^t0.city=t1.city->t0.region=t1.region

		根据原有筛选条件会被过滤掉，因此需要调整阈值使系统继续生成非常数谓词
	*/

)

// 谓词类型
const (
	Equal        = "="
	GreaterE     = ">="
	Less         = "<"
	Greater      = ">"
	NotEqual     = "!="
	Poly         = "poly"
	Similar      = "similar"
	PolyRight    = "true"
	Regular      = "regular"
	RegularRight = "true"
	ML           = "ML"
	Decode       = "decode"
	Category     = "category"
	TitleSimilar = "titleSimilar"
	Contains     = "contains"
	NotContains  = "notContains"
	List         = "list"
	GroupBy      = "groupBy"
)

// 存储中的数据类型
const (
	StringType = "string"
	IntType    = "int64"
	FloatType  = "float64"
	BoolType   = "bool"
	TimeType   = "time"
	TextType   = "text"
)

const (
	//系统中现在没有enum类型, string类型列unique值<某个阈值时该列为enum
	EnumType    = "enum"
	EnumSize    = 50
	GroupByEnum = "enum"
	ListType    = "list"
)

const (
	PredicateType_Const      = 0
	PredicateType_Struct     = 1
	PredicateType_FKey       = 2
	PredicateType_MultiTable = 3
	PredicateType_ML         = 4
	PredicateType_GroupBy    = 5
	PredicateType_Regular    = 6
)

// CR FTR
const (
	FTR = 0.9
	CR  = 0.05
)

// NilIndex nil 值索引
const NilIndex = int32(-1)

const PredicateRuleLimit = 15

// 聚集规则发现相关
const (
	SplitSymbol   = "__"
	Max           = "max"
	Min           = "min"
	Avg           = "avg"
	Sum           = "sum"
	Roc           = "roc"
	Formatted     = "formatted"
	MonthPeriod   = "month"
	QuarterPeriod = "quarter"
	YearPeriod    = "year"
)

// 多项式规则发现相关
const (
	PolyMaxLength                = 4   //计算树中的层数
	formulaRightValue    float32 = 0.0 //公式右手值
	PolyMinConfidence    float64 = 0.9
	PolySampleCount              = 1000
	Add                          = "+"
	Subtract                     = "-"
	Multiply                     = "*"
	Divide                       = "/"
	AllowableError               = 0.00001
	MultiPolySampleGroup         = 5000
)

var PolyOpts = [3]string{Add, Subtract, Multiply}
var PolyOptIndex = []int{0, 1, 2}

const SchemaMappingThreshold = float64(0.8)

const (
	JoinCrossPredicate  = 0
	OtherCrossPredicate = 1
)

const (
	CorrelationSampleCount = 5000
	WorkNum                = 1
)

const (
	DecisionTreeYNilReplace = math.MaxFloat64
	DecisionTreeMaxDepth    = 3
)

var DecisionTreeSampleThreshold2Ratio = map[int]float64{
	2000000:  0.5,
	10000000: 0.1,
}

const (
	PredicateSupportLimit = float64(0.95)
	TopKLayer             = 2
	TopKSize              = 20
	Confidence            = float64(0.9)
	Support               = float64(0.00001)
	TableRuleLimit        = 20
	RdsSize               = 10 * 10000 * 10000 // 10e
	DecisionNodeSize      = 20
	EnableCrossTable      = true
	EnableErRule          = true
	EnableTimeRule        = true
	EnableDecisionTree    = true
	EnableEmbedding       = true
	EnableML              = true
	EnableSimilar         = true
	EnableEnum            = true

	RdsSizeNoLimit          = -1
	CrossTablePredicateSupp = 0.5
)

const DecisionTreeMaxRowSize = 1000 * 10

const NewHyperCubeAlgo = false

const UdfColumnPrefix = "$"
const UdfColumnConn = "@"

const DecisionSplitSymbol = "$%"

const FpPath = "fp_path"
const DTPath = "decision_tree_path"
