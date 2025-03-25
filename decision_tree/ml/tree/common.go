package tree

import "math"

// FeatureId 标志一个属性
type FeatureId uint32 // todo:到时候把这个结构和dataFrame放一起吧，不然会循环引用

// AttributeType 属性的类别，如数值、非数值等
type AttributeType int8

const (
	NonNumeric       AttributeType = iota // NonNumeric 非数值类型，那么就是不能用这个值进行比较的
	NumericLimited                        // NumericLimited  是数值类型，可以比较大小，且可以按数值大小分类。但取值有限，可以不划分区间
	NumericUnlimited                      // NumericUnlimited 是数值类型，取值不限，也可以说是可取的值较多，需要划分区间
)

const SMALL_NUMERIC_LIMIT = 16 // 如果数值类型取值比这个小，认为是 NumericLimited，不需要划分区间
const EPSILON = 2.220446049250313e-16

var (
	INFINITY        = math.Inf(1)
	NEG_INFINITY    = math.Inf(-1)
	InstanceKeepNum = 5 // InstanceKeepNum 每个规则保存多少实例(正例)
	CoWorkerNum     = 0 // CoWorkerNum 与划分过程中并发相关，和主线程一起运行的worker的数量(也就是除了主协程之外还有几个协程一起)
)
