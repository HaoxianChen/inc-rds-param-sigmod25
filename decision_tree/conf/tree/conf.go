package tree

import (
	"math"
)

const (
	MinSupportLimit = 5 //最低的support限制
	CorrMask        = "corrAnalysis"
	YCountPosMask   = "yCountPos"
	YCountNegMask   = "yCountNeg"
)

var (
	TreeNum             = 1                      //随机森林中决策树的数量
	TopCol              = 20                     //相关性分析中输出的相关性系数最高的属性数
	OneHotStrategy      = 0                      //one-hot编码属性值的筛选策略
	MaxLeafNum          = 0                      //决策树划分次数，指定了这个参数就选用best-first的build方法了
	MaxTreeDepth        = 3                      //决策树的最大深度
	MaxInstance         = 1000000                //不采样的最大实例数，高于此数的实例要采样到该值
	SelectNum           = math.MaxInt64          //这里取最大，会在实际计算时和当前pattern下的对比
	MaxFeatureNum       = uint32(math.MaxUint32) //决策树中最大的特征数量
	MinConfidence       = 0.1                    //置信度的最低阈值，低于此的规则不输出
	MinSupportRate      = 0.00001                //支持度的比例，这里接入了水平扩展的支持度
	MinImpurityDecrease = 0.00                   //决策树中信息增益的阈值，低于此不继续分裂
	FilterFlag          = false                  //规则去重过滤的开关，默认为关
	YFlag               = true                   //过滤与Y同key属性的开关，默认为开
	CorrFlag            = true                   //相关性分析的开关，默认为开
	WeightDownByPivot   = false                  //根据pivot下实例数量，对各实例降低权重
	ImpurityCriterion   = "entropy"              //决策树目标函数的选择，默认为信息增益
)

type InsJson struct {
	InstanceXJson []map[string]interface{}
	InstanceYJson []map[string]interface{}
}
