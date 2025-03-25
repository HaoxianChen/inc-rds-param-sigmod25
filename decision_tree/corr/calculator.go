package corr

import (
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/format"
	"math"
)

// CorrelationResult 相关性分析结果
type CorrelationResult struct {
	Index                      string
	Kendall, Pearson, Spearman float64
}

// 相关性单线程计算器
type correlationCalculator struct {
	labelNaNRatio                                              float64
	label, data                                                []float64
	reuseRankSlice                                             []rank
	reuseFloat64SliceA, reuseFloat64SliceB, reuseFloat64SliceC []float64
	reuseBoolSlice                                             []bool
}

// TODO 对于kendall与spearman的空值处理，还与panda存在差别，目前可以出结果，但是可能存在效果上的差异
func NewCalculator(label []float64, nanRation float64) correlationCalculator {
	newLabelSlice := make([]float64, len(label))
	for i := 0; i < len(label); i++ {
		newLabelSlice[i] = label[i]
	}
	return correlationCalculator{
		nanRation,
		newLabelSlice,
		make([]float64, len(label)),
		make([]rank, len(label)),
		make([]float64, len(label)),
		make([]float64, len(label)),
		make([]float64, len(label)+1),
		make([]bool, len(label)+1)}
}

func (ca *correlationCalculator) CalculateOneIndex(index string, df *format.DataFrame) CorrelationResult {
	df.GetAllValuesOf(index, ca.data)

	spearmanValue := SpearmanV3(ca.data, ca.label, ca.reuseRankSlice, ca.reuseFloat64SliceA, ca.reuseFloat64SliceB)
	if math.IsNaN(spearmanValue) {
		spearmanValue = 0
	}

	pearsonValue := pearsonV3(ca.label, ca.data)
	if math.IsNaN(pearsonValue) {
		pearsonValue = 0
	}

	//TODO 对于空值太多数据，kendall计算目前会有问题，目前先默认将空值高于0.3的数据的kendall系数直接置为0
	kendallValue := 0.0
	if ca.labelNaNRatio > 0.3 || nullRatio(ca.data) > 0.3 {
		kendallValue = 0.0
	} else {
		if len(ca.data) < 10000 {
			kendallValue = kendallExact(ca.label, ca.data)
		} else {
			kendallValue = kendallAsymptotic(ca.label, ca.data, ca.reuseFloat64SliceA, ca.reuseFloat64SliceB, ca.reuseFloat64SliceC, ca.reuseBoolSlice)
		}
	}
	if math.IsNaN(kendallValue) {
		kendallValue = 0
	}

	result := CorrelationResult{index, math.Abs(kendallValue), math.Abs(pearsonValue), math.Abs(spearmanValue)}
	return result
}
