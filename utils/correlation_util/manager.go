package correlation_util

import (
	"sort"
)

// CalculatorManager 计算器管理器
type CalculatorManager struct {
	workerNum         int
	filterStrategy    string
	calculatorCluster chan *correlationCalculator
	results           []CorrelationResult
	indexStack        []string
	table             []string
}

func NewCalculatorManager(label []float64, strategy string, workerNum int) *CalculatorManager {
	calculatorChan := make(chan *correlationCalculator, workerNum)
	labelRation := nullRatio(label)
	//logger.Debugf("Y Label Null Value: %v", labelRation)
	for i := 0; i < workerNum; i++ {
		go func() {
			//TODO 还有优化空间
			newCalculator := NewCalculator(label, labelRation)
			calculatorChan <- &newCalculator
		}()
	}
	for true {
		if len(calculatorChan) == workerNum {
			break
		}
	}
	return &CalculatorManager{workerNum, strategy, calculatorChan, make([]CorrelationResult, 0), make([]string, 0), make([]string, 0)}
}

// AppendColIndex 添加需要计算的属性
func (cm *CalculatorManager) AppendColIndex(index string) {
	cm.indexStack = append(cm.indexStack, index)
}

// AppendColIndex 添加属性的表
func (cm *CalculatorManager) AppendColTable(table string) {
	cm.table = append(cm.table, table)
}

// Run 计算
func (cm *CalculatorManager) Run(df *correlationDF) {
	resultChan := make(chan CorrelationResult, len(cm.indexStack))
	for i := 0; i < len(cm.indexStack); i++ {
		freeCalculator := <-cm.calculatorCluster
		index := i
		go func() {
			result := freeCalculator.CalculateOneIndex(cm.indexStack[index], cm.table[index], df)
			resultChan <- result
			cm.calculatorCluster <- freeCalculator

		}()
	}

	for i := 0; i < len(cm.indexStack); i++ {
		cm.results = append(cm.results, <-resultChan)
	}
	cm.indexStack = make([]string, 0)
}

func (cm *CalculatorManager) GetAbandonRowIndex(top int) map[string]int {
	topIndex := make(map[string]int)
	if top > len(cm.results) {
		for _, r := range cm.results {
			topIndex[r.Index] = 1
		}
		return topIndex
	}
	//kendall Top
	sort.Slice(cm.results, func(i, j int) bool {
		return cm.results[i].Kendall > cm.results[j].Kendall
	})

	for i, r := range cm.results {
		if r.Kendall != 0 {
			topIndex[r.Index] = 1
		}
		if i >= top {
			break
		}
	}
	//pearson Top
	sort.Slice(cm.results, func(i, j int) bool {
		return cm.results[i].Pearson > cm.results[j].Pearson
	})
	for i, r := range cm.results {
		topIndex[r.Index] = 1
		if i >= top {

			break
		}
	}
	//spearman Top
	sort.Slice(cm.results, func(i, j int) bool {
		return cm.results[i].Spearman > cm.results[j].Spearman
	})
	for i, r := range cm.results {
		topIndex[r.Index] = 1
		if i >= top {
			break
		}
	}
	return topIndex
}

func (cm *CalculatorManager) GetResult() []CorrelationResult {
	return cm.results
}

type correlationDF struct {
	data map[string][]float64
}

func newCorrelationDF() correlationDF {
	return correlationDF{data: make(map[string][]float64)}
}

func (fdf *correlationDF) getValue(index string, d []float64) {
	target := fdf.data[index]
	for i := 0; i < len(d); i++ {
		d[i] = target[i]
	}
}
