package corr

import (
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/format"
	"sort"
	"sync"
)

// CalculatorManager 计算器管理器
type CalculatorManager struct {
	workerNum         int
	filterStrategy    string
	calculatorCluster chan *correlationCalculator
	results           []CorrelationResult
	indexStack        []string
}

func NewCalculatorManager(label []float64, strategy string, workerNum int) *CalculatorManager {
	calculatorChan := make(chan *correlationCalculator, workerNum)
	labelRation := nullRatio(label)
	//logger.Infof("length: %v", len(calculatorChan))
	//log.Warn().Msgf("Y Label Null Value: %v", labelRation)

	var wg sync.WaitGroup
	for i := 0; i < workerNum; i++ {
		wg.Add(1)
		//logger.Infof("before create i:%v", i)
		go func(i int) {
			defer wg.Done()
			//logger.Infof("create i:%v", i)
			newCalculator := NewCalculator(label, labelRation)
			//logger.Infof("finish new calculator, i:%v", i)
			calculatorChan <- &newCalculator
			//logger.Infof("length: %v", len(calculatorChan))
		}(i)
	}

	wg.Wait()
	//for true {
	//	if len(calculatorChan) == workerNum {
	//		break
	//	}
	//}
	return &CalculatorManager{workerNum, strategy, calculatorChan, make([]CorrelationResult, 0), make([]string, 0)}
}

// AppendColIndex 添加需要计算的属性
func (cm *CalculatorManager) AppendColIndex(index string) {
	cm.indexStack = append(cm.indexStack, index)
}

// Run 计算
func (cm *CalculatorManager) Run(df *format.DataFrame) {
	resultChan := make(chan CorrelationResult, len(cm.indexStack))
	for i := 0; i < len(cm.indexStack); i++ {
		freeCalculator := <-cm.calculatorCluster
		index := i
		go func() {
			result := freeCalculator.CalculateOneIndex(cm.indexStack[index], df)
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

//type fakeDF struct {
//	data map[string][]float64
//}
//
//func (fdf *fakeDF) getValue(index string, d []float64) {
//	target := fdf.data[index]
//	for i := 0; i < len(d); i++ {
//		d[i] = target[i]
//	}
//}
