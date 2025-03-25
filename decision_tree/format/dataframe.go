package format

import (
	//"github.com/yourbasic/bit"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"math"
	"sort"
)

type DataFrame struct {
	data            [][]float64
	featureIndexMap map[string]int // y的key也放这里面吧
	validXIndexes   []string       // 有序
	originXIndexes  map[string]int // 记录可以访问的所有feature的indexMap，featureIndexMap为什么不直接用这个呢，还是考虑了某些列本不该存在是吧，好像也没必要之后看看
	yIndex          string

	yValueNumToValue     map[float64]interface{} //y值编号到y真实值的映射(因为y不一定是数字类型，将y作为枚举映射成数字去执行，转规则时再转为真实值)
	columnValuesSorted   map[string][]float64
	splitValue2RealValue map[string]map[float64]float64
}

// NewDataFrame todo:限制xList的顺序与data中对应，y在data的最后一列，如果不一致的话validX那要改一下
func NewDataFrame(data [][]float64, xList []string, y string) *DataFrame {
	idMap := make(map[string]int, len(xList))
	allIdMap := make(map[string]int, len(xList)) // 在一开始所有的xIndex都是有效的，所以idMap和allIdMap是一样的
	for i, x := range xList {
		idMap[x] = i
		allIdMap[x] = i
	}
	idMap[y] = len(xList) // 默认y在最后一列
	allIdMap[y] = len(xList)

	validXList := make([]string, len(xList))
	copy(validXList, xList)
	sort.Slice(validXList, func(i, j int) bool {
		return validXList[i] < validXList[j]
	})

	return &DataFrame{
		data:            data,
		featureIndexMap: idMap,
		originXIndexes:  allIdMap,
		yIndex:          y,

		//grh
		validXIndexes:        validXList,
		columnValuesSorted:   make(map[string][]float64),
		splitValue2RealValue: make(map[string]map[float64]float64),
		yValueNumToValue:     make(map[float64]interface{}),
	}
}

func (d *DataFrame) SetYNumToValueMap(yValueNumToValueMap map[float64]interface{}) {
	(*d).yValueNumToValue = yValueNumToValueMap
}

func (d *DataFrame) GetYNumToValueMap() map[float64]interface{} {
	return (*d).yValueNumToValue
}

func (d *DataFrame) SetColumnValuesSorted(feature string, valuesSorted []float64) {
	(*d).columnValuesSorted[feature] = valuesSorted
}

func (d *DataFrame) SetSplitValue2RealValue(feature string, splitValue, realValue float64) {
	splitValue2RealValue, exist := (*d).splitValue2RealValue[feature]
	if !exist {
		splitValue2RealValue = make(map[float64]float64)
	}
	splitValue2RealValue[splitValue] = realValue
	(*d).splitValue2RealValue[feature] = splitValue2RealValue
}

func (d *DataFrame) QueryExistRealValue(feature string, splitValue float64) (float64, bool) {
	splitValue2RealValue := (*d).splitValue2RealValue[feature]
	if splitValue2RealValue == nil {
		return 0, false
	}
	value, exist := splitValue2RealValue[splitValue]
	if !exist {
		return 0, false
	} else {
		return value, true
	}
}

func (d *DataFrame) GetColumnValuesSorted(feature string) []float64 {
	sortedValues, exist := (*d).columnValuesSorted[feature]
	if !exist {
		sortedValues = make([]float64, d.Len())
		(*d).GetAllValuesOf(feature, sortedValues)
		sort.Float64s(sortedValues)
		(*d).SetColumnValuesSorted(feature, sortedValues)
	}
	return sortedValues
}

// SetValidXList 只有这些属性在dataframe中是有效的
func (d *DataFrame) SetValidXList(xList ...string) {
	newFeatureMap := make(map[string]int, len(xList)+1)
	newFeatureMap[(*d).yIndex] = (*d).featureIndexMap[(*d).yIndex] // y保留
	for _, x := range xList {
		id, has := (*d).originXIndexes[x]
		if !has {
			panic("missing x")
		}
		newFeatureMap[x] = id
	}
	(*d).featureIndexMap = newFeatureMap
	newValid := make([]string, len(xList))
	copy(newValid, xList)
	sort.Slice(newValid, func(i, j int) bool {
		return newValid[i] < newValid[j]
	})
	(*d).validXIndexes = newValid
}

func (d *DataFrame) SetYIndex(y string) {
	(*d).yIndex = y
}

func (d *DataFrame) Len() int {
	return len((*d).data)
}

func (d *DataFrame) GetXIndexList() []string {
	// 只返回有效的
	return (*d).validXIndexes
}

func (d *DataFrame) GetYIndex() string {
	return (*d).yIndex
}

// IsNumeric 判断一个属性是不是数值类型的(判断是不是连续类型的)
func (d *DataFrame) IsNumeric(index string) bool {
	// todo:到时候看看怎么弄，可以在这个index里加个前缀啥的
	//grh
	for _, x := range d.validXIndexes {
		if index == x {
			return true
		}
	}
	return false
	//_, err := strconv.ParseFloat(index, 64)
	//return err == nil
	//panic("1234")
	//if lKey, ok := d.indexLiteralMap[index]; ok {
	//	return attribute.IsContinuousNumericAttribute(lKey.KeyCode())
	//}
	//if l, ok := d.oneHotLiteralMap[index]; ok {
	//	return attribute.IsContinuousNumericAttribute(l.KeyCode())
	//}
	//return false
}

func (d *DataFrame) GetPivotWithId(sampleId int) uint64 {
	// todo:暂时用不上
	return uint64(sampleId)
}

func (d *DataFrame) GetSomeInstances(source []int, targetNum int) []int {
	// todo:暂时用不上
	return nil
}

func (d *DataFrame) GetFloat64Element(sampleId int, feature string) float64 {
	col, has := (*d).featureIndexMap[feature]
	if !has {
		logger.Error("feature not exists in dataframe!", feature, (*d).featureIndexMap)
		return math.NaN()
	}
	return (*d).data[sampleId][col]
}

func (d *DataFrame) GetFloat64ElementByCol(row int, col int) float64 {
	return d.data[row][col]
}

func (d *DataFrame) GetSpecificValueList(samples []int, feature string, target []float64) {
	col, has := (*d).featureIndexMap[feature]
	if !has {
		logger.Warnf("feature not exists in dataframe! %v --> %v", feature, (*d).featureIndexMap)
		fillNum := len(target)
		for i := 0; i < fillNum; i++ {
			target[i] = math.NaN()
		}
		return
	}
	for i, sampleId := range samples {
		target[i] = (*d).data[sampleId][col]
	}
}

func (d *DataFrame) GetAllValuesOf(feature string, target []float64) {
	col, has := (*d).featureIndexMap[feature]
	if !has {
		logger.Warnf("feature not exists in dataframe! %v --> %v", feature, (*d).featureIndexMap)
		fillNum := len(target)
		for i := 0; i < fillNum; i++ {
			target[i] = math.NaN()
		}
		return
	}
	num := d.Len()
	for i := 0; i < num; i++ {
		target[i] = d.GetFloat64ElementByCol(i, col)
	}
}

// grh
func (d *DataFrame) GetFeatureByValidIndex(index int) string {
	return d.validXIndexes[index]
}
