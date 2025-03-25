package inc_rule_dig

import "gitlab.grandhoo.com/rock/rock-share/base/logger"

const CdfLen = 15

var CdfValues = []float64{0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95}

func MergeCDF(cdf1 []int, cdf2 []int, mergeTotal bool) []int {
	newCDF := make([]int, len(cdf1))
	for i, val := range cdf1 {
		if !mergeTotal && i == 0 {
			newCDF[i] = val
		} else {
			newCDF[i] = val + cdf2[i]
		}
	}
	return newCDF
}

func AddConfidence2CDF(confidence float64, cdf []int) {
	indexes := GetCDFIndexByConfidence(confidence)
	for _, idx := range indexes {
		cdf[idx] += 1
	}
}

func GetValidAndInvalidCount(confidence float64, cdf []int) (valid int, invalid int) {
	// 使用二分查找找到目标值应插入的位置
	insertPos, isMid := binarySearch(CdfValues, confidence)
	logger.Infof("[GetValidAndTotalCount] confidence:%v, insertPos:%v, isMid:%v", confidence, insertPos, isMid)

	if isMid {
		valid = cdf[insertPos]
	} else {
		valid = cdf[insertPos-1]
	}

	return valid, cdf[0] - valid
}

func GetCDFIndexByConfidence(confidence float64) []int {
	indexes := make([]int, 0, len(CdfValues))
	for i, val := range CdfValues {
		if confidence >= val {
			indexes = append(indexes, i)
		}
	}
	return indexes
}

// 二分查找插入点
func binarySearch(arr []float64, target float64) (insertPos int, isMid bool) {
	left, right := 0, len(arr)-1

	for left <= right {
		mid := left + (right-left)/2

		// 如果目标值等于中间值
		if arr[mid] == target {
			return mid, true
		}

		// 如果目标值小于中间值
		if arr[mid] > target {
			right = mid - 1
		} else {
			// 如果目标值大于中间值
			left = mid + 1
		}
	}

	// 返回插入点位置
	return left, false
}

func GetCDFIndexByConfidenceNew(confidence float64) []int {
	indexes := make([]int, 0, CdfLen)
	if confidence >= 0.0 {
		indexes = append(indexes, 0)
	}
	if confidence >= 0.1 {
		indexes = append(indexes, 1)
	}
	if confidence >= 0.2 {
		indexes = append(indexes, 2)
	}
	if confidence >= 0.3 {
		indexes = append(indexes, 3)
	}
	if confidence >= 0.4 {
		indexes = append(indexes, 4)
	}
	if confidence >= 0.5 {
		indexes = append(indexes, 5)
	}
	if confidence >= 0.55 {
		indexes = append(indexes, 6)
	}
	if confidence >= 0.6 {
		indexes = append(indexes, 7)
	}
	if confidence >= 0.65 {
		indexes = append(indexes, 8)
	}
	if confidence >= 0.7 {
		indexes = append(indexes, 9)
	}
	if confidence >= 0.75 {
		indexes = append(indexes, 10)
	}
	if confidence >= 0.8 {
		indexes = append(indexes, 11)
	}
	if confidence >= 0.85 {
		indexes = append(indexes, 12)
	}
	if confidence >= 0.9 {
		indexes = append(indexes, 13)
	}
	if confidence >= 0.95 {
		indexes = append(indexes, 14)
	}
	return indexes
}
