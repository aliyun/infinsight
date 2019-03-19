/*
// =====================================================================================
//
//       Filename:  peakSamplingFilter.go
//
//    Description:
//
//        Version:  1.0
//        Created:  09/25/2018 03:26:42 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package filter

import (
	"errors"
	"fmt"
	"inspector/util"

	"github.com/golang/glog"
)

func min(x, y int) int {
	if x <= y {
		return x
	} else {
		return y
	}
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  PeakSamplingFilter
//  Description:  极值采样过滤
// =====================================================================================
*/
func PeakSamplingFilter(input []int64, dataOutput []int64, indexOutput []int) error {
	if dataOutput == nil {
		var errStr = fmt.Sprintf("filter dataOutput buffer is nil")
		glog.Errorf(errStr)
		return errors.New(errStr)
	}
	var inputLen = len(input)
	var outputLen = len(dataOutput)
	if inputLen < outputLen {
		var errStr = fmt.Sprintf("input len should be larger than dataOutput len")
		glog.Errorf(errStr)
		return errors.New(errStr)
	}

	// golang的浮点数精度奇差无比，1.8累加1000次等于1799.992
	// 所以在不得不使用golang浮点数时，必须注意消除累计误差
	var n float32 = 0
	var step = float32(inputLen) / float32(outputLen) * 2
	var inputIndex float32 = 0
	var inputIndexInt = int(inputIndex)
	var outputIndexInt = 0
	var lastValueIsMinValue = true
	for inputIndexInt < inputLen {
		var l = inputIndexInt
		var r = min(int(inputIndex+step), len(input))
		var minValue int64 = util.INT64_MAX
		var maxValue int64 = util.INT64_MIN
		var minIndex = l + (r-l)*1/4
		var maxIndex = l + (r-l)*3/4
		for i := l; i < r; i++ {
			if input[i] == util.NullData {
				continue
			}
			if input[i] < minValue {
				minValue = input[i]
				minIndex = i
			}
			if input[i] > maxValue {
				maxValue = input[i]
				maxIndex = i
			}
		}
		if minIndex == maxIndex {
			minIndex = l
			maxIndex = (l + r) / 2
		}
		if outputLen%2 != 0 && outputIndexInt == outputLen-1 {
			// 特殊处理dataOutput长度为奇数的最后一条数据
			if lastValueIsMinValue {
				if minValue == util.INT64_MAX || maxValue == util.INT64_MIN {
					indexOutput[outputIndexInt] = (l + r) / 2
					dataOutput[outputIndexInt] = util.NullData
				} else {
					indexOutput[outputIndexInt] = maxIndex
					dataOutput[outputIndexInt] = maxValue
				}
				outputIndexInt++
			} else {
				if minValue == util.INT64_MIN {
					indexOutput[outputIndexInt] = (l + r) / 2
					dataOutput[outputIndexInt] = util.NullData
				} else {
					indexOutput[outputIndexInt] = minIndex
					dataOutput[outputIndexInt] = minValue
				}
				outputIndexInt++
			}
		} else {
			if minIndex <= maxIndex {
				if minValue == util.INT64_MAX {
					indexOutput[outputIndexInt] = (l + maxIndex) / 2
					dataOutput[outputIndexInt] = util.NullData
				} else {
					indexOutput[outputIndexInt] = minIndex
					dataOutput[outputIndexInt] = minValue
				}
				outputIndexInt++
				if maxValue == util.INT64_MIN || minIndex == maxIndex {
					indexOutput[outputIndexInt] = (minIndex + r) / 2
					dataOutput[outputIndexInt] = util.NullData
				} else {
					indexOutput[outputIndexInt] = maxIndex
					dataOutput[outputIndexInt] = maxValue
				}
				outputIndexInt++
			} else {
				if maxValue == util.INT64_MIN {
					indexOutput[outputIndexInt] = (l + minIndex) / 2
					dataOutput[outputIndexInt] = util.NullData
				} else {
					indexOutput[outputIndexInt] = maxIndex
					dataOutput[outputIndexInt] = maxValue
				}
				outputIndexInt++
				if minValue == util.INT64_MAX || minIndex == maxIndex {
					indexOutput[outputIndexInt] = r
					dataOutput[outputIndexInt] = util.NullData
				} else {
					indexOutput[outputIndexInt] = minIndex
					dataOutput[outputIndexInt] = minValue
				}
				outputIndexInt++
			}
		}
		if maxIndex <= minIndex {
			lastValueIsMinValue = true
		} else {
			lastValueIsMinValue = false
		}
		n++
		inputIndex = n * step
		inputIndexInt = int(inputIndex)
	}

	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  PeakSamplingFilterAvg
//  Description:  平均极值采样过滤
//                由于极限值采样过滤返回的时间点不是均匀的，所以使用PeakSamplingFilterAvg
//                对dataOutput中对应数据的时间点进行篡改，即引入误差，将数据分散均匀
// =====================================================================================
*/
func PeakSamplingFilterAvg(input []int64, dataOutput []int64, indexOutput []int) error {
	// 首先使用PeakSamplingFilter进行极值采样过滤
	var err = PeakSamplingFilter(input, dataOutput, indexOutput)
	if err != nil {
		return err
	}
	// 然后将indexOutput篡改为均匀时间
	var inputLen = len(input)
	var outputLen = len(dataOutput)
	var step = float32(inputLen) / float32(outputLen)
	var outputIndex float32 = 0
	var outputIndexInt = 0
	for i, _ := range indexOutput {
		indexOutput[i] = outputIndexInt
		outputIndex += step
		outputIndexInt = int(outputIndex)
	}
	return nil
}
