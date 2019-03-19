/*
// =====================================================================================
//
//       Filename:  fixedPointSamplingFilter.go
//
//    Description:  固定点采样过滤器
//
//        Version:  1.0
//        Created:  09/25/2018 11:43:21 AM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package filter

import (
	"errors"
	"fmt"

	"github.com/golang/glog"
)

/*
// ===  FUNCTION  ======================================================================
//         Name:  FixedPointSamplingFilter
//  Description:  固定点采样过滤器
// =====================================================================================
*/
func FixedPointSamplingFilter(input []int64, dataOutput []int64, indexOutput []int) error {
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
	var step = float32(inputLen) / float32(outputLen)
	var inputIndex float32 = 0
	var inputIndexInt = int(inputIndex)
	var outputIndexInt = 0
	var n float32 = 0
	for outputIndexInt < outputLen {
		inputIndex = step * n
		inputIndexInt = int(inputIndex)

		indexOutput[outputIndexInt] = inputIndexInt
		dataOutput[outputIndexInt] = input[inputIndexInt]

		outputIndexInt++
		n++
	}

	return nil
}
