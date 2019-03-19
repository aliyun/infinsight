/*
// ===================================================================================== //
//       Filename:  unary_operation.go
//
//    Description:  一元数组运算
//
//        Version:  1.0
//        Created:  09/19/2018 08:11:10 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package syntax

import (
	"inspector/util"
)

/*
// ===  FUNCTION  ======================================================================
//         Name:  sum
//  Description:  返回数组中所有元素的累加和
// =====================================================================================
*/
func sum(input []int64) int64 {
	var sum int64 = 0
	for _, it := range input {
		if it == util.NullData {
			continue
		}
		sum += it
	}
	return sum
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  arrayDiff
//  Description:  差值计算，0下标不变，其他下标位置写入当前值与前值的差值
// =====================================================================================
*/
func arrayDiff(pinput *[]int64, poutput *[]int64) *[]int64 {
	var input = *pinput
	var output []int64
	if len(input) == 0 {
		*poutput = []int64{}
		return poutput
	}
	if poutput == nil {
		output = make([]int64, len(input))
	} else {
		output = *poutput
	}
	for i := len(input) - 1; i > 0; i-- {
		if input[i] == util.NullData || input[i-1] == util.NullData {
			output[i] = util.NullData
		} else {
			output[i] = input[i] - input[i-1]
		}
	}
	return &output
}
