/*
// =====================================================================================
//
//       Filename:  dyadic_operation.go
//
//    Description:  二元数组运算
//
//        Version:  1.0
//        Created:  09/19/2018 08:11:10 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package syntax

import "inspector/util"

/*
// ===  FUNCTION  ======================================================================
//         Name:  arrayAdd
//  Description:  数组间加法运算
// =====================================================================================
*/
func arrayAdd(px, py *[]int64, poutput *[]int64) *[]int64 {
	return arrayOP(px, py, '+', poutput)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  arraySub
//  Description:  数组间减法运算
// =====================================================================================
*/
func arraySub(px, py *[]int64, poutput *[]int64) *[]int64 {
	return arrayOP(px, py, '-', poutput)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  arrayMul
//  Description:  数组间乘法运算
// =====================================================================================
*/
func arrayMul(px, py *[]int64, poutput *[]int64) *[]int64 {
	return arrayOP(px, py, '*', poutput)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  arrayDiv
//  Description:  数组间除法运算
// =====================================================================================
*/
func arrayDiv(px, py *[]int64, poutput *[]int64) *[]int64 {
	return arrayOP(px, py, '/', poutput)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  arrayMod
//  Description:  数组间取余运算
// =====================================================================================
*/
func arrayMod(px, py *[]int64, poutput *[]int64) *[]int64 {
	return arrayOP(px, py, '%', poutput)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  arrayOP
//  Description:  数组间运算
// =====================================================================================
*/
func arrayOP(px, py *[]int64, op byte, poutput *[]int64) *[]int64 {
	var x = *px
	var y = *py
	var result []int64
	if poutput == nil {
		if len(x) <= len(y) {
			result = make([]int64, len(x))
		} else {
			result = make([]int64, len(y))
		}
	} else {
		result = *poutput
	}

	switch op {
	case '+':
		for i := 0; i < len(result); i++ {
			if x[i] == util.NullData || y[i] == util.NullData {
				result[i] = util.NullData
			} else {
				result[i] = x[i] + y[i]
			}
		}
	case '-':
		for i := 0; i < len(result); i++ {
			if x[i] == util.NullData || y[i] == util.NullData {
				result[i] = util.NullData
			} else {
				result[i] = x[i] - y[i]
			}
		}
	case '*':
		for i := 0; i < len(result); i++ {
			if x[i] == util.NullData || y[i] == util.NullData {
				result[i] = util.NullData
			} else {
				result[i] = x[i] * y[i]
			}
		}
	case '/':
		for i := 0; i < len(result); i++ {
			if x[i] == util.NullData || y[i] == util.NullData {
				result[i] = util.NullData
			} else {
				result[i] = x[i] / y[i]
			}
		}
	case '%':
		for i := 0; i < len(result); i++ {
			if x[i] == util.NullData || y[i] == util.NullData {
				result[i] = util.NullData
			} else {
				result[i] = x[i] % y[i]
			}
		}
	default:
		return nil
	}
	return &result
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  arrayDigitAdd
//  Description:  数组与数值加法运算
// =====================================================================================
*/
func arrayDigitAdd(parray *[]int64, digit int64, poutput *[]int64) *[]int64 {
	return arrayDigitOP(parray, digit, '+', poutput)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  arrayDigitSub
//  Description:  数组与数值减法运算
// =====================================================================================
*/
func arrayDigitSub(parray *[]int64, digit int64, poutput *[]int64) *[]int64 {
	return arrayDigitOP(parray, digit, '-', poutput)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  arrayDigitMul
//  Description:  数组与数值乘法运算
// =====================================================================================
*/
func arrayDigitMul(parray *[]int64, digit int64, poutput *[]int64) *[]int64 {
	return arrayDigitOP(parray, digit, '*', poutput)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  arrayDigitDiv
//  Description:  数组与数值除法运算
// =====================================================================================
*/
func arrayDigitDiv(parray *[]int64, digit int64, poutput *[]int64) *[]int64 {
	return arrayDigitOP(parray, digit, '/', poutput)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  arrayDigitMod
//  Description:  数组与数值取余运算
// =====================================================================================
*/
func arrayDigitMod(parray *[]int64, digit int64, poutput *[]int64) *[]int64 {
	return arrayDigitOP(parray, digit, '%', poutput)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  arrayDigitOP
//  Description:  数组与数值运算
// =====================================================================================
*/
func arrayDigitOP(parray *[]int64, digit int64, op byte, poutput *[]int64) *[]int64 {
	var array = *parray
	var result []int64
	if poutput == nil {
		result = make([]int64, len(array))
	} else {
		result = *poutput
	}

	switch op {
	case '+':
		for i := 0; i < len(array); i++ {
			if array[i] == util.NullData {
				result[i] = util.NullData
			} else {
				result[i] = array[i] + digit
			}
		}
	case '-':
		for i := 0; i < len(array); i++ {
			if array[i] == util.NullData {
				result[i] = util.NullData
			} else {
				result[i] = array[i] - digit
			}
		}
	case '*':
		for i := 0; i < len(array); i++ {
			if array[i] == util.NullData {
				result[i] = util.NullData
			} else {
				result[i] = array[i] * digit
			}
		}
	case '/':
		for i := 0; i < len(array); i++ {
			if array[i] == util.NullData {
				result[i] = util.NullData
			} else {
				result[i] = array[i] / digit
			}
		}
	case '%':
		for i := 0; i < len(array); i++ {
			if array[i] == util.NullData {
				result[i] = util.NullData
			} else {
				result[i] = array[i] % digit
			}
		}
	default:
		return nil
	}
	return &result
}
