/*
// =====================================================================================
//
//       Filename:  parser.go
//
//    Description:
//
//        Version:  1.0
//        Created:  09/19/2018 08:06:31 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package syntax

import (
	"bytes"
	"errors"
	"fmt"
	"inspector/util"
	"inspector/util/unsafe"
	"strconv"
)

var priorityMap = map[byte]int{
	',': 0,
	')': 1,
	'+': 2, '-': 2,
	'*': 3, '/': 3, '%': 3,
	'a': 5, 'b': 5, 'c': 5, 'd': 5, 'e': 5, 'f': 5, 'g': 5, 'h': 5, 'i': 5, 'j': 5, 'k': 5, 'l': 5, 'm': 5,
	'n': 5, 'o': 5, 'p': 5, 'q': 5, 'r': 5, 's': 5, 't': 5, 'u': 5, 'v': 5, 'w': 5, 'x': 5, 'y': 5, 'z': 5,
	'A': 5, 'B': 5, 'C': 5, 'D': 5, 'E': 5, 'F': 5, 'G': 5, 'H': 5, 'I': 5, 'J': 5, 'K': 5, 'L': 5, 'M': 5,
	'N': 5, 'O': 5, 'P': 5, 'Q': 5, 'R': 5, 'S': 5, 'T': 5, 'U': 5, 'V': 5, 'W': 5, 'X': 5, 'Y': 5, 'Z': 5,
	'(': 9,
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  isDigit
//  Description:
// =====================================================================================
*/
func isDigit(x byte) bool {
	if x < '0' || x > '9' {
		return false
	}
	return true
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  ArrayCalculation
//  Description:  数组计算：将params矩阵每一行作为一个数组，进行数组计算
//                          $n表示使用第n行进行计算，$0表示使用每一行都进行相同的计算
//                          目前由于数组与数值混合计算，暂不提供运算符操作，所有的计算都必须使用函数
//                函数列表如下：
//                一元运算：
//                          sum($n)
//                          arrayDiff($n)
//                二元运算：
//                          arrayAdd($n, $n)
//                          arraySub($n, $n)
//                          arrayMul($n, $n)
//                          arrayDiv($n, $n)
//                          arrayMod($n, $n)
//                          arrayDigitAdd($n, num)
//                          arrayDigitSub($n, num)
//                          arrayDigitMul($n, num)
//                          arrayDigitDiv($n, num)
//                          arrayDigitMod($n, num)
// =====================================================================================
*/
func ArrayCalculation(format string, params ...[]int64) ([][]int64, error) {
	var result = make([][]int64, len(params))
	for i, _ := range result {
		result[i] = make([]int64, len(params[0]))
	}
	var ok bool
	var funcStack = util.NewStack()
	var dataStack = util.NewStack()

	var str = unsafe.String2Bytes(format)
	var i int = 0
	var j int = 0
	for i < len(str) {
		// empty byte
		if str[i] == ' ' || str[i] == '\t' || str[i] == '\n' {
			i++
			continue
		}
		// params list split
		if str[i] == ',' || str[i] == '(' {
			i++
			continue
		}

		// do func
		if str[i] == ')' {
			var data1 [][]int64
			var data2 [][]int64
			var numList [][]int64
			var ok bool

			var lastFunc = funcStack.Pop()
			switch opFunc := lastFunc.(type) {
			case func([]int64) int64:
				if data1, ok = dataStack.Pop().([][]int64); !ok {
					return nil, errors.New(fmt.Sprintf("invalid params at %d: expact []int64", j))
				}
				var k int
				for k = 0; k < len(data1); k++ {
					result[k][0] = opFunc(data1[k])
					result[k] = result[k]
				}
				result = result[:k]
				dataStack.Push(result)
			case func(*[]int64, *[]int64) *[]int64:
				if data1, ok = dataStack.Pop().([][]int64); !ok {
					return nil, errors.New(fmt.Sprintf("invalid params at %d: expact []int64", j))
				}
				for k, it := range data1 {
					result[k] = *opFunc(&it, &result[k])
				}
				dataStack.Push(result)
			case func(*[]int64, *[]int64, *[]int64) *[]int64:
				if data2, ok = dataStack.Pop().([][]int64); !ok {
					return nil, errors.New(fmt.Sprintf("invalid params at %d: expact []int64 for second param", j))
				}
				if data1, ok = dataStack.Pop().([][]int64); !ok {
					return nil, errors.New(fmt.Sprintf("invalid params at %d: expact []int64 for first param", j))
				}
				var k int
				for k = 0; k < len(data1) && k < len(data2); k++ {
					result[k] = *opFunc(&data1[k], &data2[k], &result[k])
				}
				result = result[:k]
				dataStack.Push(result)
			case func(*[]int64, int64, *[]int64) *[]int64:
				if numList, ok = dataStack.Pop().([][]int64); !ok {
					return nil, errors.New(fmt.Sprintf("invalid params at %d: expact int64 for second param", j))
				}
				if data1, ok = dataStack.Pop().([][]int64); !ok {
					return nil, errors.New(fmt.Sprintf("invalid params at %d: expact []int64 for first param", j))
				}
				var k int
				for k = 0; k < len(data1) && k < len(numList); k++ {
					result[k] = *opFunc(&data1[k], numList[k][0], &result[k])
				}
				dataStack.Push(result)
			default:
				return nil, errors.New(fmt.Sprintf("invalid func type at %d", j))
			}

			i++
			continue
		}

		// variable
		if str[i] == '$' {
			i++
			j = i
			for j < len(str) {
				if isDigit(str[j]) {
					j++
				} else {
					break
				}
			}
			if j == i {
				return nil, errors.New(fmt.Sprintf("invalid syntax at %d: expact digit after \"$\"", j))
			}
			if n, err := strconv.Atoi(unsafe.Bytes2String(str[i:j])); err != nil {
				return nil, errors.New(fmt.Sprintf("invalid syntax at %d: %s", j, err.Error()))
			} else {

				if n == 0 && len(params) > 0 { // $0
					dataStack.Push(params)
				} else { // $n
					if len(params) < n {
						return nil, errors.New(fmt.Sprintf("invalid param list at %d: params[%d] not exist", j, n))
					}
					dataStack.Push(params[n-1 : n])
				}
			}
			i = j
			continue
		}

		// digit
		if str[i] >= '0' && str[i] <= '9' {
			j = i
			j++
			for j < len(str) {
				if isDigit(str[j]) {
					j++
				} else {
					break
				}
			}
			if n, err := strconv.Atoi(unsafe.Bytes2String(str[i:j])); err != nil {
				return nil, errors.New(fmt.Sprintf("invalid syntax at %d: %s", j, err.Error()))
			} else {
				var numList = make([][]int64, len(params))
				for i, _ := range result {
					numList[i] = append(numList[i], int64(n))
				}
				dataStack.Push(numList)
			}
			i = j
			continue
		}

		// alphabet (func)
		if (str[i] >= 'a' && str[i] <= 'z') || (str[i] >= 'A' && str[i] <= 'Z') {
			if j = i + bytes.IndexByte(str[i:], '('); j == -1 {
				return nil, errors.New(fmt.Sprintf("invalid syntax at %d: expect \"(\" after func[%s]", i, unsafe.Bytes2String(str[i:])))
			}
			var funcName = unsafe.Bytes2String(str[i:j])
			switch funcName {
			case "sum":
				funcStack.Push(sum)
			case "arrayDiff":
				funcStack.Push(arrayDiff)
			case "arrayAdd":
				funcStack.Push(arrayAdd)
			case "arraySub":
				funcStack.Push(arraySub)
			case "arrayMul":
				funcStack.Push(arrayMul)
			case "arrayDiv":
				funcStack.Push(arrayDiv)
			case "arrayMod":
				funcStack.Push(arrayMod)
			case "arrayDigitAdd":
				funcStack.Push(arrayDigitAdd)
			case "arrayDigitSub":
				funcStack.Push(arrayDigitSub)
			case "arrayDigitMul":
				funcStack.Push(arrayDigitMul)
			case "arrayDigitDiv":
				funcStack.Push(arrayDigitDiv)
			case "arrayDigitMod":
				funcStack.Push(arrayDigitMod)
			default:
				return nil, errors.New(fmt.Sprintf("invalid syntax at %d: unexpect func[%s]", i, unsafe.Bytes2String(str[i:j])))
			}

			i = j
			continue
		}

		return nil, errors.New(fmt.Sprintf("invalid syntax at %d", i))
	}

	if result, ok = dataStack.Pop().([][]int64); !ok {
		return nil, errors.New(fmt.Sprintf("invalid final result: expect type is [][]int64"))
	}
	return result, nil
}
