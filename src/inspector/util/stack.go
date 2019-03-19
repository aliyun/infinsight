/*
// =====================================================================================
//
//       Filename:  stack.go
//
//    Description:  数组栈
//
//        Version:  1.0
//        Created:  09/21/2018 03:51:02 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package util

type Stack struct {
	array []interface{}
	index int
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  NewStack
//  Description:
// =====================================================================================
*/
func NewStack() *Stack {
	return &Stack{
		array: make([]interface{}, 0),
		index: 0,
	}
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Push
//  Description:
// =====================================================================================
*/
func (stack *Stack) Push(e interface{}) {
	stack.array = append(stack.array, e)
	stack.index++
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Pop
//  Description:
// =====================================================================================
*/
func (stack *Stack) Pop() interface{} {
	if stack.index == 0 {
		return nil
	}
	stack.index--
	var e = stack.array[stack.index]
	stack.array = stack.array[:stack.index]
	return e
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Top
//  Description:
// =====================================================================================
*/
func (stack *Stack) Top() interface{} {
	return stack.array[stack.index-1]
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Size
//  Description:
// =====================================================================================
*/
func (stack *Stack) Size() int {
	return stack.index
}
