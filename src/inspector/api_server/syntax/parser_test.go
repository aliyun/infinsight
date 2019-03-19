/*
// =====================================================================================
//
//       Filename:  parser_test.go
//
//    Description:
//
//        Version:  1.0
//        Created:  09/19/2018 08:19:19 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package syntax

import (
	"fmt"
	"inspector/util"
	"runtime"
	"testing"
)

func TestUnary(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	arrayEqual := func(x, y []int64) bool {
		for i := 0; i < len(x); i++ {
			if x[i] != y[i] {
				return false
			}
		}
		return true
	}

	var array []int64
	array = []int64{1, 2, util.NullData, 4, 5}
	var result = *arrayDiff(&array, &array)
	check(arrayEqual(result, []int64{1, 1, util.NullData, util.NullData, 1}), "test")

	check(sum([]int64{util.NullData, 1, 2, 3, 4, -1}) == 9, "test")

	check(true, "test")
}

func TestDyadic(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	arrayEqual := func(x, y []int64) bool {
		for i := 0; i < len(x); i++ {
			if x[i] != y[i] {
				return false
			}
		}
		return true
	}

	var x = []int64{1, 2, util.NullData, 4, 5}
	var y = []int64{2, 3, 4, util.NullData, 6}
	var a []int64 = make([]int64, 5)
	var result []int64

	// case 1
	{
		result = *arrayAdd(&x, &y, &a)
		check(arrayEqual(result, []int64{3, 5, util.NullData, util.NullData, 11}), "test")
		check(arrayEqual(result, a), "test")
		result = *arraySub(&y, &x, &a)
		check(arrayEqual(result, []int64{1, 1, util.NullData, util.NullData, 1}), "test")
		check(arrayEqual(result, a), "test")
		result = *arrayMul(&x, &y, &a)
		check(arrayEqual(result, []int64{2, 6, util.NullData, util.NullData, 30}), "test")
		check(arrayEqual(result, a), "test")
		result = *arrayDiv(&y, &x, &a)
		check(arrayEqual(result, []int64{2, 1, util.NullData, util.NullData, 1}), "test")
		check(arrayEqual(result, a), "test")
		result = *arrayMod(&y, &x, &a)
		check(arrayEqual(result, []int64{0, 1, util.NullData, util.NullData, 1}), "test")
		check(arrayEqual(result, a), "test")
	}

	// case 2
	{
		result = *arrayDigitAdd(&y, 1, &a)
		check(arrayEqual(result, []int64{3, 4, 5, util.NullData, 7}), "test")
		check(arrayEqual(result, a), "test")
		result = *arrayDigitSub(&y, 1, &a)
		check(arrayEqual(result, []int64{1, 2, 3, util.NullData, 5}), "test")
		check(arrayEqual(result, a), "test")
		result = *arrayDigitMul(&y, -1, &a)
		check(arrayEqual(result, []int64{-2, -3, -4, util.NullData, -6}), "test")
		check(arrayEqual(result, a), "test")
		result = *arrayDigitDiv(&y, 2, &a)
		check(arrayEqual(result, []int64{1, 1, 2, util.NullData, 3}), "test")
		check(arrayEqual(result, a), "test")
		result = *arrayDigitMod(&y, 2, &a)
		check(arrayEqual(result, []int64{0, 1, 0, util.NullData, 0}), "test")
		check(arrayEqual(result, a), "test")
	}

	check(true, "test")
}

func TestParse(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	arrayEqual := func(x, y []int64) bool {
		for i := 0; i < len(x); i++ {
			if x[i] != y[i] {
				return false
			}
		}
		return true
	}

	var result [][]int64
	var err error
	var params = [][]int64{
		[]int64{1, 2, 3, 4, 5},
		[]int64{2, 3, 4, 5, 6},
		[]int64{1, 1, 1, 1, 1},
		[]int64{2, 2, 2, 2, 2},
	}

	// case 1
	result, err = ArrayCalculation("$1", params[0])
	check(err == nil, "test")
	check(arrayEqual(result[0], []int64{1, 2, 3, 4, 5}), "test")

	// case 2
	result, err = ArrayCalculation("arrayAdd($1, $2)", params[0], params[1])
	check(err == nil, "test")
	check(arrayEqual(result[0], []int64{3, 5, 7, 9, 11}), "test")

	// case 3
	result, err = ArrayCalculation("arrayDiv(arrayMul(arraySub($3, arrayAdd($1, $2)), $4), $4)",
		params[0], params[1], params[2], params[3])
	check(err == nil, "test")
	check(arrayEqual(result[0], []int64{-2, -4, -6, -8, -10}), "test")

	// case 4
	result, err = ArrayCalculation("arrayDigitDiv(arrayDigitMul(arrayDigitSub(arrayDigitAdd($1, 2), 1), 4), 2)",
		params[0], params[1], params[2], params[3])
	check(err == nil, "test")
	check(arrayEqual(result[0], []int64{4, 6, 8, 10, 12}), "test")

	// case 5
	result, err = ArrayCalculation("arrayDigitAdd($1, sum($2))", params[0], params[1])
	check(err == nil, "test")
	check(arrayEqual(result[0], []int64{21, 22, 23, 24, 25}), "test")

	// case 6
	result, err = ArrayCalculation("arrayDiff(arrayDigitAdd($1, sum($2)))", params[0], params[1])
	check(err == nil, "test")
	check(arrayEqual(result[0], []int64{21, 1, 1, 1, 1}), "test")

	// case 7
	result, err = ArrayCalculation("sum($0)", params...)
	check(err == nil, "test")
	check(len(result) == 4, "test")
	check(result[0][0] == 15, "test")
	check(result[1][0] == 20, "test")
	check(result[2][0] == 5, "test")
	check(result[3][0] == 10, "test")

	// case 8
	result, err = ArrayCalculation("arrayDiff($0)", params...)
	check(err == nil, "test")
	check(len(result) == 4, "test")
	check(arrayEqual(result[0], []int64{0, 1, 1, 1, 1}), "test")
	check(arrayEqual(result[1], []int64{0, 1, 1, 1, 1}), "test")
	check(arrayEqual(result[2], []int64{0, 0, 0, 0, 0}), "test")
	check(arrayEqual(result[3], []int64{0, 0, 0, 0, 0}), "test")

	check(true, "test")
}
