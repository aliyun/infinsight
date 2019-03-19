/*
// =====================================================================================
//
//       Filename:  filter_test.go
//
//    Description:
//
//        Version:  1.0
//        Created:  09/25/2018 11:40:18 AM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package filter

import (
	"fmt"
	"inspector/util"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

func TestFixedPointSamplingFilter(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	arrayEqual := func(x, y []int) bool {
		for i := 0; i < len(x); i++ {
			if x[i] != y[i] {
				return false
			}
		}
		return true
	}

	arrayEqualInt64 := func(x, y []int64) bool {
		for i := 0; i < len(x); i++ {
			if x[i] != y[i] {
				return false
			}
		}
		return true
	}

	var input = []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	var indexOutput []int
	var dataOutput []int64
	var err error

	// case 1
	{
		input = []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		indexOutput = make([]int, 10)
		dataOutput = make([]int64, 10)
		err = FixedPointSamplingFilter(input, dataOutput, indexOutput)
		check(err == nil, "test")
		check(arrayEqualInt64(dataOutput, input), "test")
		check(arrayEqual(indexOutput, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}), "test")
	}

	// case 2
	{
		input = []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		indexOutput = make([]int, 6)
		dataOutput = make([]int64, 6)
		err = FixedPointSamplingFilter(input, dataOutput, indexOutput)
		check(err == nil, "test")
		check(arrayEqualInt64(dataOutput, []int64{1, 2, 4, 6, 7, 9}), "test")
		check(arrayEqual(indexOutput, []int{0, 1, 3, 5, 6, 8}), "test")
	}

	// case 3
	{
		input = []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		indexOutput = make([]int, 5)
		dataOutput = make([]int64, 5)
		err = FixedPointSamplingFilter(input, dataOutput, indexOutput)
		check(err == nil, "test")
		check(arrayEqualInt64(dataOutput, []int64{1, 3, 5, 7, 9}), "test")
		check(arrayEqual(indexOutput, []int{0, 2, 4, 6, 8}), "test")
	}

	// case 4
	{
		input = []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		indexOutput = make([]int, 3)
		dataOutput = make([]int64, 3)
		err = FixedPointSamplingFilter(input, dataOutput, indexOutput)
		check(err == nil, "test")
		check(arrayEqualInt64(dataOutput, []int64{1, 4, 7}), "test")
		check(arrayEqual(indexOutput, []int{0, 3, 6}), "test")
	}

	// case 5
	{
		input = []int64{1, util.NullData, 3, util.NullData, 5, util.NullData, 7, util.NullData, 9, util.NullData}
		indexOutput = make([]int, 3)
		dataOutput = make([]int64, 3)
		err = FixedPointSamplingFilter(input, dataOutput, indexOutput)
		check(err == nil, "test")
		check(arrayEqualInt64(dataOutput, []int64{1, util.NullData, 7}), "test")
		check(arrayEqual(indexOutput, []int{0, 3, 6}), "test")
	}

	// case 6
	{
		input = []int64{util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData}
		indexOutput = make([]int, 3)
		dataOutput = make([]int64, 3)
		err = FixedPointSamplingFilter(input, dataOutput, indexOutput)
		check(err == nil, "test")
		check(arrayEqualInt64(dataOutput, []int64{util.NullData, util.NullData, util.NullData}), "test")
		check(arrayEqual(indexOutput, []int{0, 3, 6}), "test")
	}

	check(true, "test")
}

func TestPeakSamplingFilter(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	arrayEqual := func(x, y []int) bool {
		for i := 0; i < len(x); i++ {
			if x[i] != y[i] {
				return false
			}
		}
		return true
	}

	arrayEqualInt64 := func(x, y []int64) bool {
		for i := 0; i < len(x); i++ {
			if x[i] != y[i] {
				return false
			}
		}
		return true
	}

	var input = []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	var indexOutput []int
	var dataOutput []int64
	var err error

	// case 1
	{
		input = []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		indexOutput = make([]int, 10)
		dataOutput = make([]int64, 10)
		err = PeakSamplingFilter(input, dataOutput, indexOutput)
		check(err == nil, "test")
		check(arrayEqual(indexOutput, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}), "test")
		check(arrayEqualInt64(dataOutput, input), "test")
	}

	// case 2
	{
		input = []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		indexOutput = make([]int, 6)
		dataOutput = make([]int64, 6)
		err = PeakSamplingFilter(input, dataOutput, indexOutput)
		check(err == nil, "test")
		check(arrayEqual(indexOutput, []int{0, 2, 3, 5, 6, 9}), "test")
		check(arrayEqualInt64(dataOutput, []int64{1, 3, 4, 6, 7, 10}), "test")
	}

	// case 3
	{
		input = []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		indexOutput = make([]int, 5)
		dataOutput = make([]int64, 5)
		err = PeakSamplingFilter(input, dataOutput, indexOutput)
		check(err == nil, "test")
		check(arrayEqual(indexOutput, []int{0, 3, 4, 7, 8}), "test")
		check(arrayEqualInt64(dataOutput, []int64{1, 4, 5, 8, 9}), "test")
	}

	// case 4
	{
		input = []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		indexOutput = make([]int, 3)
		dataOutput = make([]int64, 3)
		err = PeakSamplingFilter(input, dataOutput, indexOutput)
		check(err == nil, "test")
		check(arrayEqual(indexOutput, []int{0, 5, 6}), "test")
		check(arrayEqualInt64(dataOutput, []int64{1, 6, 7}), "test")
	}

	// case 5
	{
		input = []int64{
			96, 35, 2, 99, 43, 37, 56, 64, 26, 91,
			70, 77, 45, 25, 42, 51, 5, 89, 32, 64,
			52, 34, 9, 77, 42, 38, 10, 93, 72, 68,
			96, 94, 71, 20, 29, 27, 36, 81, 14, 9,
			8, 73, 10, 89, 61, 35, 27, 72, 81, 85,
			99, 3, 27, 86, 26, 96, 94, 82, 49, 90,
			52, 54, 9, 79, 87, 16, 8, 14, 59, 85,
			68, 5, 48, 88, 20, 12, 80, 39, 51, 23,
			59, 21, 39, 37, 96, 35, 87, 44, 55, 86,
			45, 37, 99, 26, 25, 100, 88, 72, 27, 5,
		}
		indexOutput = make([]int, 20)
		dataOutput = make([]int64, 20)
		err = PeakSamplingFilter(input, dataOutput, indexOutput)
		check(err == nil, "test")
		check(arrayEqual(indexOutput, []int{
			2, 3,
			16, 17,
			22, 27,
			30, 39,
			40, 43,
			50, 51,
			64, 66,
			71, 73,
			81, 84,
			95, 99,
		}), "test")
		check(arrayEqualInt64(dataOutput, []int64{
			2, 99,
			5, 89,
			9, 93,
			96, 9,
			8, 89,
			99, 3,
			87, 8,
			5, 88,
			21, 96,
			100, 5,
		}), "test")
	}

	// case 6
	{
		input = []int64{1, util.NullData, 3, util.NullData, 5, util.NullData, 7, util.NullData, 9, util.NullData}
		indexOutput = make([]int, 5)
		dataOutput = make([]int64, 5)
		err = PeakSamplingFilter(input, dataOutput, indexOutput)
		check(err == nil, "test")
		check(arrayEqual(indexOutput, []int{0, 2, 4, 6, 8}), "test")
		check(arrayEqualInt64(dataOutput, []int64{1, 3, 5, 7, 9}), "test")
	}

	// case 7
	{
		input = []int64{util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData}
		indexOutput = make([]int, 5)
		dataOutput = make([]int64, 5)
		err = PeakSamplingFilter(input, dataOutput, indexOutput)
		check(err == nil, "test")
		check(arrayEqual(indexOutput, []int{1, 2, 5, 6, 8}), "test")
		check(arrayEqualInt64(dataOutput, []int64{util.NullData, util.NullData, util.NullData, util.NullData, util.NullData}), "test")
	}

	// case 8
	{
		input = make([]int64, 1800)
		s := rand.NewSource(time.Now().Unix())
		r := rand.New(s)
		for i := 0; i < 1800; i++ {
			input[i] = r.Int63()
			input[i] = int64(i)
		}
		indexOutput = make([]int, 1000)
		dataOutput = make([]int64, 1000)
		err = PeakSamplingFilter(input, dataOutput, indexOutput)
		check(err == nil, "test")
	}

	// case 9
	{
		input = []int64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
		indexOutput = make([]int, 10)
		dataOutput = make([]int64, 10)
		err = PeakSamplingFilter(input, dataOutput, indexOutput)
		check(err == nil, "test")
		check(arrayEqual(indexOutput, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}), "test")
		check(arrayEqualInt64(dataOutput, input), "test")
	}

	check(true, "test")
}

func TestPeakSamplingFilterAvg(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	arrayEqual := func(x, y []int) bool {
		for i := 0; i < len(x); i++ {
			if x[i] != y[i] {
				return false
			}
		}
		return true
	}

	arrayEqualInt64 := func(x, y []int64) bool {
		for i := 0; i < len(x); i++ {
			if x[i] != y[i] {
				return false
			}
		}
		return true
	}

	var input = []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	var indexOutput []int
	var dataOutput []int64
	var err error

	// case 1
	{
		input = []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		indexOutput = make([]int, 10)
		dataOutput = make([]int64, 10)
		err = PeakSamplingFilterAvg(input, dataOutput, indexOutput)
		check(err == nil, "test")
		check(arrayEqual(indexOutput, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}), "test")
		check(arrayEqualInt64(dataOutput, input), "test")
	}

	// case 2
	{
		input = []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		indexOutput = make([]int, 5)
		dataOutput = make([]int64, 5)
		err = PeakSamplingFilterAvg(input, dataOutput, indexOutput)
		check(err == nil, "test")
		check(arrayEqual(indexOutput, []int{0, 2, 4, 6, 8}), "test")
		check(arrayEqualInt64(dataOutput, []int64{1, 4, 5, 8, 9}), "test")
	}

	// case 3
	{
		input = []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		indexOutput = make([]int, 3)
		dataOutput = make([]int64, 3)
		err = PeakSamplingFilterAvg(input, dataOutput, indexOutput)
		check(err == nil, "test")
		check(arrayEqual(indexOutput, []int{0, 3, 6}), "test")
		check(arrayEqualInt64(dataOutput, []int64{1, 6, 7}), "test")
	}

	check(true, "test")
}
