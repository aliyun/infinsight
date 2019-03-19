/*
// =====================================================================================
//
//       Filename:  ringcache_test.go
//
//    Description:  基于时间的环形队列，只支持int的存储
//
//        Version:  1.0
//        Created:  07/02/2018 05:54:42 PM
//       Compiler:  g++
//
// =====================================================================================
*/

package cache

import (
	"fmt"
	"inspector/util"
	"runtime"
	"testing"
)

func TestPushBackAndQuery(t *testing.T) {
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

	var err error
	var timestamp uint32
	var data []int64

	var rc = new(RingCache)
	rc.Init("test", 10)

	// case 0
	{
		timestamp, data = rc.Query(0, uint32(1000), nil)
		check(timestamp == 0, "test")
		check(data == nil, "test")
	}

	// case 1
	{
		err = rc.PushBack(1, uint32(1000), int64(1000))
		check(err == nil, "test")
		timestamp, data = rc.Query(1, uint32(1000), nil)
		check(timestamp == 991, "test")
		check(arrayEqual(data, []int64{util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, 1000}), "test")

		err = rc.PushBack(1, uint32(1001), int64(1001))
		check(err == nil, "test")
		timestamp, data = rc.Query(1, uint32(1001), nil)
		check(timestamp == 992, "test")
		check(arrayEqual(data, []int64{util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, 1000, 1001}), "test")

		err = rc.PushBack(1, uint32(1002), int64(1002))
		check(err == nil, "test")
		timestamp, data = rc.Query(1, uint32(1002), nil)
		check(timestamp == 993, "test")
		check(arrayEqual(data, []int64{util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, 1000, 1001, 1002}), "test")

		err = rc.PushBack(1, uint32(1003), int64(1003))
		check(err == nil, "test")
		timestamp, data = rc.Query(1, uint32(1003), nil)
		check(timestamp == 994, "test")
		check(arrayEqual(data, []int64{util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, 1000, 1001, 1002, 1003}), "test")

		err = rc.PushBack(1, uint32(1004), int64(1004))
		check(err == nil, "test")
		timestamp, data = rc.Query(1, uint32(1004), nil)
		check(timestamp == 995, "test")
		check(arrayEqual(data, []int64{util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, 1000, 1001, 1002, 1003, 1004}), "test")

		err = rc.PushBack(1, uint32(1005), int64(1005))
		check(err == nil, "test")
		timestamp, data = rc.Query(1, uint32(1005), nil)
		check(timestamp == 996, "test")
		check(arrayEqual(data, []int64{util.NullData, util.NullData, util.NullData, util.NullData, 1000, 1001, 1002, 1003, 1004, 1005}), "test")

		err = rc.PushBack(1, uint32(1006), int64(1006))
		check(err == nil, "test")
		timestamp, data = rc.Query(1, uint32(1006), nil)
		check(timestamp == 997, "test")
		check(arrayEqual(data, []int64{util.NullData, util.NullData, util.NullData, 1000, 1001, 1002, 1003, 1004, 1005, 1006}), "test")

		err = rc.PushBack(1, uint32(1007), int64(1007))
		check(err == nil, "test")
		timestamp, data = rc.Query(1, uint32(1007), nil)
		check(timestamp == 998, "test")
		check(arrayEqual(data, []int64{util.NullData, util.NullData, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007}), "test")

		err = rc.PushBack(1, uint32(1008), int64(1008))
		check(err == nil, "test")
		timestamp, data = rc.Query(1, uint32(1008), nil)
		check(timestamp == 999, "test")
		check(arrayEqual(data, []int64{util.NullData, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008}), "test")

		err = rc.PushBack(1, uint32(1009), int64(1009))
		check(err == nil, "test")
		timestamp, data = rc.Query(1, uint32(1009), nil)
		check(timestamp == 1000, "test")
		check(arrayEqual(data, []int64{1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009}), "test")
	}

	// case 2
	for i := 1010; i < 2000; i++ {
		var arrayCheck = make([]int64, 10)
		for j := 0; j < 10; j++ {
			arrayCheck[j] = int64(i - 10 + j + 1)
		}

		err = rc.PushBack(1, uint32(i), int64(i))
		check(err == nil, "test")
		timestamp, data = rc.Query(1, uint32(i), nil)
		check(timestamp == uint32(i-9), "test")
		check(arrayEqual(data, arrayCheck), "test")
	}

	check(true, "test")
}

func TestPushBackPrevious(t *testing.T) {
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

	var err error
	var timestamp uint32
	var data []int64

	var rc = new(RingCache)
	rc.Init("test", 10)

	for i := 1000; i < 1010; i++ {
		err = rc.PushBack(1, uint32(i), int64(i))
		check(err == nil, "test")
	}

	// case 1
	err = rc.PushBack(1, uint32(1005), int64(1015))
	check(err == nil, "test")
	timestamp, data = rc.Query(1, uint32(1009), nil)
	check(timestamp == 1000, "test")
	check(arrayEqual(data, []int64{1000, 1001, 1002, 1003, 1004, 1015, 1006, 1007, 1008, 1009}), "test")

	err = rc.PushBack(1, uint32(1009), int64(1019))
	check(err == nil, "test")
	timestamp, data = rc.Query(1, uint32(1009), nil)
	check(timestamp == 1000, "test")
	check(arrayEqual(data, []int64{1000, 1001, 1002, 1003, 1004, 1015, 1006, 1007, 1008, 1019}), "test")

	err = rc.PushBack(1, uint32(999), int64(1018))
	check(err != nil, "test")

	check(true, "test")
}

func TestSkipPushBack(t *testing.T) {
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

	var err error
	var timestamp uint32
	var data []int64

	var rc = new(RingCache)
	rc.Init("test", 10)

	// case 1
	{
		err = rc.PushBack(1, uint32(1000), int64(1000))
		check(err == nil, "test")
		timestamp, data = rc.Query(1, uint32(1000), nil)
		check(timestamp == 991, "test")
		check(arrayEqual(data, []int64{util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, 1000}), "test")

		err = rc.PushBack(1, uint32(1005), int64(1005))
		check(err == nil, "test")
		timestamp, data = rc.Query(1, uint32(1005), nil)
		check(timestamp == 996, "test")
		check(arrayEqual(data, []int64{util.NullData, util.NullData, util.NullData, util.NullData, 1000, util.NullData, util.NullData, util.NullData, util.NullData, 1005}), "test")

		err = rc.PushBack(1, uint32(1009), int64(1009))
		check(err == nil, "test")
		timestamp, data = rc.Query(1, uint32(1009), nil)
		check(timestamp == 1000, "test")
		check(arrayEqual(data, []int64{1000, util.NullData, util.NullData, util.NullData, util.NullData, 1005, util.NullData, util.NullData, util.NullData, 1009}), "test")
	}

	// case 2
	{
		err = rc.PushBack(1, uint32(1015), int64(1015))
		check(err == nil, "test")
		timestamp, data = rc.Query(1, uint32(1009), nil)
		check(timestamp == 1000, "test")
		check(arrayEqual(data, []int64{util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, 1009}), "test")
	}

	// case 3
	{
		timestamp, data = rc.Query(1, uint32(1029), nil)
		check(timestamp == 1020, "test")
		check(arrayEqual(data, []int64{util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData}), "test")

		timestamp, data = rc.Query(1, uint32(1019), nil)
		check(timestamp == 1010, "test")
		check(arrayEqual(data, []int64{util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, 1015, util.NullData, util.NullData, util.NullData, util.NullData}), "test")

		timestamp, data = rc.Query(1, uint32(1015), nil)
		check(timestamp == 1006, "test")
		check(arrayEqual(data, []int64{util.NullData, util.NullData, util.NullData, 1009, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, 1015}), "test")

		timestamp, data = rc.Query(1, uint32(1010), nil)
		check(timestamp == 1001, "test")
		check(arrayEqual(data, []int64{util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, 1009, util.NullData}), "test")

		timestamp, data = rc.Query(1, uint32(1000), nil)
		check(timestamp == 991, "test")
		check(arrayEqual(data, []int64{util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData}), "test")
	}

	check(true, "test")
}

func TestClean(t *testing.T) {
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

	var err error
	var timestamp uint32
	var data []int64

	var rc = new(RingCache)
	rc.Init("test", 10)

	err = rc.PushBack(1, uint32(1000), int64(1000))
	check(err == nil, "test")

	timestamp, data = rc.Query(1, uint32(1000), nil)
	check(timestamp == 991, "test")
	check(arrayEqual(data, []int64{util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, 1000}), "test")

	rc.CleanExpire(1000)
	timestamp, data = rc.Query(1, uint32(1000), nil)
	check(timestamp == 991, "test")
	check(arrayEqual(data, []int64{util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, 1000}), "test")

	rc.CleanExpire(1010)
	timestamp, data = rc.Query(1, uint32(1000), nil)
	check(timestamp == 991, "test")
	check(arrayEqual(data, []int64{util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, 1000}), "test")

	rc.CleanExpire(1021)
	timestamp, data = rc.Query(1, uint32(1000), nil)
	check(timestamp == 0, "test")
	check(data == nil, "test")

	check(true, "test")
}

func BenchmarkRingCache(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	var rc = new(RingCache)
	rc.Init("test", 10)

	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			rc.PushBack(j, uint32(1000+i), int64(i))
		}
	}
}
