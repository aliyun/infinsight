/*
// =====================================================================================
//
//       Filename:  ringcache-map_test.go
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

import "fmt"
import "runtime"
import "testing"

func TestMapPushBackAndQuery(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var ok bool
	var rc = new(RingCacheMap)
	var ts uint32
	var data []int64
	rc.Init("test", 10)

	ts, data = rc.QueryCurrent("cpu")
	check(ts == 0, "test")
	check(data == nil, "test")

	ts, data = rc.QueryPrevious("cpu")
	check(ts == 0, "test")
	check(data == nil, "test")

	for i := 0; i < 10; i++ {
		ok = rc.PushBack("cpu", uint32(1000+i), int64(i))
		check(ok, "test")
		ts, data = rc.QueryCurrent("cpu")
		check(ts == 1000, "test")
		check(len(data) == i+1, "test")
		ts, data = rc.QueryPrevious("cpu")
		check(len(data) == 10, "test")
		for i := 0; i < 10; i++ {
			check(data[i] == 0xffffffff, "test")
		}
	}

	for i := 10; i < 20; i++ {
		ok = rc.PushBack("cpu", uint32(1000+i), int64(i))
		check(ok, "test")
		ts, data = rc.QueryCurrent("cpu")
		check(ts == 1010, "test")
		check(len(data) == i-9, "test")
		ts, data = rc.QueryPrevious("cpu")
		check(len(data) == 10, "test")
		for i := 0; i < 10; i++ {
			check(data[i] == int64(i), "test")
		}
	}

	for i := 20; i < 30; i++ {
		ok = rc.PushBack("cpu", uint32(1000+i), int64(i))
		check(ok, "test")
		ts, data = rc.QueryCurrent("cpu")
		check(ts == 1020, "test")
		check(len(data) == i-19, "test")
		ts, data = rc.QueryPrevious("cpu")
		check(len(data) == 10, "test")
		for i := 0; i < 10; i++ {
			check(data[i] == int64(i+10), "test")
		}
	}

	check(true, "test")
}

func TestMapSkipPushBack(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var ok bool
	var rc = new(RingCacheMap)
	var ts uint32
	var data []int64
	rc.Init("test", 10)

	rc.PushBack("cpu", uint32(1000), int64(1000))
	rc.PushBack("cpu", uint32(1002), int64(1002))
	rc.PushBack("cpu", uint32(1003), int64(1003))
	rc.PushBack("cpu", uint32(1005), int64(1005))
	rc.PushBack("cpu", uint32(1007), int64(1007))
	rc.PushBack("cpu", uint32(1011), int64(1011))
	rc.PushBack("cpu", uint32(1013), int64(1013))
	rc.PushBack("cpu", uint32(1017), int64(1017))
	rc.PushBack("cpu", uint32(1019), int64(1019))

	ts, data = rc.QueryPrevious("cpu")
	check(ts == 1000, "test")
	check(len(data) == 10, "test")
	check(data[0] == 1000, "test")
	check(data[1] == 0xffffffff, "test")
	check(data[2] == 1002, "test")
	check(data[3] == 1003, "test")
	check(data[4] == 0xffffffff, "test")
	check(data[5] == 1005, "test")
	check(data[6] == 0xffffffff, "test")
	check(data[7] == 1007, "test")
	check(data[8] == 0xffffffff, "test")
	check(data[9] == 0xffffffff, "test")
	ts, data = rc.QueryCurrent("cpu")
	check(ts == 1010, "test")
	check(len(data) == 10, "test")
	check(data[0] == 0xffffffff, "test")
	check(data[1] == 1011, "test")
	check(data[2] == 0xffffffff, "test")
	check(data[3] == 1013, "test")
	check(data[4] == 0xffffffff, "test")
	check(data[5] == 0xffffffff, "test")
	check(data[6] == 0xffffffff, "test")
	check(data[7] == 1017, "test")
	check(data[8] == 0xffffffff, "test")
	check(data[9] == 1019, "test")

	ok = rc.PushBack("cpu", uint32(999), int64(999))
	check(!ok, "test")

	ts, data = rc.QueryPrevious("cpu")
	check(data[0] == 1000, "test")
	ok = rc.PushBack("cpu", uint32(1000), int64(0))
	ts, data = rc.QueryPrevious("cpu")
	check(data[0] == 0, "test")

	ts, data = rc.QueryPrevious("cpu")
	check(data[1] == 0xffffffff, "test")
	ok = rc.PushBack("cpu", uint32(1001), int64(1))
	ts, data = rc.QueryPrevious("cpu")
	check(data[1] == 1, "test")

	ok = rc.PushBack("cpu", uint32(1023), int64(1023))
	check(ok, "test")

	ok = rc.PushBack("cpu", uint32(1002), int64(2))
	check(!ok, "test")

	ts, data = rc.QueryPrevious("cpu")
	check(data[3] == 1013, "test")
	ok = rc.PushBack("cpu", uint32(1013), int64(1113))
	ts, data = rc.QueryPrevious("cpu")
	check(data[3] == 1113, "test")

	ts, data = rc.QueryPrevious("cpu")
	check(data[3] == 1113, "test")
	ok = rc.PushBack("cpu", uint32(1013), int64(13))
	ts, data = rc.QueryPrevious("cpu")
	check(data[3] == 13, "test")

	ts, data = rc.QueryPrevious("cpu")
	check(data[0] == 0xffffffff, "test")
	ok = rc.PushBack("cpu", uint32(1010), int64(10))
	ts, data = rc.QueryPrevious("cpu")
	check(data[0] == 10, "test")

	check(true, "test")
}

func TestMapClean(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var ok bool
	var rc = new(RingCacheMap)
	rc.Init("test", 10)

	ok = rc.PushBack("cpu", uint32(1000), int64(1010))
	check(ok, "test")
	ok = rc.PushBack("mem", uint32(1010), int64(1020))
	check(ok, "test")
	ok = rc.PushBack("io", uint32(1020), int64(1030))
	check(ok, "test")
	ok = rc.PushBack("net", uint32(1030), int64(1040))
	check(ok, "test")

	ok = rc.PushBack("cpu", uint32(100), int64(100))
	check(!ok, "test")
	ok = rc.PushBack("mem", uint32(100), int64(100))
	check(!ok, "test")
	ok = rc.PushBack("io", uint32(100), int64(100))
	check(!ok, "test")
	ok = rc.PushBack("net", uint32(100), int64(100))
	check(!ok, "test")

	rc.CleanExpire(1050)
	ok = rc.PushBack("cpu", uint32(100), int64(100))
	check(!ok, "test")
	ok = rc.PushBack("mem", uint32(100), int64(100))
	check(!ok, "test")
	ok = rc.PushBack("io", uint32(100), int64(100))
	check(!ok, "test")
	ok = rc.PushBack("net", uint32(100), int64(100))
	check(!ok, "test")

	rc.CleanExpire(1051)
	ok = rc.PushBack("cpu", uint32(100), int64(100))
	check(ok, "test")
	ok = rc.PushBack("mem", uint32(100), int64(100))
	check(!ok, "test")
	ok = rc.PushBack("io", uint32(100), int64(100))
	check(!ok, "test")
	ok = rc.PushBack("net", uint32(100), int64(100))
	check(!ok, "test")

	rc.CleanExpire(1061)
	ok = rc.PushBack("cpu", uint32(100), int64(100))
	check(ok, "test")
	ok = rc.PushBack("mem", uint32(100), int64(100))
	check(ok, "test")
	ok = rc.PushBack("io", uint32(100), int64(100))
	check(!ok, "test")
	ok = rc.PushBack("net", uint32(100), int64(100))
	check(!ok, "test")

	rc.CleanExpire(1081)
	ok = rc.PushBack("cpu", uint32(100), int64(100))
	check(ok, "test")
	ok = rc.PushBack("mem", uint32(100), int64(100))
	check(ok, "test")
	ok = rc.PushBack("io", uint32(100), int64(100))
	check(ok, "test")
	ok = rc.PushBack("net", uint32(100), int64(100))
	check(ok, "test")

	check(true, "test")
}

func BenchmarkRingCacheMap(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	var rc = new(RingCacheMap)
	rc.Init("test", 10)

	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			rc.PushBack(fmt.Sprintf("item%d", j), uint32(1000+i), int64(i))
		}
	}
}
