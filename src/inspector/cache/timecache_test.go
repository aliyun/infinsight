package cache

import (
	"encoding/binary"
	"fmt"
	"inspector/proto/core"
	"runtime"
	"testing"
)

func TestCacheGet(t *testing.T) {
	// info
	var info *core.Info = new(core.Info)
	info.Header = new(core.Header)
	info.Header.Host = string("ins0")
	info.Timestamp = uint32(1000)
	info.Count = uint32(10)
	info.Step = uint32(1)
	info.Items = make([]*core.KVPair, 0)
	info.Items = append(info.Items, &core.KVPair{string("0a"), []byte("0a1000")})
	info.Items = append(info.Items, &core.KVPair{string("0b"), []byte("0b1000")})
	info.Items = append(info.Items, &core.KVPair{string("0c"), []byte("0c1000")})
	info.Items = append(info.Items, &core.KVPair{string("0d"), []byte("0d1000")})

	// query
	var query *core.Query = new(core.Query)
	query.Header = &core.Header{Host: string("ins0")}
	query.TimeBegin = uint32(500)
	query.TimeEnd = uint32(1500)
	query.KeyList = []string{"0a", "0f", "0c", "0d", "0e"}

	// run
	var timeBegin uint32
	var timeEnd uint32
	var err error
	var infoRanges []*core.InfoRange
	var tc *TimeCache = NewTimeCache(10, 100)
	err = tc.Set(info)
	if err != nil {
		t.Error("")
	}
	infoRanges, timeBegin, timeEnd, err = tc.Get(query)
	if err != nil {
		t.Error("")
	}
	if timeBegin != 1000 {
		t.Error("")
	}
	if timeEnd != 1009 {
		t.Error("")
	}
	if len(infoRanges) != 1 {
		t.Error("")
	}
	if infoRanges[0].Header.Host != "ins0" {
		t.Error("")
	}
	if infoRanges[0].Count != 10 {
		t.Error("")
	}
	if infoRanges[0].Step != 1 {
		t.Error("")
	}
	var x, y, z int
	var n int
	var key []byte
	x = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
	n += 4
	if x != 5 {
		t.Error("")
	}
	for i := 0; i < x; i++ {
		y = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
		n += 4
		key = infoRanges[0].Data[n : n+y]
		n += y
		if string(key) != query.KeyList[i] {
			t.Error("")
		}
		y = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
		if string(key) == "0f" || string(key) == "0e" {
			if y != 0 {
				t.Error("")
			}
			n += 4
		} else {
			if y != 1 {
				t.Error("")
			}
			n += 4
			for j := 0; j < y; j++ {
				timestamp := binary.BigEndian.Uint32(infoRanges[0].Data[n:])
				if timestamp != 1000 {
					t.Error("")
				}
				n += 4
				z = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
				n += 4
				data := infoRanges[0].Data[n : n+z]
				if string(data) != fmt.Sprintf("%s1000", key) {
					t.Error("")
				}
				n += z
			}
		}
	}
}

func TestCacheGetDiff(t *testing.T) {
	// info1
	var info1 *core.Info = new(core.Info)
	info1.Header = new(core.Header)
	info1.Header.Host = string("ins0")
	info1.Timestamp = uint32(1000)
	info1.Count = uint32(10)
	info1.Step = uint32(1)
	info1.Items = make([]*core.KVPair, 0)
	info1.Items = append(info1.Items, &core.KVPair{string("0a"), []byte("0a1000")})

	// info2
	var info2 *core.Info = new(core.Info)
	info2.Header = new(core.Header)
	info2.Header.Host = string("ins0")
	info2.Timestamp = uint32(1010)
	info2.Count = uint32(10)
	info2.Step = uint32(1)
	info2.Items = make([]*core.KVPair, 0)
	info2.Items = append(info2.Items, &core.KVPair{string("0a"), []byte("0a1010")})

	// info3
	var info3 *core.Info = new(core.Info)
	info3.Header = new(core.Header)
	info3.Header.Host = string("ins0")
	info3.Timestamp = uint32(1013)
	info3.Count = uint32(10)
	info3.Step = uint32(1)
	info3.Items = make([]*core.KVPair, 0)
	info3.Items = append(info3.Items, &core.KVPair{string("0a"), []byte("0a1013")})

	// query
	var query *core.Query = new(core.Query)
	query.Header = &core.Header{Host: string("ins0")}
	query.TimeBegin = uint32(500)
	query.TimeEnd = uint32(1500)

	// run
	var err error
	var timeBegin uint32
	var timeEnd uint32
	var infoRanges []*core.InfoRange
	var tc *TimeCache = NewTimeCache(10, 100)
	tc.Set(info1)
	tc.Set(info2)
	tc.Set(info3)
	infoRanges, timeBegin, timeEnd, err = tc.Get(query)
	if err != nil {
		t.Error("")
	}
	if timeBegin != 1000 {
		t.Error("")
	}
	if timeEnd != 1022 {
		t.Error("")
	}
	if len(infoRanges) != 2 {
		t.Error("")
	}
	if infoRanges[0].Header.Host != "ins0" {
		t.Error("")
	}

	if infoRanges[1].Header.Host != "ins0" {
		t.Error("")
	}

	var x, y, z int
	var n int
	var key []byte
	x = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
	n += 4
	if x != 1 {
		t.Error("")
	}
	for i := 0; i < x; i++ {
		y = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
		n += 4
		key = infoRanges[0].Data[n : n+y]
		n += y
		if string(key) != "0a" {
			t.Error("")
		}
		y = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
		if y != 2 {
			t.Error("")
		}
		n += 4
		for j := 0; j < y; j++ {
			timestamp := binary.BigEndian.Uint32(infoRanges[0].Data[n:])
			if j == 0 {
				if timestamp != 1000 {
					t.Error("")
				}
			}
			if j == 1 {
				if timestamp != 1010 {
					t.Error("")
				}
			}
			n += 4
			z = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
			n += 4
			data := infoRanges[0].Data[n : n+z]
			if j == 0 {
				if string(data) != "0a1000" {
					t.Error("")
				}
			}
			if j == 1 {
				if string(data) != "0a1010" {
					t.Error("")
				}
			}
			n += z
		}
	}

	n = 0
	x = int(binary.BigEndian.Uint32(infoRanges[1].Data[n:]))
	n += 4
	if x != 1 {
		t.Error("")
	}
	for i := 0; i < x; i++ {
		y = int(binary.BigEndian.Uint32(infoRanges[1].Data[n:]))
		n += 4
		key = infoRanges[1].Data[n : n+y]
		n += y
		if string(key) != "0a" {
			t.Error("")
		}
		y = int(binary.BigEndian.Uint32(infoRanges[1].Data[n:]))
		if y != 1 {
			t.Error("")
		}
		n += 4
		for j := 0; j < y; j++ {
			timestamp := binary.BigEndian.Uint32(infoRanges[1].Data[n:])
			if timestamp != 1013 {
				t.Error("")
			}
			n += 4
			z = int(binary.BigEndian.Uint32(infoRanges[1].Data[n:]))
			n += 4
			data := infoRanges[1].Data[n : n+z]
			if string(data) != "0a1013" {
				t.Error("")
			}
			n += z
		}
	}
}

func TestCacheTimeoutGet(t *testing.T) {
	// info1
	var info1 *core.Info = new(core.Info)
	info1.Header = new(core.Header)
	info1.Header.Host = string("ins0")
	info1.Timestamp = uint32(1000)
	info1.Count = uint32(10)
	info1.Step = uint32(1)
	info1.Items = make([]*core.KVPair, 0)
	info1.Items = append(info1.Items, &core.KVPair{string("0a"), []byte("0a1000")})

	// info2
	var info2 *core.Info = new(core.Info)
	info2.Header = new(core.Header)
	info2.Header.Host = string("ins0")
	info2.Timestamp = uint32(1210)
	info2.Count = uint32(10)
	info2.Step = uint32(1)
	info2.Items = make([]*core.KVPair, 0)
	info2.Items = append(info2.Items, &core.KVPair{string("0a"), []byte("0a1210")})

	// info3
	var info3 *core.Info = new(core.Info)
	info3.Header = new(core.Header)
	info3.Header.Host = string("ins0")
	info3.Timestamp = uint32(1413)
	info3.Count = uint32(10)
	info3.Step = uint32(1)
	info3.Items = make([]*core.KVPair, 0)
	info3.Items = append(info3.Items, &core.KVPair{string("0a"), []byte("0a1413")})

	// query
	var query *core.Query = new(core.Query)
	query.Header = &core.Header{Host: "ins0"}
	query.TimeBegin = uint32(500)
	query.TimeEnd = uint32(1500)

	// run
	var err error
	var timeBegin uint32
	var timeEnd uint32
	var infoRanges []*core.InfoRange
	var tc *TimeCache = NewTimeCache(10, 100)
	tc.Set(info1)
	tc.Set(info2)
	infoRanges, timeBegin, timeEnd, err = tc.Get(query)
	if err != nil {
		t.Error("")
	}
	if timeBegin != 1120 {
		t.Error("")
	}
	if timeEnd != 1219 {
		t.Error("")
	}
	if len(infoRanges) != 1 {
		t.Error("")
	}

	var x, y, z int
	var n int
	var key []byte
	x = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
	if x != 1 {
		t.Error("")
	}
	n += 4
	for i := 0; i < x; i++ {
		// get key
		y = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
		n += 4
		key = infoRanges[0].Data[n : n+y]
		n += y
		if string(key) != "0a" {
			t.Error("")
		}
		// get value count
		y = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
		if y != 10 {
			t.Error("")
		}
		n += 4
		// get values
		for j := 0; j < y; j++ {
			// get timestamp
			timestamp := binary.BigEndian.Uint32(infoRanges[0].Data[n:])
			if j == 0 {
				if timestamp != 1210-90 {
					t.Error("")
				}
			}
			n += 4
			// get value list count
			z = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
			n += 4
			// get value list
			data := infoRanges[0].Data[n : n+z]
			if j == 9 {
				if string(data) != "0a1210" {
					t.Error("")
				}
			}
			n += z
		}
	}

	tc.Set(info3)
	infoRanges, timeBegin, timeEnd, err = tc.Get(query)
	if err != nil {
		t.Error("")
	}
	if timeBegin != 1413 {
		t.Error("")
	}
	if timeEnd != 1422 {
		t.Error("")
	}
	if len(infoRanges) != 1 {
		t.Error("")
	}

	n = 0
	x = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
	if x != 1 {
		t.Error("")
	}
	n += 4
	for i := 0; i < x; i++ {
		// get key
		y = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
		n += 4
		key = infoRanges[0].Data[n : n+y]
		n += y
		if string(key) != "0a" {
			t.Error("")
		}
		// get value count
		y = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
		if y != 1 {
			t.Error("")
		}
		n += 4
		// get values
		for j := 0; j < y; j++ {
			// get timestamp
			timestamp := binary.BigEndian.Uint32(infoRanges[0].Data[n:])
			if j == 0 {
				if timestamp != 1413 {
					t.Error("")
				}
			}
			n += 4
			// get value list count
			z = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
			n += 4
			// get value list
			data := infoRanges[0].Data[n : n+z]
			if j == 0 {
				if string(data) != "0a1413" {
					t.Error("")
				}
			}
			n += z
		}
	}

}

func TestCacheGetNoExist(t *testing.T) {
	// info
	var info *core.Info = new(core.Info)
	info.Header = new(core.Header)
	info.Header.Host = string("ins0")
	info.Timestamp = uint32(1000)
	info.Count = uint32(10)
	info.Step = uint32(1)
	info.Items = make([]*core.KVPair, 0)
	info.Items = append(info.Items, &core.KVPair{string("0a"), []byte("0a1000")})
	info.Items = append(info.Items, &core.KVPair{string("0b"), []byte("0b1000")})
	info.Items = append(info.Items, &core.KVPair{string("0c"), []byte("0c1000")})
	info.Items = append(info.Items, &core.KVPair{string("0d"), []byte("0d1000")})

	// query
	var query *core.Query = new(core.Query)
	query.Header = &core.Header{Host: string("ins0")}
	query.TimeBegin = uint32(500)
	query.TimeEnd = uint32(1500)
	query.KeyList = []string{"0a", "0f", "0c", "0d", "0e"}

	// run
	var err error
	var timeBegin uint32
	var timeEnd uint32
	var infoRanges []*core.InfoRange
	var tc *TimeCache = NewTimeCache(10, 100)

	// Set
	err = tc.Set(info)
	if err != nil {
		t.Error("")
	}

	// Get no name
	query.Header.Host = string("ins1")
	infoRanges, timeBegin, timeEnd, err = tc.Get(query)
	if err == nil {
		t.Error("")
	}
	if timeBegin != 0 {
		t.Error("")
	}
	if timeEnd != 0 {
		t.Error("")
	}
	if infoRanges != nil {
		t.Error("")
	}
	query.Header.Host = string("ins0")

	// Get no time
	query.TimeBegin = uint32(2000)
	query.TimeEnd = uint32(3000)
	infoRanges, timeBegin, timeEnd, err = tc.Get(query)
	if len(infoRanges) != 0 {
		t.Error("")
	}

	// Get no key
	query.KeyList = []string{"a0", "f0", "c0", "d0", "e0"}
	infoRanges, timeBegin, timeEnd, err = tc.Get(query)
	if len(infoRanges) != 0 {
		t.Error("")
	}
}

func TestCacheGetInRandomTime(t *testing.T) {
	// info0
	var info0 *core.Info = new(core.Info)
	info0.Header = new(core.Header)
	info0.Header.Host = string("ins0")
	info0.Timestamp = uint32(1040)
	info0.Count = uint32(10)
	info0.Step = uint32(1)
	info0.Items = make([]*core.KVPair, 0)
	info0.Items = append(info0.Items, &core.KVPair{string("0a"), []byte("0a1040")})

	// info1
	var info1 *core.Info = new(core.Info)
	info1.Header = new(core.Header)
	info1.Header.Host = string("ins0")
	info1.Timestamp = uint32(1030)
	info1.Count = uint32(10)
	info1.Step = uint32(1)
	info1.Items = make([]*core.KVPair, 0)
	info1.Items = append(info1.Items, &core.KVPair{string("0a"), []byte("0a1030")})

	// info2
	var info2 *core.Info = new(core.Info)
	info2.Header = new(core.Header)
	info2.Header.Host = string("ins0")
	info2.Timestamp = uint32(1020)
	info2.Count = uint32(10)
	info2.Step = uint32(1)
	info2.Items = make([]*core.KVPair, 0)
	info2.Items = append(info2.Items, &core.KVPair{string("0a"), []byte("0a1020")})

	// info3
	var info3 *core.Info = new(core.Info)
	info3.Header = new(core.Header)
	info3.Header.Host = string("ins0")
	info3.Timestamp = uint32(1010)
	info3.Count = uint32(10)
	info3.Step = uint32(1)
	info3.Items = make([]*core.KVPair, 0)
	info3.Items = append(info3.Items, &core.KVPair{string("0a"), []byte("0a1010")})

	// info4
	var info4 *core.Info = new(core.Info)
	info4.Header = new(core.Header)
	info4.Header.Host = string("ins0")
	info4.Timestamp = uint32(1000)
	info4.Count = uint32(10)
	info4.Step = uint32(1)
	info4.Items = make([]*core.KVPair, 0)
	info4.Items = append(info4.Items, &core.KVPair{string("0a"), []byte("0a1000")})

	// query
	var query *core.Query = new(core.Query)
	query.Header = &core.Header{Host: string("ins0")}
	query.TimeBegin = uint32(500)
	query.TimeEnd = uint32(1500)

	// run
	var err error
	var timeBegin uint32
	var timeEnd uint32
	var infoRanges []*core.InfoRange
	var tc *TimeCache = NewTimeCache(10, 100)
	tc.Set(info0)
	tc.Set(info1)
	tc.Set(info2)
	tc.Set(info3)
	tc.Set(info4)
	infoRanges, timeBegin, timeEnd, err = tc.Get(query)
	if err != nil {
		t.Error("")
	}
	if timeBegin != 1040 {
		t.Error("")
	}
	if timeEnd != 1049 {
		t.Error("")
	}
	if len(infoRanges) != 1 {
		t.Error("")
	}
	if infoRanges[0].Header.Host != "ins0" {
		t.Error("")
	}

	var x, y, z int
	var n int
	var key []byte
	x = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
	n += 4
	if x != 1 {
		t.Error("")
	}
	for i := 0; i < x; i++ {
		y = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
		n += 4
		key = infoRanges[0].Data[n : n+y]
		n += y
		if string(key) != "0a" {
			t.Error("")
		}
		y = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
		if y != 1 {
			t.Error("")
		}
		n += 4
		for j := 0; j < y; j++ {
			timestamp := binary.BigEndian.Uint32(infoRanges[0].Data[n:])
			if timestamp != 1040 {
				t.Error("")
			}
			n += 4
			z = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
			n += 4
			data := infoRanges[0].Data[n : n+z]
			if string(data) != fmt.Sprintf("0a1040") {
				t.Error("")
			}
			n += z
		}
	}
}

func TestIsSameTimeLevel(t *testing.T) {
	if isSameTimeLevel(
		&instance{lastTime: 1040, count: 10, step: 1},
		&core.Info{Timestamp: 1040, Count: 10, Step: 1}) == false {
		t.Error("")
	}
	if isSameTimeLevel(
		&instance{lastTime: 1030, count: 10, step: 1},
		&core.Info{Timestamp: 1040, Count: 10, Step: 1}) == false {
		t.Error("")
	}
	if isSameTimeLevel(
		&instance{lastTime: 1040, count: 10, step: 1},
		&core.Info{Timestamp: 1030, Count: 10, Step: 1}) == false {
		t.Error("")
	}
}

// 跨time level查询，前一个level没有数据会直接退出，导致后一个level有数据但是查不到
func TestCacheBug1(t *testing.T) {
	var check = func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var newInfo = func(timestamp uint32) *core.Info {
		var info *core.Info = new(core.Info)
		info.Header = new(core.Header)
		info.Header.Host = string("instance")
		info.Timestamp = uint32(timestamp)
		info.Count = uint32(10)
		info.Step = uint32(1)
		info.Items = make([]*core.KVPair, 0)
		info.Items = append(info.Items, &core.KVPair{string("key"), []byte("value")})

		return info
	}

	var newQuery = func(start, end uint32) *core.Query {
		var query *core.Query = new(core.Query)
		query.Header = &core.Header{Host: string("instance")}
		query.TimeBegin = uint32(start)
		query.TimeEnd = uint32(end)

		return query
	}

	// run
	var timeBegin uint32
	var timeEnd uint32
	var err error
	var infoRanges []*core.InfoRange
	var tc *TimeCache = NewTimeCache(10, 1000)
	for i := 0; i < 100; i++ {
		check(tc.Set(newInfo(1000+uint32(i)*10)) == nil, "test")
	}
	for i := 0; i < 10; i++ {
		check(tc.Set(newInfo(2105+uint32(i)*10)) == nil, "test")
	}
	infoRanges, timeBegin, timeEnd, err = tc.Get(newQuery(2002, 3000))
	if err != nil {
		t.Error("")
	}
	if timeBegin != 2105 {
		t.Error("")
	}
	if timeEnd != 2204 {
		t.Error("")
	}
	if len(infoRanges) != 2 {
		t.Error("")
	}
	if infoRanges[0].Header.Host != "instance" {
		t.Error("")
	}
	if infoRanges[0].Count != 10 {
		t.Error("")
	}
	if infoRanges[0].Step != 1 {
		t.Error("")
	}
}

// 当数据正好少存1分钟时，timecache错误的将下一分钟数据保存为当前数据，导致数据整体偏移
func TestCacheBug2(t *testing.T) {
	var check = func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var newInfo = func(timestamp uint32) *core.Info {
		var info *core.Info = new(core.Info)
		info.Header = new(core.Header)
		info.Header.Host = string("instance")
		info.Timestamp = uint32(timestamp)
		info.Count = uint32(60)
		info.Step = uint32(1)
		info.Items = make([]*core.KVPair, 0)
		info.Items = append(info.Items, &core.KVPair{
			string("2S"), []byte(fmt.Sprintf("value:%d", timestamp)),
		})

		return info
	}

	var newQuery = func(start, end uint32) *core.Query {
		var query *core.Query = new(core.Query)
		query.Header = &core.Header{Host: string("instance")}
		query.TimeBegin = uint32(start)
		query.TimeEnd = uint32(end)

		return query
	}

	// run
	var timeBegin uint32
	var timeEnd uint32
	var err error
	var infoRanges []*core.InfoRange
	var tc *TimeCache = NewTimeCache(10, 3600)
	for i := 0; i < 15; i++ {
		check(tc.Set(newInfo(1540734936+uint32(i)*60)) == nil, "test")
	}
	for i := 0; i < 15; i++ {
		check(tc.Set(newInfo(1540735896+uint32(i)*60)) == nil, "test")
	}
	infoRanges, timeBegin, timeEnd, err = tc.Get(newQuery(1540735739, 1540736400))
	check(err == nil, "test")
	check(timeBegin == 1540735716, "test")
	check(timeEnd == 1540736435, "test")
	check(len(infoRanges) == 1, "test")

	var x, y, z int
	var n int
	var key []byte
	x = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
	n += 4
	check(x == 1, "test")
	for i := 0; i < x; i++ {
		y = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
		n += 4
		key = infoRanges[0].Data[n : n+y]
		n += y
		check(string(key) == "2S", "test")
		y = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
		n += 4
		for j := 0; j < y; j++ {
			timestamp := binary.BigEndian.Uint32(infoRanges[0].Data[n:])
			n += 4
			z = int(binary.BigEndian.Uint32(infoRanges[0].Data[n:]))
			n += 4
			data := infoRanges[0].Data[n : n+z]
			n += z
			if timestamp == 1540735836 {
				check(len(data) == 0, "test")
			} else {
				check(fmt.Sprintf("value:%d", timestamp) == string(data), "test")
			}
		}
	}
}
