package cache

import "testing"
import "math/rand"
import "fmt"

func TestDataNotExist(t *testing.T) {
	var mm *memManager = new(memManager)
	var ok bool
	var timeBegin uint32
	var timeEnd uint32
	var data [][]byte
	mm.init()
	ins := new(instance)
	ins.init("test_ins", mm, 100, 100, 10, 1)
	for i := 0; i < 5; i++ {
		ok = ins.pushBack("a", uint32(1000+i*10), []byte(fmt.Sprintf("a%d", i)))
		if !ok {
			t.Error("")
		}
	}

	// test 1: key not exist
	timeBegin, timeEnd, data = ins.queryRange("b", 500, 1500)
	if timeBegin != 0 {
		t.Error("")
	}
	if timeEnd != 0 {
		t.Error("")
	}
	if data != nil {
		t.Error("")
	}

	// test 2: time not exist
	timeBegin, timeEnd, data = ins.queryRange("a", 2000, 3000)
	if timeBegin != 0 {
		t.Error("")
	}
	if timeEnd != 0 {
		t.Error("")
	}
	if data != nil {
		t.Error("")
	}

}

func TestDataNotFull(t *testing.T) {
	var mm *memManager = new(memManager)
	var ok bool
	var timeBegin uint32
	var timeEnd uint32
	var data [][]byte
	mm.init()
	ins := new(instance)
	ins.init("test_ins", mm, 100, 100, 10, 1)
	for i := 0; i < 5; i++ {
		ok = ins.pushBack("a", uint32(1000+i*10), []byte(fmt.Sprintf("a%d", i)))
		if !ok {
			t.Error("")
		}
	}

	// test 1
	timeBegin, timeEnd, data = ins.queryRange("a", 500, 1500)
	if timeBegin != 1000 {
		t.Error("")
	}
	if timeEnd != 1049 {
		t.Error("")
	}
	if len(data) != 5 {
		t.Error("")
	}
	for i := 0; i < len(data); i++ {
		if string(data[i]) != fmt.Sprintf("a%d", i) {
			t.Error("")
		}
	}

	// test 2
	timeBegin, timeEnd, data = ins.queryRange("a", 500, 1024)
	if timeBegin != 1000 {
		t.Error("")
	}
	if timeEnd != 1029 {
		t.Error("")
	}
	if len(data) != 3 {
		t.Error("")
	}
	for i := 0; i < len(data); i++ {
		if string(data[i]) != fmt.Sprintf("a%d", i) {
			t.Error("")
		}
	}

	// test 3
	timeBegin, timeEnd, data = ins.queryRange("a", 1024, 1500)
	if timeBegin != 1020 {
		t.Error("")
	}
	if timeEnd != 1049 {
		t.Error("")
	}
	if len(data) != 3 {
		t.Error("")
	}
	for i := 0; i < len(data); i++ {
		if string(data[i]) != fmt.Sprintf("a%d", i+2) {
			t.Error("")
		}
	}

	// test 4
	timeBegin, timeEnd, data = ins.queryRange("a", 1024, 1038)
	if timeBegin != 1020 {
		t.Error("")
	}
	if timeEnd != 1039 {
		t.Error("")
	}
	if len(data) != 2 {
		t.Error("")
	}
	for i := 0; i < len(data); i++ {
		if string(data[i]) != fmt.Sprintf("a%d", i+2) {
			t.Error("")
		}
	}
}

func TestDataFull(t *testing.T) {
	var mm *memManager = new(memManager)
	var ok bool
	var timeBegin uint32
	var timeEnd uint32
	var data [][]byte
	mm.init()
	ins := new(instance)
	ins.init("test_ins", mm, 100, 100, 10, 1)
	for i := 0; i < 10; i++ {
		ok = ins.pushBack("a", uint32(1000+i*10), []byte(fmt.Sprintf("a%d", i)))
		if !ok {
			t.Error("")
		}
	}

	// test 1
	timeBegin, timeEnd, data = ins.queryRange("a", 500, 1500)
	if timeBegin != 1000 {
		t.Error("")
	}
	if timeEnd != 1099 {
		t.Error("")
	}
	if len(data) != 10 {
		t.Error("")
	}
	for i := 0; i < len(data); i++ {
		if string(data[i]) != fmt.Sprintf("a%d", i) {
			t.Error("")
		}
	}

	// test 2
	timeBegin, timeEnd, data = ins.queryRange("a", 500, 1048)
	if timeBegin != 1000 {
		t.Error("")
	}
	if timeEnd != 1049 {
		t.Error("")
	}
	if len(data) != 5 {
		t.Error("")
	}
	for i := 0; i < len(data); i++ {
		if string(data[i]) != fmt.Sprintf("a%d", i) {
			t.Error("")
		}
	}

	// test 3
	timeBegin, timeEnd, data = ins.queryRange("a", 1024, 1500)
	if timeBegin != 1020 {
		t.Error("")
	}
	if timeEnd != 1099 {
		t.Error("")
	}
	if len(data) != 8 {
		t.Error("")
	}
	for i := 0; i < len(data); i++ {
		if string(data[i]) != fmt.Sprintf("a%d", i+2) {
			t.Error("")
		}
	}

	// test 4
	timeBegin, timeEnd, data = ins.queryRange("a", 1038, 1062)
	if timeBegin != 1030 {
		t.Error("")
	}
	if timeEnd != 1069 {
		t.Error("")
	}
	if len(data) != 4 {
		t.Error("")
	}
	for i := 0; i < len(data); i++ {
		if string(data[i]) != fmt.Sprintf("a%d", i+3) {
			t.Error("")
		}
	}
}

func TestDataOutOfFull(t *testing.T) {
	var mm *memManager = new(memManager)
	var ok bool
	var timeBegin uint32
	var timeEnd uint32
	var data [][]byte
	mm.init()
	ins := new(instance)
	ins.init("test_ins", mm, 100, 100, 10, 1)
	for i := 0; i < 13; i++ {
		ok = ins.pushBack("a", uint32(1000+i*10), []byte(fmt.Sprintf("a%d", i)))
		if !ok {
			t.Error("")
		}
	}

	// test 1
	timeBegin, timeEnd, data = ins.queryRange("a", 500, 1500)
	if timeBegin != 1030 {
		t.Error("")
	}
	if timeEnd != 1129 {
		t.Error("")
	}
	if len(data) != 10 {
		t.Error("")
	}
	for i := 0; i < len(data); i++ {
		if string(data[i]) != fmt.Sprintf("a%d", i+3) {
			t.Error("")
		}
	}

	// test 2
	timeBegin, timeEnd, data = ins.queryRange("a", 500, 1048)
	if timeBegin != 1030 {
		t.Error("")
	}
	if timeEnd != 1049 {
		t.Error("")
	}
	if len(data) != 2 {
		t.Error("")
	}
	for i := 0; i < len(data); i++ {
		if string(data[i]) != fmt.Sprintf("a%d", i+3) {
			t.Error("")
		}
	}

	// test 3
	timeBegin, timeEnd, data = ins.queryRange("a", 1064, 1500)
	if timeBegin != 1060 {
		t.Error("")
	}
	if timeEnd != 1129 {
		t.Error("")
	}
	if len(data) != 7 {
		t.Error("")
	}
	for i := 0; i < len(data); i++ {
		if string(data[i]) != fmt.Sprintf("a%d", i+6) {
			t.Error("")
		}
	}

	// test 4
	timeBegin, timeEnd, data = ins.queryRange("a", 1038, 1062)
	if timeBegin != 1030 {
		t.Error("")
	}
	if timeEnd != 1069 {
		t.Error("")
	}
	if len(data) != 4 {
		t.Error("")
	}
	for i := 0; i < len(data); i++ {
		if string(data[i]) != fmt.Sprintf("a%d", i+3) {
			t.Error("")
		}
	}
}

func TestTwoDiffItem(t *testing.T) {
	var mm *memManager = new(memManager)
	var ok bool
	var timeBegin uint32
	var timeEnd uint32
	var data [][]byte
	mm.init()
	ins := new(instance)
	ins.init("test_ins", mm, 100, 100, 10, 1)
	for i := 0; i < 5; i++ {
		ok = ins.pushBack("a", uint32(1000+i*10), []byte(fmt.Sprintf("a%d", i)))
		if !ok {
			t.Error("")
		}
	}
	for i := 0; i < 15; i++ {
		ok = ins.pushBack("b", uint32(1000+i*10), []byte(fmt.Sprintf("b%d", i)))
		if !ok {
			t.Error("")
		}
	}
	timeBegin, timeEnd, data = ins.queryRange("a", 500, 1500)
	if timeBegin != 1000 {
		t.Error("")
	}
	if timeEnd != 1049 {
		t.Error("")
	}
	if len(data) != 5 {
		t.Error("")
	}
	for i := 0; i < len(data); i++ {
		if string(data[i]) != fmt.Sprintf("a%d", i) {
			t.Error("")
		}
	}
	timeBegin, timeEnd, data = ins.queryRange("b", 500, 1500)
	if timeBegin != 1050 {
		t.Error("")
	}
	if timeEnd != 1149 {
		t.Error("")
	}
	if len(data) != 10 {
		t.Error("")
	}
	for i := 0; i < len(data); i++ {
		if string(data[i]) != fmt.Sprintf("b%d", i+5) {
			t.Error("")
		}
	}
}

func TestDataCavity(t *testing.T) {
	var mm *memManager = new(memManager)
	var ok bool
	var timeBegin uint32
	var timeEnd uint32
	var data [][]byte
	mm.init()
	ins := new(instance)
	ins.init("test_ins", mm, 100, 100, 10, 1)
	for i := 0; i < 5; i++ {
		ok = ins.pushBack("a", uint32(1000+i*10), []byte(fmt.Sprintf("a%d", i)))
		if !ok {
			t.Error("")
		}
	}
	ok = ins.pushBack("a", uint32(1000+80), []byte("a8"))
	if !ok {
		t.Error("")
	}
	ok = ins.pushBack("a", uint32(1000+90), []byte("a9"))
	if !ok {
		t.Error("")
	}
	timeBegin, timeEnd, data = ins.queryRange("a", 500, 1500)
	if timeBegin != 1000 {
		t.Error("")
	}
	if timeEnd != 1099 {
		t.Error("")
	}

	if len(data) != 10 {
		t.Error("")
	}
	for i := 0; i < 5; i++ {
		if string(data[i]) != fmt.Sprintf("a%d", i) {
			t.Error("")
		}
	}
	if len(data[5]) != 0 {
		t.Error("")
	}
	if len(data[6]) != 0 {
		t.Error("")
	}
	if len(data[7]) != 0 {
		t.Error("")
	}
	if string(data[8]) != "a8" {
		t.Error("")
	}
	if string(data[9]) != "a9" {
		t.Error("")
	}

	ok = ins.pushBack("a", uint32(1000+130), []byte("a13"))
	if !ok {
		t.Error("")
	}
	timeBegin, timeEnd, data = ins.queryRange("a", 500, 1500)
	if timeBegin != 1040 {
		t.Error("")
	}
	if timeEnd != 1139 {
		t.Error("")
	}

	if len(data) != 10 {
		t.Error("")
	}
	if string(data[0]) != "a4" {
		t.Error("")
	}
	if string(data[4]) != "a8" {
		t.Error("")
	}
	if string(data[5]) != "a9" {
		t.Error("")
	}
	if string(data[9]) != "a13" {
		t.Error("")
	}

	// insert in middle
	ok = ins.pushBack("a", uint32(1000+70), []byte("a7"))
	if !ok {
		t.Error("")
	}

	timeBegin, timeEnd, data = ins.queryRange("a", 500, 1500)
	if timeBegin != 1040 {
		t.Error("")
	}
	if timeEnd != 1139 {
		t.Error("")
	}

	if len(data) != 10 {
		t.Error("")
	}
	if string(data[0]) != "a4" {
		t.Error("")
	}
	if string(data[3]) != "a7" {
		t.Error("")
	}
	if string(data[4]) != "a8" {
		t.Error("")
	}
	if string(data[5]) != "a9" {
		t.Error("")
	}
	if string(data[9]) != "a13" {
		t.Error("")
	}

}

func TestAutoClean(t *testing.T) {
	var mm *memManager = new(memManager)
	var ok bool
	var timeBegin uint32
	var timeEnd uint32
	var data [][]byte
	mm.init()
	ins := new(instance)
	ins.init("test_ins", mm, 100, 100, 10, 1)
	for i := 0; i < 13; i++ {
		ok = ins.pushBack("a", uint32(1000+i*10), []byte(fmt.Sprintf("a%d", i)))
		if !ok {
			t.Error("")
		}
	}

	timeBegin, timeEnd, data = ins.queryRange("a", 500, 1500)
	if timeBegin != 1030 {
		t.Error("")
	}
	if timeEnd != 1129 {
		t.Error("")
	}
	if len(data) != 10 {
		t.Error("")
	}
	for i := 0; i < len(data); i++ {
		if string(data[i]) != fmt.Sprintf("a%d", i+3) {
			t.Error("")
		}
	}

	// don't remove if data not old enough
	ins.autoClean("a", 1129)
	timeBegin, timeEnd, data = ins.queryRange("a", 500, 1500)
	if timeBegin != 1030 {
		t.Error("")
	}
	if timeEnd != 1129 {
		t.Error("")
	}
	if len(data) != 10 {
		t.Error("")
	}
	for i := 0; i < len(data); i++ {
		if string(data[i]) != fmt.Sprintf("a%d", i+3) {
			t.Error("")
		}
	}

	// clean older data
	ins.autoClean("a", 1130)
	timeBegin, timeEnd, data = ins.queryRange("a", 500, 1500)
	if timeBegin != 1040 {
		t.Error("")
	}
	if timeEnd != 1129 {
		t.Error("")
	}
	if len(data) != 9 {
		t.Error("")
	}
	for i := 0; i < len(data); i++ {
		if string(data[i]) != fmt.Sprintf("a%d", i+4) {
			t.Error("")
		}
	}
}

func TestLastTime(t *testing.T) {
	var mm *memManager = new(memManager)
	var ok bool
	mm.init()
	ins := new(instance)
	ins.init("test_ins", mm, 100, 100, 10, 1)
	for i := 0; i < 13; i++ {
		ok = ins.pushBack("a", uint32(1000+i*10), []byte(fmt.Sprintf("a%d", i)))
		if !ok {
			t.Error("")
		}
		if ins.lastTime != uint32(1000+i*10) {
			t.Error("")
		}
	}
}

func TestSkipPushBackForTimeCacheBug2(t *testing.T) {
	var mm *memManager = new(memManager)
	mm.init()
	ins := new(instance)
	ins.init("test_ins", mm, 1000, 100, 10, 1)
	ins.pushBack("a", uint32(1000), []byte("1000"))
	ins.pushBack("a", uint32(1020), []byte("1020"))
	ins.queryRange("a", 1000, 1020)
	var timeBegin, timeEnd, data = ins.queryRange("a", 1000, 1020)
	if timeBegin != 1000 {
		t.Error("")
	}
	if timeEnd != 1029 {
		t.Error("")
	}
	if string(data[0]) != "1000" {
		t.Error("")
	}
	if data[1] != nil {
		t.Error("")
	}
	if string(data[2]) != "1020" {
		t.Error("")
	}
}

func TestQueryEmpty(t *testing.T) {
	var mm *memManager = new(memManager)
	mm.init()
	ins := new(instance)
	ins.init("test_ins", mm, 1000, 100, 10, 1)
	ins.pushBack("a", uint32(1000), []byte("1000"))
	ins.pushBack("a", uint32(1010), []byte("1010"))
	var timeBegin, timeEnd, data = ins.queryRange("a", 1020, 1029)
	if timeBegin != 0 {
		t.Error("")
	}
	if timeEnd != 0 {
		t.Error("")
	}
	if data != nil {
		t.Error("")
	}
}

func BenchmarkPushBack(b *testing.B) {
	var mm *memManager = new(memManager)
	var ok bool
	mm.init()
	ins := new(instance)
	ins.init("test_ins", mm, 1000, 86400, 60, 1)

	var keyList []string = make([]string, 0)
	for j := 0; j < 700; j++ {
		keyList = append(keyList, fmt.Sprintf("%d", j))
	}

	for i := 0; i < b.N; i++ {
		for j := 0; j < 700; j++ {
			ok = ins.pushBack(keyList[j], uint32(1000+i*60), make([]byte, 1024, 1024))
			if !ok {
				b.Error("")
			}
		}
	}
}

func BenchmarkQueryRange(b *testing.B) {
	var mm *memManager = new(memManager)
	var ok bool
	mm.init()
	ins := new(instance)
	ins.init("test_ins", mm, 1000, 86400, 60, 1)
	for i := 0; i < b.N; i++ {
		ok = ins.pushBack("a", uint32(1000+i*60), make([]byte, 1024, 1024))
		if !ok {
			b.Error("")
		}
	}

	base := 1000 + 60*b.N - 86400

	for i := 0; i < b.N; i++ {
		s := rand.NewSource(int64(i))
		r := rand.New(s)
		left := base + r.Intn(43200)
		right := left + r.Intn(43200)
		ins.queryRange("a", uint32(left), uint32(right))
		if !ok {
			b.Error("")
		}
	}
}
