/*
// =====================================================================================
//
//       Filename:  handler_test.go
//
//    Description:
//
//        Version:  1.0
//        Created:  10/10/2018 07:21:20 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package handler

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"inspector/compress"
	"inspector/util"
	"runtime"
	"strconv"
	"testing"
)

func TestParsePanelQuery(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var h *ApiHandler = new(ApiHandler)

	// case 1
	{
		var panelQuery = "mongo|c1|c2|c3|cpu,mongo|m1|m2|m3|memory;mongo|i1|i2|i3|io [opExpression[]] {hostId=dds-2ze2ae2045feb3e4{hid=4931065}, host=11.218.80.161:3079-P; host2=-p}"
		metrics, opExpression, instances := h.parsePanelQuery(panelQuery)
		check(metrics == "mongo|c1|c2|c3|cpu,mongo|m1|m2|m3|memory;mongo|i1|i2|i3|io", "test")
		check(opExpression == "opExpression[]", "test")
		check(instances == "hostId=dds-2ze2ae2045feb3e4{hid=4931065}, host=11.218.80.161:3079-P; host2=-p", "test")
	}

	// case 2
	{
		var panelQuery = "mongo|c1|c2|c3|cpu [] {hostId=hid}"
		metrics, opExpression, instances := h.parsePanelQuery(panelQuery)
		check(metrics == "mongo|c1|c2|c3|cpu", "test")
		check(opExpression == "", "test")
		check(instances == "hostId=hid", "test")
	}

	// case 3
	{
		var panelQuery = "mongo|c1|c2|c3|cpu {hostId=hid}"
		metrics, opExpression, instances := h.parsePanelQuery(panelQuery)
		check(metrics == "mongo|c1|c2|c3|cpu", "test")
		check(opExpression == "", "test")
		check(instances == "hostId=hid", "test")
	}

	// case 4
	{
		var panelQuery = "mongo|c1|c2|c3|cpu,mongo|m1|m2|m3|memory;mongo|i1|i2|i3|io [opExpression[]] "
		metrics, opExpression, instances := h.parsePanelQuery(panelQuery)
		check(metrics == "mongo|c1|c2|c3|cpu,mongo|m1|m2|m3|memory;mongo|i1|i2|i3|io", "test")
		check(opExpression == "opExpression[]", "test")
		check(instances == "", "test")
	}

	// case 5
	{
		var panelQuery = "mongo|c1|c2|c3|cpu,mongo|m1|m2|m3|memory;mongo|i1|i2|i3|io"
		metrics, opExpression, instances := h.parsePanelQuery(panelQuery)
		check(metrics == "mongo|c1|c2|c3|cpu,mongo|m1|m2|m3|memory;mongo|i1|i2|i3|io", "test")
		check(opExpression == "", "test")
		check(instances == "", "test")
	}

	check(true, "test")
}

func TestParseMetrics(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var h *ApiHandler = new(ApiHandler)

	// case 1
	{
		var metricsQuery = "mongo|c1|c2|c3|cpu,mongo|m1|m2|m3|memory;mongo|i1|i2|i3|io&mongo|n1|n2|n3|net"
		var metricList = h.parseMetrics(metricsQuery)
		check(len(metricList) == 4, "test")
		check(metricList[0] == "mongo|c1|c2|c3|cpu", "test")
		check(metricList[1] == "mongo|m1|m2|m3|memory", "test")
		check(metricList[2] == "mongo|i1|i2|i3|io", "test")
		check(metricList[3] == "mongo|n1|n2|n3|net", "test")
	}

	// case 2
	{
		var metricsQuery = "mongo|c1|c2|c3|one&;,"
		var metricList = h.parseMetrics(metricsQuery)
		check(len(metricList) == 1, "test")
		check(metricList[0] == "mongo|c1|c2|c3|one", "test")
	}

	// case 3
	{
		var metricsQuery = "mongo|c1|c2|c3|one;mongo|c1|c2|c3|two;"
		var metricList = h.parseMetrics(metricsQuery)
		check(len(metricList) == 2, "test")
		check(metricList[0] == "mongo|c1|c2|c3|one", "test")
		check(metricList[1] == "mongo|c1|c2|c3|two", "test")
	}

	check(true, "test")
}

func TestParseService(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var h *ApiHandler = new(ApiHandler)

	// case 1
	{
		var metric = "mongo|c1|c2|c3|cpu"
		var service = h.parseService(metric)
		check(service == "mongo", "test")
	}

	check(true, "test")
}

func TestParseRealMetric(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var h *ApiHandler = new(ApiHandler)

	// case 1
	{
		var metric = "mongo|c1|c2|c3|cpu"
		var service = h.parseRealMetric(metric)
		check(service == "c1|c2|c3|cpu", "test")
	}

	check(true, "test")
}

func TestParsePureMetric(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var h *ApiHandler = new(ApiHandler)

	// case 1
	{
		var metric = "mongo|c1|c2|c3|cpu"
		var pureMetric = h.parsePureMetric(metric)
		check(pureMetric == "c1|c2|c3|cpu", "test")
	}

	check(true, "test")
}

func TestParseMetricsReg(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var h *ApiHandler = new(ApiHandler)

	var matricList = []string{
		"metrics|commands|update|total",
		"memory|update|total",
	}

	// case 1
	{
		var reg = "metrics\\|commands\\|.*\\|total"
		var metricList = h.parseMetricsReg(reg, matricList)
		check(len(metricList) == 1, "test")
		check(metricList[0] == "metrics|commands|update|total", "test")
	}

	check(true, "test")
}

func TestParseInstanceSelector(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var h *ApiHandler = new(ApiHandler)

	// case 0
	{
		var instance = "hostId=dds-2ze2ae2045feb3e4{id=4931065}, host=11.218.80.161:3079-P; host2=-p"
		var insMap = h.parseInstanceSelector(instance)
		check(insMap["instanceName"] == "dds-2ze2ae2045feb3e4", "test")
		check(insMap["instanceId"] == "4931065", "test")
		check(insMap["hid"] == "4931065", "test")
		check(insMap["host"] == "11.218.80.161:3079", "test")
		check(insMap["subHost"] == "", "test")
	}

	// case 1
	{
		var instance = "hostId=dds-2ze2ae2045feb3e4{hid=4931065}, host=11.218.80.161:3079-P; host2=-p"
		var insMap = h.parseInstanceSelector(instance)
		check(insMap["instanceName"] == "dds-2ze2ae2045feb3e4", "test")
		check(insMap["instanceId"] == "4931065", "test")
		check(insMap["hid"] == "4931065", "test")
		check(insMap["host"] == "11.218.80.161:3079", "test")
		check(insMap["subHost"] == "", "test")
	}

	// case 2
	{
		var instance = "hostId=dds-2ze2ae2045feb3e4{hid=4931065, pid=46734}, host=11.218.80.161:3079-P; host2=-p"
		var insMap = h.parseInstanceSelector(instance)
		check(insMap["instanceName"] == "dds-2ze2ae2045feb3e4", "test")
		check(insMap["instanceId"] == "4931065", "test")
		check(insMap["hid"] == "4931065", "test")
		check(insMap["pid"] == "46734", "test")
		check(insMap["host"] == "11.218.80.161:3079", "test")
		check(insMap["subHost"] == "", "test")
	}

	check(true, "test")
}

func TestInfoRangeList2dataMap(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	check(true, "test")
}

func TestData2Map(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var h *ApiHandler = new(ApiHandler)

	type valueItem struct {
		timestamp uint32
		value     []byte
	}

	var mockValueMap = make(map[string][]valueItem)
	var n = 10
	for i := 0; i < n; i++ {
		var x = i + 1
		var it = make([]valueItem, x)
		mockValueMap[fmt.Sprintf("key:%d", x)] = it
		for j := 0; j < x; j++ {
			it[j].timestamp = uint32(1000 + j*60)
			var value = make([]int64, 60)
			value = value[0:0]
			for k := 0; k < 60; k++ {
				value = append(value, int64(x*1000000+int(it[j].timestamp)+k))
			}
			data, err := compress.Compress(compress.DiffCompress, int64(1), value)
			check(err == nil, "test")
			it[j].value = data
		}
	}

	// write to infoRange
	bytesBuffer := bytes.NewBuffer([]byte{})
	// write key list size
	binary.Write(bytesBuffer, binary.BigEndian, uint32(len(mockValueMap)))
	for k, v := range mockValueMap {
		// write key size
		binary.Write(bytesBuffer, binary.BigEndian, uint32(len(k)))
		// write key
		bytesBuffer.WriteString(k)
		// write data size
		binary.Write(bytesBuffer, binary.BigEndian, uint32(len(v)))
		for _, it := range v {
			binary.Write(bytesBuffer, binary.BigEndian, it.timestamp)
			binary.Write(bytesBuffer, binary.BigEndian, uint32(len(it.value)))
			bytesBuffer.Write(it.value)
		}
	}
	var infoRangeData = bytesBuffer.Bytes()

	// case 1
	{
		var dataMap = h.data2map(infoRangeData, 1050, 1550, 60)
		var count int

		for k, v := range dataMap {
			count, _ = strconv.Atoi(k[4:])
			var i = 0
			for ; i < 10+(count-1)*60; i++ {
				if i < len(v) {
					check(v[i] == int64(count*1000000+1050+i), "test")
				}

			}
			for ; i < 500; i++ {
				if i < len(v) {
					check(v[i] == util.NullData, "test")
				}
			}
		}
	}

	// case 2
	{
		var count int
		var dataMap = h.data2map(infoRangeData, 500, 1550, 60)
		for k, v := range dataMap {
			count, _ = strconv.Atoi(k[4:])
			var i = 0
			for ; i < 500; i++ {
				if i < len(v) {
					check(v[i] == util.NullData, "test")
				}

			}
			for ; i < (count)*60+500; i++ {
				if i < len(v) {
					check(v[i] == int64(count*1000000+1000+i-500), "test")
				}

			}
			for ; i < 1000; i++ {
				if i < len(v) {
					check(v[i] == util.NullData, "test")
				}
			}
		}
	}

	// case 3
	{
		var count int
		var dataMap = h.data2map(infoRangeData, 1050, 2050, 60)
		for k, v := range dataMap {
			count, _ = strconv.Atoi(k[4:])
			var i = 0
			for ; i < 10+(count-1)*60; i++ {
				if i < len(v) {
					check(v[i] == int64(count*1000000+1050+i), "test")
				}

			}
			for ; i < 1000; i++ {
				if i < len(v) {
					check(v[i] == util.NullData, "test")
				}
			}
		}
	}

	check(true, "test")
}

func TestMergeDataMap(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var h *ApiHandler = new(ApiHandler)

	arrayEqual := func(x, y []int64) bool {
		if x == nil || y == nil {
			if x == nil && y == nil {
				return true
			} else {
				return false
			}
		}
		if len(x) != len(y) {
			return false
		}
		for i := 0; i < len(x); i++ {
			if x[i] != y[i] {
				return false
			}
		}
		return true
	}

	var xMap map[string][]int64
	var yMap map[string][]int64
	var result map[string][]int64

	// case 0
	{
		var ok bool
		var list []int64

		xMap = nil
		yMap = map[string][]int64{
			"k1": []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		}
		result = h.mergeDataMap(xMap, yMap)
		check(len(result) == 1, "test")
		list, ok = result["k1"]
		check(ok, "test")
		check(arrayEqual(list, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), "test")

		xMap = map[string][]int64{
			"k1": []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		}
		yMap = nil
		result = h.mergeDataMap(xMap, yMap)
		check(len(result) == 1, "test")
		list, ok = result["k1"]
		check(ok, "test")
		check(arrayEqual(list, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), "test")
	}

	// case 1
	{
		xMap = map[string][]int64{
			"k1": []int64{1, 2, 3, 4, 5, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData},
		}
		yMap = map[string][]int64{
			"k1": []int64{11, 12, 13, 14, 15, 6, 7, 8, 9, 10},
		}
		result = h.mergeDataMap(xMap, yMap)
		check(arrayEqual(result["k1"], []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), "test")
		check(arrayEqual(xMap["k1"], []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), "test")

	}

	// case 2
	{
		xMap = map[string][]int64{
			"k1": []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			"k2": []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		}
		yMap = map[string][]int64{
			"k1": []int64{11, 12, 13, 14, 15, 6, 7, 8, 9, 10},
			"k3": []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		}
		result = h.mergeDataMap(xMap, yMap)
		check(len(result) == 3, "test")
		check(arrayEqual(result["k1"], []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), "test")
		check(arrayEqual(result["k2"], []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), "test")
		check(arrayEqual(result["k3"], []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), "test")
	}

	// case 3
	{
		xMap = map[string][]int64{
			"k1": []int64{},
			"k2": []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		}
		yMap = map[string][]int64{
			"k1": []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			"k2": []int64{},
		}
		result = h.mergeDataMap(xMap, yMap)
		check(len(result) == 2, "test")
		check(arrayEqual(result["k1"], []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), "test")
		check(arrayEqual(result["k2"], []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), "test")
	}

	check(true, "test")
}

func TestPerf(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var h *ApiHandler = new(ApiHandler)

	h.timeReset()
	for i := 0; i < 10; i++ {
		h.timeTick(fmt.Sprintf("k%d", i))
	}
	var durationAll, durationList = h.getTimeConsumeResult()
	fmt.Printf("perf time all consume: %v\n", durationAll)
	for _, it := range durationList {
		fmt.Printf("perf time each consume: name[%v], step[%v], duration[%v]\n",
			it.name, it.step, it.duration)
	}

	check(true, "test")
}
