/*
// =====================================================================================
//
//       Filename:  queryRangeHandler.go
//
//    Description:  范围查询处理函数
//
//        Version:  1.0
//        Created:  08/16/2018 05:41:09 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package handler

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"inspector/api_server/configure"
	"inspector/api_server/filter"
	"inspector/api_server/syntax"
	"inspector/compress"
	"inspector/dict_server"
	"inspector/heartbeat"
	"inspector/proto/collector"
	"inspector/proto/core"
	"inspector/proto/store"
	"inspector/util"
	"inspector/util/unsafe"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"google.golang.org/grpc"
)

/*
// =====================================================================================
// grafana data model
// =====================================================================================
*/
type PrometheusQueryRangeResult struct {
	Metric map[string]string `json:"metric"`
	Values [][2]float64      `json:"values"`
}
type PrometheusQueryRangeData struct {
	ResultType string                       `json:"resultType"` // matrix
	Result     []PrometheusQueryRangeResult `json:"result"`
}
type PrometheusQueryRangeModel struct {
	Status string                   `json:"status"` // success or failure
	Data   PrometheusQueryRangeData `json:"data"`
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  QueryRangeHandler
 *  Description:
 * =====================================================================================
 */
func (h *ApiHandler) QueryRangeHandler(w http.ResponseWriter, r *http.Request) {
	glog.V(1).Infof("[Trace][QueryRangeHandler] called: Request[%v]", r)
	h.timeReset()

	var params map[string][]string
	var err error

	var metrics string
	var metricList []string
	var opExpression string
	var instances string
	var instanceSelector map[string]string

	// parse request
	params, err = url.ParseQuery(r.URL.RawQuery)
	glog.V(3).Infof("[Debug][QueryRangeHandler] parse http query: params[%v]", params)
	// fmt.Printf("debug QueryRangeHandler: Request[%v]\n", params)
	if err != nil {
		var errStr = fmt.Sprintf("query_range parse query error: %s", err.Error())
		glog.Warning(errStr)
		fmt.Fprintln(w, errStr)
	}
	var query = params["query"][0]
	var start = params["start"][0]
	var end = params["end"][0]
	var step = params["step"][0]
	var tmpNum int
	tmpNum, _ = strconv.Atoi(start)
	var startTime = uint32(tmpNum)
	tmpNum, _ = strconv.Atoi(end)
	var endTime = uint32(tmpNum)
	tmpNum, _ = strconv.Atoi(step)
	var showStep = tmpNum

	h.timeTick("parse request")

	// parse query
	var service string
	metrics, opExpression, instances = h.parsePanelQuery(query)
	if metrics[0:4] == "reg(" {
		// 正则表达式

		metrics = metrics[4 : len(metrics)-1] // 去除"reg("和最后的")"
		service = h.parseService(metrics)
		var serviceEndIndex = strings.IndexByte(metrics, '|')
		metrics = metrics[serviceEndIndex+1:] // 去除首部"service|"字符
		var dict interface{}
		var ok bool
		if dict, ok = configure.Options.DictServerMap.Load(service); !ok {
			var errStr = fmt.Sprintf("can't find DictServer[%v]", service)
			glog.Errorf(errStr)
			fmt.Fprintln(w, errStr)
		}
		if metricList, err = dict.(*dictServer.DictServer).GetKeyList(); err != nil {
			var errStr = fmt.Sprintf("can't get dict key list from DictServer[%v]", service)
			glog.Errorf(errStr)
			fmt.Fprintln(w, errStr)
		}
		metricList = h.parseMetricsReg(metrics, metricList)
	} else {
		// 监控项列表
		metricList = h.parseMetrics(metrics)
		service = h.parseService(metricList[0])
		// cut service
		for i, it := range metricList {
			metricList[i] = h.parseRealMetric(it)
		}

	}

	instanceSelector = h.parseInstanceSelector(instances)
	glog.V(3).Infof("[Debug][QueryRangeHandler] parse metric query: "+
		"metrics[%v], metricList[%v], instanceSelector[%v]",
		metrics, metricList, instanceSelector)

	h.timeTick("parse query")

	var filterIndexResult [][]int
	var filterDataResult [][]int64
	startTime, filterIndexResult, filterDataResult = h.doQueryRange(service, metricList, opExpression, instanceSelector, startTime, endTime, showStep)
	if filterDataResult == nil {
		glog.Errorf("query data error: service[%v], metricList[%v], opExpression[%v], instanceSelector[%v], startTime[%v], endTime[%v]",
			service, metricList, opExpression, instanceSelector, startTime, endTime)
		return
	}
	// fmt.Println("debug filterDataResult: ", filterIndexResult, filterDataResult)

	h.timeTick("doQueryRange")

	// compose final result(http data)
	// 如果有过数组计算，则需要调整metricList，从而将数值与命名对应
	// 目前只支持原地计算和数组多和一运算
	// 原地计算结果保持与输入nameList相同的顺序，多和一运算返回运算过程
	var nameList []string
	if len(filterDataResult) == len(metricList) {
		nameList = metricList
	} else {
		nameList = append(nameList, opExpression)
	}
	var dataStep uint32
	var tmp int
	tmp, err = configure.Options.ConfigServer.GetInt(util.MetaCollection, service, "interval")
	dataStep = uint32(tmp)

	var finalResult = h.array2json(nameList, startTime, dataStep, filterIndexResult, filterDataResult)
	// fmt.Println("debug finalResult: ", len(finalResult), finalResult)

	// print perf info
	h.timeTick("array2json")
	if glog.V(2) {
		bytesBuffer := bytes.NewBuffer([]byte{})
		var durationAll, durationList = h.getTimeConsumeResult()

		bytesBuffer.WriteString("[Perf][QueryRangeHandler]: ")
		for _, it := range durationList {
			bytesBuffer.WriteString(
				fmt.Sprintf("step[%v](%v) time duration[%v]|",
					it.name, it.step, it.duration))
		}
		bytesBuffer.WriteString(fmt.Sprintf("all time duration[%v]\n", durationAll))
		glog.Infof(bytesBuffer.String())
	}
	fmt.Fprintln(w, finalResult)
	glog.Flush()
	return

	// mock
	ShowContext(r)
	var result = FakeResult()
	fmt.Printf("qurey range result: %s\n", result)
	fmt.Fprintln(w, result)
	return
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  doQueryRange
 *  Description:  返回值中uint32为虚拟时间，需要乘以step变为真实时间
 * =====================================================================================
 */
func (h *ApiHandler) doQueryRange(service string,
	metricList []string,
	opExpression string,
	instanceSelector map[string]string,
	startTime, endTime uint32, showStep int) (uint32, [][]int, [][]int64) {
	glog.V(1).Infof("[Trace][doQueryRange] called: service[%v], metricList[%v], opExpression[%v], instanceSelector[%v], startTime[%v], endTime[%v], showStep[%v]",
		service, metricList, opExpression, instanceSelector, startTime, endTime, showStep)

	var count, step int
	var err error
	count, err = configure.Options.ConfigServer.GetInt(util.MetaCollection, service, "count")
	step, err = configure.Options.ConfigServer.GetInt(util.MetaCollection, service, "interval")

	startTime /= uint32(step) // 针对大于1s采集的数据，通过虚拟时间变为1s采集
	endTime /= uint32(step)   // 针对大于1s采集的数据，通过虚拟时间变为1s采集
	showStep /= step          // 针对大于1s采集的数据进行处理，通过虚拟show step将数据变为1s采集

	var innerTimer = new(ApiHandler)
	innerTimer.timeReset()

	var hid int
	var pid int
	var hidStr = instanceSelector["hid"]
	var pidStr = instanceSelector["pid"]
	var host = instanceSelector["host"]
	var keyList = make([]string, len(metricList))
	hid, err = strconv.Atoi(hidStr)
	if err != nil {
		glog.Errorf("hid[%s] is not a number", hidStr)
		return 0, nil, nil
	}
	pid, err = strconv.Atoi(pidStr)
	if err != nil {
		// pid为parient_id，不写默认为0
		glog.Errorf("pid[%s] is not a number", pidStr)
		pid = 0
	}

	if keyList, err = h.metricList2keyList(service, metricList, keyList); err != nil {
		glog.Error(err)
		return 0, nil, nil
	}
	glog.V(3).Infof("[Debug][doQueryRange] metricList2keyList: keyList[%s]", keyList)

	innerTimer.timeTick("convert key list")

	// 先将startTime向前偏移一个showStep-1的位置，这是为了后面filter时的数据对齐考虑
	startTime = startTime - uint32(showStep) + 1
	// get data from collector
	var collectorInfoRangeList = h.getFromCollector(service, uint32(pid), int32(hid), host, keyList, startTime, endTime)
	innerTimer.timeTick("getFromCollector")
	var collectorData = h.infoRangeList2dataMap(collectorInfoRangeList, startTime, endTime)
	innerTimer.timeTick("parserCollectorData")
	// fmt.Println("debug collectorData: ", len(collectorData), collectorData)

	// get data from store
	// startTime向前多取1个存储单元，这是因为db的存储如果想取到指定时间的数据，就得用这个时间段的起始时间
	var storeInfoRangeList = h.getFromStore(service, uint32(pid), int32(hid), host, keyList, startTime-uint32(count), endTime)
	innerTimer.timeTick("getFromStore")
	var storeData = h.infoRangeList2dataMap(storeInfoRangeList, startTime, endTime)
	innerTimer.timeTick("parserStoreData")
	// fmt.Println("debug storeData: ", len(storeData), storeData)

	if len(storeData) == 0 && len(collectorData) == 0 {
		glog.Error("query data is not exist")
		return 0, nil, nil
	}

	// merge store and collector data
	var mergedData = h.mergeDataMap(storeData, collectorData)
	innerTimer.timeTick("mergeDataMap")
	// fmt.Println("debug mergedData: ", len(mergedData), mergedData)

	// cauculate
	var dataList [][]int64
	for _, it := range keyList {
		dataList = append(dataList, mergedData[it])
	}
	// fmt.Println("debug dataList: ", len(dataList), dataList)
	var calculateResule [][]int64
	if len(opExpression) > 0 {
		glog.V(3).Infof("[Debug][doQueryRange] do calculate")
		// 目前数组计算只支持多个数组变成1个数组的模式，随着后续功能扩展，也可以支持返回矩阵
		if calculateResule, err = syntax.ArrayCalculation(opExpression, dataList...); err != nil {
			glog.Errorf("calculate data service[%s] hid[%s] host[%s] keyList[%v] error: %s",
				service, hid, host, metricList, err.Error())
		}
	} else {
		glog.V(3).Infof("[Debug][doQueryRange] no calculate")
		calculateResule = dataList
	}
	innerTimer.timeTick("calculate")
	// fmt.Println("debug calculateResule: ", len(calculateResule), calculateResule)

	// filter
	var filterIndexResult [][]int
	var filterDataResult [][]int64
	// 将时间和show step变为虚拟时间和虚拟step，返回数据需要将时间恢复变为实际值
	if showStep > 1 {
		// do filter
		glog.V(3).Infof("[Debug][doQueryRange] do filter[%v]", instanceSelector["filter"])
		var filterFunc func([]int64, []int64, []int) error
		switch instanceSelector["filter"] {
		case "fix":
			filterFunc = filter.FixedPointSamplingFilter
		case "peak":
			filterFunc = filter.PeakSamplingFilterAvg
		default:
			filterFunc = filter.FixedPointSamplingFilter
		}
		var align = int(startTime % uint32(showStep))
		if align != 0 {
			align = showStep - align
		}
		startTime += uint32(align)
		for i, it := range calculateResule {
			// 由于Filter都是按照高精度浮点型时间进行step累加的，所以有时会造成小数部分累计
			// 从而引起整数部分没有按照step对齐的情况，这样将导致grafana显示出现断点
			// 所以在filter之前，需要将数据进行对齐截断，丢弃最后不足一个step的数据
			var filterLen = len(it) / showStep
			var inputLen = len(it)
			inputLen -= align
			inputLen -= inputLen % showStep
			// fmt.Println("debug filter align: ", startTime, align, len(it), inputLen, filterLen)

			filterIndexResult = append(filterIndexResult, make([]int, filterLen))
			filterDataResult = append(filterDataResult, make([]int64, filterLen))
			if inputLen == 0 {
				filterDataResult[i] = nil
				filterIndexResult[i] = nil
			} else {
				if err = filterFunc(it[align:align+inputLen], filterDataResult[i], filterIndexResult[i]); err != nil {
					glog.Errorf("filter data service[%s] hid[%s] host[%s] keyList[%v] error: %s",
						service, hid, host, metricList, err.Error())
				}
			}

			// fmt.Printf("debug filterDataResult[%d]: \n", i, filterIndexResult[i], filterDataResult[i])
		}
	} else {
		// don't use filter
		glog.V(3).Infof("[Debug][doQueryRange] no filter")
		filterIndexResult = nil
		filterDataResult = calculateResule
	}
	innerTimer.timeTick("filter")
	// fmt.Println("debug filterDataResult: ", filterIndexResult, filterDataResult)

	// print perf info
	if glog.V(2) {
		bytesBuffer := bytes.NewBuffer([]byte{})
		var durationAll, durationList = innerTimer.getTimeConsumeResult()
		bytesBuffer.WriteString("[Perf][doQueryRange]: ")
		for _, it := range durationList {
			bytesBuffer.WriteString(
				fmt.Sprintf("step[%v](%v) time duration[%v]|",
					it.name, it.step, it.duration))
		}
		bytesBuffer.WriteString(fmt.Sprintf("all time duration[%v]\n", durationAll))
		glog.Infof(bytesBuffer.String())
	}

	return startTime, filterIndexResult, filterDataResult
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  parsePanelQuery
 *  Description:  解析DIY语法，形如：
 *                metrics[op expression]{instance selector}，其中op expression部分可省略
 *                目前代码对语法没有严格检查
 * =====================================================================================
 */
func (h *ApiHandler) parsePanelQuery(query string) (string, string, string) {
	var metrics string
	var opExpression string
	var instances string
	var bracePair int
	var squarePair int

	var begin int = 0
	for i, it := range query {
		switch it {
		case '[':
			if squarePair == 0 {
				metrics = util.StringTrim(query[begin:i])
				begin = i + 1
			}
			squarePair++
		case ']':
			squarePair--
			if squarePair == 0 {
				opExpression = util.StringTrim(query[begin:i])
				begin = i + 1
			}
		case '{':
			if bracePair == 0 {
				if len(metrics) == 0 {
					metrics = util.StringTrim(query[begin:i])
				}
				begin = i + 1
			}
			bracePair++
		case '}':
			bracePair--
			if bracePair == 0 {
				instances = util.StringTrim(query[begin:i])
				begin = i + 1
			}
		default:
		}
	}

	if len(metrics) == 0 {
		metrics = util.StringTrim(query)
	}

	return metrics, opExpression, instances
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  parseMetricsReg
 *  Description:  解析正则表达式语法，并从dict server中获取对应的keylist
 * =====================================================================================
 */
func (h *ApiHandler) parseMetricsReg(reg string, metricList []string) []string {
	var hitList []string = make([]string, 0)
	for _, it := range metricList {
		if hit, err := regexp.Match(reg, unsafe.String2Bytes(it)); err != nil {
			glog.Errorf("regexp.Match() error with pattern[%v] target[%v]", reg, it)
		} else if hit {
			hitList = append(hitList, it)
		}
	}

	return hitList
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  parseMetrics
 *  Description:  解析DIY语法，metric列表，形如：
 *                path|metric1,path|metric2,path|metric3
 * =====================================================================================
 */
func (h *ApiHandler) parseMetrics(metrics string) []string {
	var metricList []string = make([]string, 0)
	var metric string

	var begin int = 0
	for i, it := range metrics {
		switch it {
		case '&':
			fallthrough
		case ';':
			fallthrough
		case ',':
			metric = util.StringTrim(metrics[begin:i])
			if len(metric) > 0 {
				metricList = append(metricList, metric)
			}
			begin = i + 1
		case '|':
		default:
		}
	}
	metric = util.StringTrim(metrics[begin:len(metrics)])
	if len(metric) > 0 {
		metricList = append(metricList, metric)
	}

	return metricList
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  parseService
 *  Description:  解析DIY语法，从metric中获取service信息
 *                形如：service|path|path|metric1
 * =====================================================================================
 */
func (h *ApiHandler) parseService(metrics string) string {
	for i, it := range metrics {
		switch it {
		case '|':
			fallthrough
		case '\\': // 处理可能出现的转义字符
			return util.StringTrim(metrics[0:i])
		default:
		}
	}

	return ""
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  parseRealMetric
 *  Description:  解析DIY语法，从metric中获取除去service的实际metric信息
 *                形如：service|path|path|metric1
 * =====================================================================================
 */
func (h *ApiHandler) parseRealMetric(metrics string) string {
	for i, it := range metrics {
		switch it {
		case '|':
			return util.StringTrim(metrics[i+1:])
		default:
		}
	}

	return ""
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  parsePureMetric
 *  Description:  解析DIY语法，从metric中获取除去service的真实metric信息
 *                形如：service|path|path|metric1
 * =====================================================================================
 */
func (h *ApiHandler) parsePureMetric(metrics string) string {
	for i, it := range metrics {
		switch it {
		case '|':
			return util.StringTrim(metrics[i+1:])
		default:
		}
	}

	return ""
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  parseInstanceSelector
 *  Description:  解析DIY语法，形如：
 *                hostId=dds-2ze2ae2045feb3e4{hid=4931065, pid=46734}, host=11.218.80.161:3079-P; host2=-p
 * =====================================================================================
 */
func (h *ApiHandler) parseInstanceSelector(instances string) map[string]string {
	var instanceSelector map[string]string = make(map[string]string, 0)

	var key string
	var value string
	var begin int = 0
	var paired bool
	for i, it := range instances {
		switch it {
		case '=':
			key = util.StringTrim(instances[begin:i])
			begin = i + 1
			paired = false
		case '{':
			fallthrough
		case '}':
			fallthrough
		case ';':
			fallthrough
		case ',':
			if paired == false {
				value = util.StringTrim(instances[begin:i])
				instanceSelector[key] = value
				paired = true
			}
			begin = i + 1
		default:
		}
	}
	value = util.StringTrim(instances[begin:len(instances)])
	instanceSelector[key] = value

	// 这部分代码为了兼容原inspector的query格式，在新inspector中换个名字，表意更明确
	// instanceSelector["hid"] = instanceSelector["hid"]
	// instanceSelector["pid"] = instanceSelector["pid"]
	if value, ok := instanceSelector["id"]; ok {
		// 兼容老系统，老系统的hid使用的是id
		instanceSelector["hid"] = value
	}
	instanceSelector["instanceName"] = instanceSelector["hostId"]
	instanceSelector["instanceId"] = instanceSelector["hid"]
	if _, ok := instanceSelector["host2"]; ok {
		instanceSelector["subHost"] = instanceSelector["host2"]
	}

	// special op with host
	if index := strings.IndexByte(instanceSelector["host"], '-'); index != -1 {
		instanceSelector["host"] = instanceSelector["host"][:index]
	}
	if index := strings.IndexByte(instanceSelector["subHost"], '-'); index != -1 {
		instanceSelector["subHost"] = instanceSelector["subHost"][:index]
	}

	return instanceSelector
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  getFromStore
 *  Description:
 * =====================================================================================
 */
func (h *ApiHandler) getFromStore(service string, pid uint32, hid int32, host string,
	keyList []string, start, end uint32) []*core.InfoRange {

	var conn *grpc.ClientConn
	var err error

	// select store server
	var allStore []*heartbeat.NodeStatus
	if allStore, err = configure.Options.HeartbeatServer.GetServices(heartbeat.ModuleStore, heartbeat.ServiceBoth); err != nil {
		glog.Errorf("fail to get store server address list: %s", err.Error())
		return nil
	}
	if len(allStore) == 0 {
		glog.Errorf("store server all done")
		return nil
	}
	var n = util.HashInstanceByHid(hid, len(allStore))
	var address = strings.Replace(allStore[n].Name, "_", ".", -1)
	if glog.V(3) {
		for _, it := range allStore {
			glog.Infof("[Debug][getFromStore] allStoreList[%v]", *it)
		}
		glog.Infof("[Debug][getFromStore] call store server[%v]", address)
	}

	// get data from store server
	if conn, err = grpc.Dial(address, grpc.WithInsecure()); err != nil {
		glog.Errorf("fail to dial to address[%s] with grpc: %s", address, err.Error())
		return nil
	}
	defer conn.Close()

	c := store.NewStoreServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(configure.Options.StoreTimeout)*time.Second)
	defer cancel()

	var res *store.StoreQueryResponse
	res, err = c.Query(ctx, &store.StoreQueryRequest{
		QueryList: []*core.Query{
			&core.Query{
				Header: &core.Header{
					Service: service,
					Hid:     hid,
					Host:    host,
				},
				KeyList:   keyList,
				TimeBegin: start,
				TimeEnd:   end,
			},
		},
	})
	if err != nil {
		glog.Errorf("fail to query from store server[%s]: %s", address, err.Error())
		return nil
	}
	if res.GetError().GetErrno() != 0 {
		glog.Errorf("query from store server[%s] with error: %s[%d]", address, res.GetError().GetErrmsg(), res.GetError().GetErrno())
	}

	return res.GetSuccessList()
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  getFromCollector
 *  Description:
 * =====================================================================================
 */
func (h *ApiHandler) getFromCollector(service string, pid uint32, hid int32, host string,
	keyList []string, start, end uint32) []*core.InfoRange {

	var conn *grpc.ClientConn
	var err error

	// select collector server
	var aliveCollector []*heartbeat.NodeStatus
	if aliveCollector, err = configure.Options.HeartbeatServer.GetServices(heartbeat.ModuleCollector, heartbeat.ServiceAlive); err != nil {
		glog.Errorf("fail to get collector server address list: %s", err.Error())
		return nil
	}
	if len(aliveCollector) == 0 {
		glog.Errorf("collector server all done")
		return nil
	}
	var n = util.HashInstance(pid, hid, len(aliveCollector))
	var address = strings.Replace(aliveCollector[n].Name, "_", ".", -1)
	if glog.V(3) {
		for _, it := range aliveCollector {
			glog.Infof("[Debug][getFromCollector] aliveCollector[%v]", *it)
		}
		glog.Infof("[Debug][getFromCollector] call collelctor server[%v]", address)
	}

	// get data from collector server
	conn, err = grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		glog.Errorf("fail to dial to address[%s] with grpc: %s", address, err.Error())
		return nil
	}
	defer conn.Close()

	c := collector.NewCollectorClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(configure.Options.CollectorTimeout)*time.Second)
	defer cancel()

	var res *collector.CollectorQueryResponse
	res, err = c.Query(ctx, &collector.CollectorQueryRequest{
		QueryList: []*core.Query{
			&core.Query{
				Header: &core.Header{
					Service: service,
					Hid:     hid,
					Host:    host,
				},
				KeyList:   keyList,
				TimeBegin: start,
				TimeEnd:   end,
			},
		},
	})
	if err != nil {
		glog.Errorf("fail to query from collector server[%s]: %s", address, err.Error())
		return nil
	}
	if res.GetError().GetErrno() != 0 {
		glog.Errorf("query from collector server[%s] with error: %s[%d]", address, res.GetError().GetErrmsg(), res.GetError().GetErrno())
		for _, it := range res.FailureList {
			glog.Errorf("query from collector server[%s] with each error: %s[%d]", address, it.Error.Errmsg, it.Error.Errno)
		}
	}

	return res.GetSuccessList()
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  infoRangeList2dataMap
 *  Description:
 * =====================================================================================
 */
func (h *ApiHandler) infoRangeList2dataMap(infoRangeList []*core.InfoRange, start, end uint32) map[string][]int64 {
	// fmt.Println("debug infoRangeList2dataMap: ", start, end)

	// compose data
	var dataMap map[string][]int64 = nil
	var count uint32
	for _, it := range infoRangeList {
		count = it.GetCount()
		var tmp = h.data2map(it.Data, start, end, count)
		dataMap = h.mergeDataMap(dataMap, tmp)
	}

	// cut off data

	return dataMap
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  data2map
 *  Description:
 * =====================================================================================
 */
func (h *ApiHandler) data2map(data []byte, start, end, count uint32) map[string][]int64 {
	var result = make(map[string][]int64)
	var n = 0
	// 之前以为grafana请求的end是最后一个显示点，其实是最后一个现实点+1
	// end++ // 由于end自身也是需要返回的时间点，所以真正的end还需要后移1个时间点

	// read list count
	var listCount = binary.BigEndian.Uint32(data[n : n+4])
	n += 4

	// read list
	for i := 0; i < int(listCount); i++ {
		// read key size
		var keySize = binary.BigEndian.Uint32(data[n : n+4])
		n += 4

		// read key
		var key = unsafe.Bytes2String(data[n : n+int(keySize)])
		n += int(keySize)

		// read value count
		var valueCount = binary.BigEndian.Uint32(data[n : n+4])
		n += 4

		// reserve n data at head and n data at tail (n is count)
		var realValue = make([]int64, int((end-start+1)+2*count))
		for i, _ := range realValue {
			realValue[i] = util.NullData
		}
		// read value
		var realStart = start - count
		for j := 0; j < int(valueCount); j++ {
			// read timestamp
			var timestamp = binary.BigEndian.Uint32(data[n : n+4])
			n += 4

			// read valueSize
			var valueSize = binary.BigEndian.Uint32(data[n : n+4])
			n += 4

			// read value
			var value = data[n : n+int(valueSize)]
			n += int(valueSize)

			// uncompress value
			if timestamp < realStart {
				continue
			}
			var index = (timestamp - realStart)
			// fmt.Println("debug uncompressValue: ", start, end, realStart, timestamp, len(realValue), index, count)
			h.uncompressValue(value, realValue[:index])
		}
		// write key value to result
		if _, ok := result[key]; !ok {
			if valueCount == 0 {
				result[key] = nil
			} else {
				result[key] = realValue[(start - realStart):(end - realStart)]
			}
		}
	}

	return result
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  mergeDataMap
 *  Description:  将collector数据与store数据进行整合，并去除重复数据
 * =====================================================================================
 */
func (h *ApiHandler) mergeDataMap(xMap, yMap map[string][]int64) map[string][]int64 {
	if xMap == nil {
		return yMap
	}
	if yMap == nil {
		return xMap
	}

	// merge value if x and y both exist
	for xKey, xValue := range xMap {
		if yValue, ok := yMap[xKey]; ok {
			if len(xValue) == 0 {
				xMap[xKey] = yValue
			}
			for i := 0; i < len(xValue); i++ {
				if xValue[i] == util.NullData {
					xValue[i] = yValue[i]
				}
			}
		}
	}

	// add y-only item to x
	for yKey, yValue := range yMap {
		if _, ok := xMap[yKey]; !ok {
			xMap[yKey] = yValue
		}
	}

	return xMap
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  metricList2keyList
 *  Description:  将metric列表通过字典转换成为压缩后的keylist
 * =====================================================================================
 */
func (h *ApiHandler) metricList2keyList(service string, metricList, keyList []string) ([]string, error) {
	if keyList == nil {
		keyList = make([]string, len(metricList))
	}
	keyList = keyList[0:0]

	var dict, ok = configure.Options.DictServerMap.Load(service)
	if !ok {
		var errStr = fmt.Sprintf("DictServer[%s] not exist", service)
		return nil, errors.New(errStr)
	}
	for _, it := range metricList {
		if key, err := dict.(*dictServer.DictServer).GetValueOnly(it); err != nil {
			glog.Warningf("dict server key[%s] get value error: %s", it, err.Error())
			keyList = append(keyList, "")
		} else {
			keyList = append(keyList, key)
		}
	}

	if len(keyList) == 0 {
		var errStr = fmt.Sprintf("can't find any key in DictServer[%v]", service)
		return nil, errors.New(errStr)
	}

	return keyList, nil
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  uncompressValue
 *  Description:  value解压缩
 * =====================================================================================
 */
func (h *ApiHandler) uncompressValue(data []byte, output []int64) []int64 {
	if output == nil {
		output = make([]int64, 0)
	}

	if result, err := compress.Decompress(data, output); err != nil {
		glog.Errorf("uncompress error: %s", err.Error())
		return nil
	} else {
		return result
	}

}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  array2json
 *  Description:  value解压缩
 * =====================================================================================
 */
func (h *ApiHandler) array2json(nameList []string,
	timestamp uint32, step uint32,
	index [][]int, data [][]int64) string {

	var resultBytes []byte
	var result = &PrometheusQueryRangeModel{}
	result.Status = "success"
	result.Data.ResultType = "matrix"
	result.Data.Result = make([]PrometheusQueryRangeResult, len(nameList))
	for i, _ := range nameList {
		result.Data.Result[i].Metric = make(map[string]string)
		var fieldList = strings.Split(nameList[i], "|")
		for j, it := range fieldList {
			result.Data.Result[i].Metric[fmt.Sprintf("field%d", j+1)] = it
		}
		result.Data.Result[i].Metric["name"] = fieldList[len(fieldList)-1]
		result.Data.Result[i].Values = make([][2]float64, len(data[i]))
		result.Data.Result[i].Values = result.Data.Result[i].Values[0:0]
		for j, it := range data[i] {
			if it != util.NullData {
				if index != nil && i < len(index) && index[i] != nil {
					result.Data.Result[i].Values =
						append(result.Data.Result[i].Values,
							[2]float64{float64((timestamp + uint32(index[i][j])) * step),
								float64(it) / util.FloatMultiple})
				} else {
					result.Data.Result[i].Values =
						append(result.Data.Result[i].Values,
							[2]float64{float64((timestamp + uint32(j)) * step),
								float64(it) / util.FloatMultiple})
				}
			}
		}
	}
	resultBytes, _ = json.Marshal(result)
	return string(resultBytes)
}

/*
// =====================================================================================
//  test
// =====================================================================================
*/
func FakeResult() string {
	var resultBytes []byte
	var result = &PrometheusQueryRangeModel{}
	var now uint32 = uint32(time.Now().Unix())
	var count = 60

	result.Status = "success"
	result.Data.ResultType = "matrix"
	result.Data.Result = make([]PrometheusQueryRangeResult, 2)

	// data 1
	result.Data.Result[0].Metric = make(map[string]string)
	result.Data.Result[0].Metric["name"] = "result"
	result.Data.Result[0].Values = make([][2]float64, 0)
	now = uint32(time.Now().Unix()) - uint32(count)
	count = 60
	for i := 0; i < count; i++ {
		if i > 25 && i < 35 {
			continue
		}
		result.Data.Result[0].Values = append(result.Data.Result[0].Values, [2]float64{float64(now + uint32(i)), float64(i)})
	}

	// data 2
	result.Data.Result[1].Metric = make(map[string]string)
	result.Data.Result[1].Metric["name"] = "result"
	result.Data.Result[1].Values = make([][2]float64, 0)
	now = uint32(time.Now().Unix()) - uint32(count)
	count = 60
	for i := 0; i < count; i++ {
		result.Data.Result[1].Values = append(result.Data.Result[1].Values, [2]float64{float64(now + uint32(i)), float64(count - i)})
	}

	// convert and send
	resultBytes, _ = json.Marshal(result)
	return string(resultBytes)
}
