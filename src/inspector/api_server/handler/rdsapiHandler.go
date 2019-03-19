/*
// =====================================================================================
//
//       Filename:  rdsapiHandler.go
//
//    Description:
//
//        Version:  1.0
//        Created:  10/18/2018 03:34:31 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"inspector/api_server/configure"
	"inspector/util"
	"inspector/util/unsafe"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
)

const CODE_SUCCESS int = 200
const STAT_SUCCESS string = "SUCCESS"
const CODE_FAILURE int = 400
const STAT_FAILURE string = "ERROR"

/*
// ===  STRUCT  ========================================================================
//         Name:  rdsapiResultModel
//  Description:
// =====================================================================================
*/
type rdsapiResultModel struct {
	Code   int         `json:"code"`
	Status string      `json:"status"`
	Msg    string      `json:"msg"`
	Data   interface{} `json:"data"`
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  queryStandardInfo
//  Description:
// =====================================================================================
*/
func (h *ApiHandler) queryStandardInfo(hid string) (StandardInfoModel, error) {
	var v StandardInfoModel
	var ok bool

	StandardInfoMapLocker.RLock()
	defer StandardInfoMapLocker.RUnlock()

	if v, ok = StandardInfoMap[hid]; !ok {
		var errStr = fmt.Sprintf("instance[%v] is not exist", hid)
		glog.Warning(errStr)
		return v, errors.New(errStr)
	}

	glog.V(3).Infof("[Debug][queryStandardInfo] hid[%v] infi[%v]", hid, v)
	return v, nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  dataFormat
//  Description:  按照rdsapi的格式进行格式化输出
// =====================================================================================
*/

func (h *ApiHandler) dataFormat(service string, start, end uint32, filterIndexResult [][]int, filterDataResult [][]int64, divNumber float64) (string, error) {
	// fmt.Println("debug dataFormat: ", service, start, end, filterIndexResult, filterDataResult, divNumber)

	var step int = configure.Options.LocalConfigCache[service]["step"].(int)

	// return if null data
	if filterDataResult == nil || len(filterDataResult) == 0 {
		// 从实际效果来看，这个协议返回卵用没有
		// 为了确保兼容性，我也只能这么写
		var result = rdsapiResultModel{
			Code:   CODE_FAILURE,
			Status: STAT_FAILURE,
			Msg:    "Empty Data",
		}
		var jstr, _ = json.Marshal(result)
		return string(jstr), errors.New("filterDataResult is nil")
	}

	var nullData = "NULL"
	// 由于瑶池对NULL数据的不兼容，所以对于redis_proxy服务需要把NULL值改为0值
	if service == "redis_proxy" {
		nullData = "0"
	}

	// compose data
	// 这里必须保证buffer足够大，否则会出现内存访问错误
	var bytesBuffer = bytes.NewBuffer(make([]byte, 0, 1024*512 /*128KB*/))
	var data = make([]map[string]interface{}, 0)

	// 假设filterDataResult至少有一条数据，并且多条数据的长度相等
	var n int = -1
	for i, it := range filterDataResult {
		if len(it) > 0 {
			n = i
			break
		}
	}
	// 如果一条数据都没有，返回错误
	if n == -1 {
		var result = rdsapiResultModel{
			Code:   CODE_FAILURE,
			Status: STAT_FAILURE,
			Msg:    "All Data Is Empty",
		}
		var jstr, _ = json.Marshal(result)
		return string(jstr), errors.New("all filterDataResult is nil")
	}
	// 处理每一个时间节点的n个key的数据
	for j := 0; j < len(filterDataResult[n]); j++ {
		// 纵向将数值按照rdsapi的格式写入buffer
		var preBuffLen = len(bytesBuffer.Bytes())
		for i := 0; i < len(filterDataResult); i++ {
			if len(filterDataResult[i]) == 0 || filterDataResult[i][j] == util.NullData {
				bytesBuffer.WriteString(nullData)
			} else {
				var v = filterDataResult[i][j]
				if divNumber == 0 { // 返回整型
					bytesBuffer.WriteString(strconv.FormatInt(v, 10))
				} else { // 返回浮点型
					bytesBuffer.WriteString(strconv.FormatFloat(float64(v)/divNumber, 'f', 1, 64))
				}
			}
			bytesBuffer.WriteByte('&')
		}
		if filterIndexResult != nil || len(filterIndexResult) > 0 {
			var oneData = map[string]interface{}{
				"Date": time.Unix(int64((start+uint32(filterIndexResult[n][j]))*uint32(step)), 0).UTC().Format("2006-01-02T15:04:05Z"),
				// "-1"的原因是最后多写了一个"&"
				"Value": unsafe.Bytes2String(bytesBuffer.Bytes()[preBuffLen : len(bytesBuffer.Bytes())-1]),
			}
			data = append(data, oneData)
		} else {
			var oneData = map[string]interface{}{
				"Date": time.Unix(int64((start+uint32(j))*uint32(step)), 0).UTC().Format("2006-01-02T15:04:05Z"),
				// "-1"的原因是最后多写了一个"&"
				"Value": unsafe.Bytes2String(bytesBuffer.Bytes()[preBuffLen : len(bytesBuffer.Bytes())-1]),
			}
			data = append(data, oneData)
		}
	}

	var result = rdsapiResultModel{
		Code:   CODE_SUCCESS,
		Status: STAT_SUCCESS,
		Data:   data,
	}
	// json marshal
	var jstr, err = json.Marshal(result)
	if err != nil {
		var errStr = fmt.Sprintf("rdsapi json.Marshal error: %s", err.Error())
		glog.Errorf(errStr)
		return "", errors.New(errStr)
	}
	return string(jstr), nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  RdsApiHandler
//  Description:
// =====================================================================================
*/
func (h *ApiHandler) RdsApiHandler(w http.ResponseWriter, r *http.Request) {
	glog.V(1).Infof("[Trace][RdsApiHandler] called: Request[%v] ", r)

	// 由于历史原因，inspector最初是专门为mongo定制的，所以所有没有service前缀的服务，默认都是mongodb
	// 其他其他service都会标记一个前缀，短期先临时使用硬编码过滤来解决
	var service string = "mongodb"
	var serviceList []string = []string{"redis_proxy"}

	// query parse
	var query string
	var metrics string
	var metricList []string
	var opExpression string = ""
	var instanceSelector map[string]string = make(map[string]string)
	var startTime uint32
	var endTime uint32

	var params map[string][]string
	var err error

	params, err = url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		var errStr = fmt.Sprintf("rdsapi parse query error: %s", err.Error())
		glog.Warning(errStr)
		fmt.Fprintln(w, errStr)
	}
	glog.V(3).Infof("[Debug][RdsApiHandler] http params: params[%v] ", params)

	query = params["valueFormat"][0]
	var start = params["start"][0]
	var end = params["end"][0]
	var tmpTime int
	tmpTime, _ = strconv.Atoi(start)
	startTime = uint32(tmpTime)
	tmpTime, _ = strconv.Atoi(end)
	endTime = uint32(tmpTime)
	glog.V(3).Infof("[Debug][RdsApiHandler] params query: query[%v] ", query)

	metrics, opExpression, _ = h.parsePanelQuery(query)
	glog.V(3).Infof("[Debug][RdsApiHandler] after parsePanelQuery: metrics[%v], opExpression[%v] ", metrics, opExpression)

	// 处理历史遗留的前缀问题
	metricList = h.parseMetrics(metrics)
	for i, metric := range metricList {
		for _, serv := range serviceList {
			if strings.HasPrefix(metric, serv) {
				service = serv
				metricList[i] = metric[len(serv)+1:]
			}
		}
	}
	glog.V(3).Infof("[Debug][RdsApiHandler] after parseMetrics: metricList[%v]", metricList)

	instanceSelector["pid"] = "0" // 目前需要兼容老系统，pid没用，随便传
	instanceSelector["hid"] = params["instId"][0]
	instanceSelector["host"] = params["host"][0]
	var hid = instanceSelector["hid"]

	var showStep int = int(endTime-startTime) / 600

	var filterIndexResult [][]int
	var filterDataResult [][]int64

	glog.V(3).Infof("[Debug][RdsApiHandler] do special data change: "+
		"service[%v], metricList[%v], opExpression[%v], instanceSelector[%v], startTime[%v], endTime[%v]",
		service, metricList, opExpression, instanceSelector, startTime, endTime)

	var divNumber float64 = 0
	// 根据业务需求所做的TMD定制化硬编码，为了兼容原来的系统，我TMD的也很无奈啊
	// 别为我这段代码什么意思，我TM也不知道，我是为了程序兼容性，从老代码里抄过来的
	// 为什么要大量代码复制，鬼TM知道这些代码会有什么问题，还是case by case的看问题比较方便
	switch metricList[0] {
	case "cpu_usage":
		glog.V(3).Infof("[Debug][RdsApiHandler] do cpu_usage")

		metricList = []string{
			"systemInfo|user cpu usage per 10 thousand",
			"systemInfo|system cpu usage per 10 thousand",
		}
		opExpression = "arrayAdd($1, $2)"

		// do query
		startTime, filterIndexResult, filterDataResult = h.doQueryRange(service, metricList, opExpression, instanceSelector, startTime, endTime, showStep /* no filter */)

		var standardInfo, err = h.queryStandardInfo(hid)
		if err != nil {
			glog.Error("get standard info error in cpu_usage: ", err.Error())
			break
		}

		if filterDataResult != nil {
			// cpu usage
			for i, row := range filterDataResult {
				for j, it := range row {
					if it != util.NullData {
						it /= int64(standardInfo.cpuMaxCore)
						filterDataResult[i][j] = it
						if it > 10000 {
							filterDataResult[i][j] = 10000
						}
					}
				}
			}
		}
		divNumber = 100
	case "mem_usage":
		glog.V(3).Infof("[Debug][RdsApiHandler] do mem_usage")

		metricList = []string{
			"mem|resident",
		}

		// do query
		startTime, filterIndexResult, filterDataResult = h.doQueryRange(service, metricList, opExpression, instanceSelector, startTime, endTime, showStep /* no filter */)

		var standardInfo, err = h.queryStandardInfo(hid)
		if err != nil {
			glog.Error("get standard info error in mem_usage: ", err.Error())
			break
		}

		if filterDataResult != nil {
			// mem usage
			for i, row := range filterDataResult {
				for j, it := range row {
					if it != util.NullData {
						it = it * 1000 / int64(standardInfo.memMaxSize)
						filterDataResult[i][j] = it
						if it > 1000 {
							filterDataResult[i][j] = 1000
						}
					}
				}
			}
		}
		divNumber = 10
	case "iops_usage":
		glog.V(3).Infof("[Debug][RdsApiHandler] do iops_usage")

		metricList = []string{
			"systemInfo|data iops",
		}
		opExpression = "arrayDiff($0)"

		// do query
		startTime, filterIndexResult, filterDataResult = h.doQueryRange(service, metricList, opExpression, instanceSelector, startTime, endTime, showStep /* no filter */)

		var standardInfo, err = h.queryStandardInfo(hid)
		if err != nil {
			glog.Error("get standard info error in iops_usage: ", err.Error())
			break
		}

		if filterDataResult != nil {
			// iops usage
			for i, row := range filterDataResult {
				for j, it := range row {
					if it != util.NullData {
						it = it * 1000 / int64(standardInfo.iopsMax)
						filterDataResult[i][j] = it
						if it > 1000 {
							filterDataResult[i][j] = 1000
						}
					}
				}
			}
		}
		divNumber = 10
	case "systemInfo|data iops":
		opExpression = "arrayDiff($0)"
		startTime, filterIndexResult, filterDataResult = h.doQueryRange(service, metricList, opExpression, instanceSelector, startTime, endTime, showStep /* no filter */)
	case "network|bytesIn":
		opExpression = "arrayDiff($0)"
		startTime, filterIndexResult, filterDataResult = h.doQueryRange(service, metricList, opExpression, instanceSelector, startTime, endTime, showStep /* no filter */)
	case "opcounters|insert":
		opExpression = "arrayDiff($0)"
		startTime, filterIndexResult, filterDataResult = h.doQueryRange(service, metricList, opExpression, instanceSelector, startTime, endTime, showStep /* no filter */)
	default:
		// do query
		glog.V(3).Infof("[Debug][RdsApiHandler] do normal")
		startTime, filterIndexResult, filterDataResult = h.doQueryRange(service, metricList, opExpression, instanceSelector, startTime, endTime, showStep /* no filter */)
		// fmt.Println("debug result", filterIndexResult, filterDataResult)

	}

	// 由于start time经过处理会变成虚拟时间，而end time没有改变，所以会出现bug
	// 这里造成了代码上的耦合，比较丑，不过rdsapi本来就是特殊处理的，所以就这样吧
	var step int = configure.Options.LocalConfigCache[service]["step"].(int)
	endTime /= uint32(step) // 针对大于1s采集的数据，通过虚拟时间变为1s采集
	var result string
	result, err = h.dataFormat(service, startTime, endTime, filterIndexResult, filterDataResult, divNumber)
	if err != nil {
		glog.Warningf("dataFormat params[%v] error: ", params, err.Error())
	}

	// fmt.Println("debug rdsapi result: ", result)
	fmt.Fprintln(w, result)
	glog.Flush()

	return
}
