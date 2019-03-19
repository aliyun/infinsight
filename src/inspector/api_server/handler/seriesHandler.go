/*
// =====================================================================================
//
//       Filename:  seriesHandler.go
//
//    Description:  下拉菜单处理函数
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
	"encoding/json"
	"fmt"
	"inspector/api_server/configure"
	"inspector/util"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/golang/glog"
)

/*
 * ===  STRUCT  ========================================================================
 *         Name:  Prometheus协议Series结构
 *  Description:
 * =====================================================================================
 */
type prometheusSeriesModel struct {
	Status string              `json:"status"` // success or failure
	Data   []map[string]string `json:"data"`
}

/*
 * ===  STRUCT  ========================================================================
 *         Name:  sqlForGrafanaVariable
 *  Description:
 * =====================================================================================
 */
type sqlForGrafanaVariable struct {
	base       string
	condition  string
	with_param bool
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  parserVar
//  Description:  目前解析器还比较搓，不支持强大的语法检查
//                必须严格按照“ filter{name{hid=abc, pid=def}} ”的格式输入
// =====================================================================================
*/
func (h *ApiHandler) parserVar(input string) (filter, name string, hid, pid int) {
	var begin = 0
	var end = 0

	// get filter
	if end = strings.IndexByte(input[begin:], '{'); end == -1 {
		glog.Errorf("parserVar[%s] error: expect filter", input)
		return "", "", -1, -1
	}
	end += begin
	filter = util.StringTrim(input[begin:end])
	begin = end + 1

	// get name
	if end = strings.IndexByte(input[begin:], '{'); end == -1 {
		glog.Errorf("parserVar[%s] error: expect name", input)
		return "", "", -1, -1
	}
	end += begin
	name = util.StringTrim(input[begin:end])
	begin = end + 1

	// get hid
	if end = strings.IndexByte(input[begin:], '='); end == -1 {
		glog.Errorf("parserVar[%s] error: expect '='", input)
		return "", "", -1, -1
	}
	end += begin
	begin = end + 1
	if end = strings.IndexByte(input[begin:], ','); end == -1 {
		glog.Errorf("parserVar[%s] error: expect hid", input)
		return "", "", -1, -1
	}
	end += begin
	hidStr := util.StringTrim(input[begin:end])
	hid, _ = strconv.Atoi(hidStr)
	begin = end + 1

	// get pid
	if end = strings.IndexByte(input[begin:], '='); end == -1 {
		glog.Errorf("parserVar[%s] error: expect '='", input)
		return "", "", -1, -1
	}
	end += begin
	begin = end + 1
	if end = strings.IndexByte(input[begin:], '}'); end == -1 {
		glog.Errorf("parserVar[%s] error: expect pid", input)
		return "", "", -1, -1
	}
	end += begin
	pidStr := util.StringTrim(input[begin:end])
	pid, _ = strconv.Atoi(pidStr)
	begin = end + 1

	return
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  SeriesHandler
//  Description:
// =====================================================================================
*/
func (h *ApiHandler) SeriesHandler(w http.ResponseWriter, r *http.Request) {
	glog.V(1).Infof("[Trace][SeriesHandler] called: Request[%v] ", r)
	h.timeReset()

	var params url.Values
	var tasks map[string]interface{}
	var err error

	if params, err = url.ParseQuery(r.URL.RawQuery); err != nil {
		glog.Errorf("ParseQuery[%s] error: %s", r.URL.RawQuery, err.Error())
		return
	}
	filter, instName, hid, pid := h.parserVar(params["match[]"][0])
	_ = instName
	glog.V(3).Infof("[Debug][SeriesHandler] query parse: filter[%v], instName[%v], hid[%v], pid[%v]", filter, instName, hid, pid)
	fmt.Println("[Debug][SeriesHandler] query parse: filter[%v], instName[%v], hid[%v], pid[%v]", filter, instName, hid, pid)

	var index int
	if index = strings.LastIndexByte(filter, '_'); index == -1 {
		glog.Errorf("filter[%s] is in invalid format, invalid format is 'service_name' ", filter)
		return
	}
	var service = filter[:index]
	if tasks, err = configure.Options.ConfigServer.GetMap(util.TaskListCollection, service, "distribute"); err != nil {
		glog.Errorf("get task from [%s.%s] error", util.TaskListCollection, service)
		return
	}

	var l []string
	var level = filter[index+1:]
	switch level {
	case "sharding":
	case "cluster":
		for _, v := range tasks {
			l = append(l, fmt.Sprintf("%s{hid=%d, pid=0}", service,
				int(v.(map[string]interface{})["hid"].(float64)),
			))
		}
	case "instance":
		for k, v := range tasks {
			if int(v.(map[string]interface{})["hid"].(float64)) == hid {
				l = append(l, strings.Replace(k, "_", ".", -1))
			}
		}
	default:
	}

	h.timeTick("find in taskList")

	var result = h.list2json(filter, l)
	fmt.Fprintln(w, result)
	h.timeTick("to json")

	if glog.V(2) {
		bytesBuffer := bytes.NewBuffer([]byte{})
		var durationAll, durationList = h.getTimeConsumeResult()

		bytesBuffer.WriteString("[Perf][SeriesHandler]: ")
		for _, it := range durationList {
			bytesBuffer.WriteString(
				fmt.Sprintf("step[%v](%v) time duration[%v]|",
					it.name, it.step, it.duration))
		}
		bytesBuffer.WriteString(fmt.Sprintf("all time duration[%v]\n", durationAll))
		glog.Infof(bytesBuffer.String())
	}

	glog.Flush()
	return
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  list2json
//  Description:
// =====================================================================================
*/
func (h *ApiHandler) list2json(name string, list []string) string {
	var resultBytes []byte
	var result = &prometheusSeriesModel{}
	result.Status = "success"

	var index int
	if index = strings.LastIndexByte(name, '_'); index == -1 {
		glog.Errorf("name[%s] is in invalid format, invalid format is 'service_name' ", name)
		return ""
	}
	var item = name[index+1:]
	switch item {
	case "instance":
		result.Data = make([]map[string]string, len(list))

		// 对instance列做硬编码特殊处理
		// 非instance列，不排序，并且增加一个all选项
		// instance列，按“主-从-隐藏”的顺序进行排序
		// 由于写个排序结构太麻烦了，还不如简单冒泡排序处理
		{
			for i := 0; i < len(list); i++ {
				for j := i + 1; j < len(list); j++ {
					var r1 = list[i][strings.LastIndexByte(list[i], '-')+1:]
					var r2 = list[j][strings.LastIndexByte(list[j], '-')+1:]
					if (r1 == "S" && r2 == "P") || (r1 == "H" && r2 == "S") {
						list[i], list[j] = list[j], list[i]
					}
				}
			}
		}

		// write list
		for i, it := range list {
			result.Data[i] = make(map[string]string)
			result.Data[i]["__name__"] = name
			result.Data[i][name] = it
		}
	case "cluster":
		result.Data = make([]map[string]string, len(list)+1)

		// write list
		for i, it := range list {
			result.Data[i] = make(map[string]string)
			result.Data[i]["__name__"] = name
			result.Data[i][name] = it
		}

		// write the special at last
		result.Data[len(list)] = make(map[string]string)
		result.Data[len(list)]["__name__"] = name
		result.Data[len(list)][name] = "all{hid=0, pid=0}"

	case "sharding":
		result.Data = make([]map[string]string, len(list)+1)

		// write the special at first
		result.Data[0] = make(map[string]string)
		result.Data[0]["__name__"] = name
		result.Data[0][name] = "all{hid=0, pid=0}"

		// write list
		for i, it := range list {
			result.Data[i+1] = make(map[string]string)
			result.Data[i+1]["__name__"] = name
			result.Data[i+1][name] = it
		}
	default:
		glog.Errorf("unknown series type[%s]", item)
		return ""
	}

	resultBytes, _ = json.Marshal(result)
	return string(resultBytes)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  isValidId
//  Description:  检测是否是有效id
// =====================================================================================
*/
func (h *ApiHandler) isValidId(id string) bool {
	var num, err = strconv.Atoi(id)
	if err != nil {
		return false
	}
	if num <= 0 {
		return false
	}
	return true
}
