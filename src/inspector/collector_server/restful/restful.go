package restful

import (
	"encoding/json"
	"fmt"
	"reflect"

	"inspector/util"

	"github.com/golang/glog"
	"github.com/gugemichael/nimo4go"
	"inspector/collector_server/metric"
)

const (
	debugAll = "all"
	debugNo  = "no"
)

var DebugPrint struct {
	/*
	 * 'all': print all
	 * 'random': random pick // not support now
	 * 'fix': fix ip:port list // not support now
	 * 'no': no print
	 */
	Switch   string // print option
	Position string // file position
	// Random  uint8    `json:"random"` // percentage x%
	// FixList []string `json:"fix"`    // fix print list
}

// register all rest api
func RestAPI() {
	registerDebug()  // register debug
	registerMetric() // register metric
	// add below if has more
}

func IsDebugOpen() bool {
	return DebugPrint.Switch == debugAll
}

func registerDebug() {
	util.HttpApi.RegisterAPI("/debug", nimo.HttpPost, func(body []byte) interface{} {
		kv := make(map[string]interface{})
		if err := json.Unmarshal(body, &kv); err != nil {
			glog.Errorf("Register set DebugPrint wrong format[%v]", err)
			return map[string]string{"debug": "request json options wrong format"}
		}

		for name := range kv {
			if !reflect.ValueOf(&DebugPrint).Elem().FieldByName(name).IsValid() {
				return map[string]string{"debug": fmt.Sprintf("%s is not exist", name)}
			}
		}

		for name, value := range kv {
			field := reflect.ValueOf(&DebugPrint).Elem().FieldByName(name)
			switch field.Kind() {
			case reflect.String:
				if v, ok := value.(string); ok {
					switch name {
					case "Switch":
						if v != debugAll && v != debugNo {
							return map[string]string{"debug": fmt.Sprintf("switch type[%s] must in [all, no]", name)}
						}
					case "Position":
					}
					field.SetString(v)
					continue
				}
			default:
			}
			return map[string]string{"debug": fmt.Sprintf("%s option isn't corret", name)}
		}
		return map[string]string{"debug": "success"}
	})
}

func registerMetric() {
	type MetricRest struct {
		ItemMax                  interface{}
		ItemsMin                 interface{}
		ItemsAvg                 interface{}
		ItemsEmpty               interface{}
		BytesGetDelta            interface{}
		BytesGetTotal            interface{}
		BytesSendDelta           interface{}
		BytesSendTotal           interface{}
		BytesSendEachClient      interface{}
		SameDigitCompressPercent interface{}
		DiffCompressPercent      interface{}
		TotalCompressPercent     interface{}
		InstanceNumber           interface{}
		StepRunTimes             interface{}
		WorkflowDurationMax      interface{}
		WorkflowDurationMin      interface{}
		WorkflowDurationAvg      interface{}
		Uptime                   interface{}
	}
	util.HttpApi.RegisterAPI("/metrics", nimo.HttpGet, func([]byte) interface{} {
		ret := make(map[string]interface{}, 3)
		metric.MetricMap.Range(func(key, val interface{}) bool {
			metricRet := val.(*metric.Metric)
			ret[key.(string)] = &MetricRest{
				ItemMax:                  metricRet.GetItemsMax(),
				ItemsMin:                 metricRet.GetItemsMin(),
				ItemsAvg:                 metricRet.GetItemsAvg(),
				ItemsEmpty:               metricRet.GetItemsEmpty(),
				BytesGetDelta:            util.ConvertTraffic(metricRet.GetBytesGetDelta()),
				BytesGetTotal:            util.ConvertTraffic(metricRet.GetBytesGetTotal()),
				BytesSendDelta:           util.ConvertTraffic(metricRet.GetBytesSendDelta()),
				BytesSendTotal:           util.ConvertTraffic(metricRet.GetBytesSendTotal()),
				BytesSendEachClient:      metricRet.GetBytesSendClient(),
				SameDigitCompressPercent: metricRet.GetSameDigitCompressPercent(),
				DiffCompressPercent:      metricRet.GetDiffCompressPercent(),
				TotalCompressPercent:     metricRet.GetTotalCompressPercent(),
				InstanceNumber:           metricRet.GetInstanceNumber(),
				StepRunTimes:             metricRet.GetStepCount(),
				WorkflowDurationMax:      metricRet.GetWorkflowDurationMax(),
				WorkflowDurationMin:      metricRet.GetWorkflowDurationMin(),
				WorkflowDurationAvg:      metricRet.GetWorkflowDurationAvg(),
				Uptime:                   metricRet.GetUptime(),
			}
			return true
		})
		return ret
	})
}
