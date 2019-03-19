package httpJsonSteps

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"inspector/collector_server/metric"
	"inspector/collector_server/model"
	"inspector/dict_server"
	"inspector/util"
	"inspector/util/unsafe"
	"inspector/util/whatson"

	"github.com/golang/glog"
)

const (
	glue byte = 124 // "|"
//	expandTimes      = 100
)

// var (
// 	expandItems = map[string]struct{}{
// 		"usage_cpu":      {},
// 		"usage_cpu_sys":  {},
// 		"usage_cpu_user": {},
// 	}
// )

func NewStepParse(id, serviceName string, instance *model.Instance, parser whatson.Parser,
	ds *dictServer.DictServer) *StepParse {
	return &StepParse{
		Id:          id,
		ServiceName: serviceName,
		Instance:    instance,
		JsonParser:  parser,
		Ds:          ds,
		byteBuffer:  new(bytes.Buffer),
		idx:         0,
	}
}

// parse data
type StepParse struct {
	Id          string                 // id == name
	ServiceName string                 // name: mongo3.4, redis4.0
	Instance    *model.Instance        // ip:port
	errG        error                  // global error
	JsonParser  whatson.Parser         // bson parser
	Ds          *dictServer.DictServer // dict server, not owned

	// below variables are used in callback
	idx        int
	mp         map[int]interface{} // map
	byteBuffer *bytes.Buffer       // store prefix key: a|b|c
}

func (sp *StepParse) Name() string {
	return sp.Id
}

func (sp *StepParse) Error() error {
	return sp.errG
}

func (sp *StepParse) Before(input interface{}, params ...interface{}) (bool, error) {
	return true, nil
}

/*
 * Input: json data: [][]byte
 * Output: map int(dict-server) -> value
 * Generally speaking, it maybe better to combine parsing and storing step together to
 * eliminate copy. But I prefer to separate them to make code clean.
 */
func (sp *StepParse) DoStep(input interface{}, params ...interface{}) (interface{}, error) {
	glog.V(2).Infof("step[%s] instance-name[%s] with service[%s] called",
		sp.Id, sp.Instance.Addr, sp.Instance.DBType)

	// update metric
	metric.GetMetric(sp.ServiceName).AddStepCount(sp.Id)

	raws := input.([][]byte)
	sp.mp = make(map[int]interface{}) // regenerate every time
	for _, raw := range raws {
		if len(raw) == 0 {
			glog.Errorf("input raw data is empty")
			return sp.mp, nil
		}

		metric.GetMetric(sp.ServiceName).AddBytesGet(uint64(len(raw))) // metric
		sp.idx = 0                                                     // reset
		sp.byteBuffer.Truncate(0)                                      // reset

		if err := sp.JsonParser.Parse(raw, sp.callback); err != nil {
			glog.Errorf("step[%s] instance-name[%s] with service[%s]: parse data error[%v]",
				sp.Id, sp.Instance.Addr, sp.Instance.DBType, err)
			sp.errG = err
			return sp.mp, err
		}
	}

	return sp.mp, nil
}

func (sp *StepParse) After(input interface{}, params ...interface{}) (bool, error) {
	return true, nil
}

func (sp *StepParse) callback(keyPath []string, value []byte, valueType whatson.ValueType) error {
	// back tracking
	keyLength := len(keyPath)
	if keyLength == 0 {
		// return immediately if input is []
		return nil
	}

	if sp.idx >= keyLength {
		util.BackTracking(sp.byteBuffer, sp.idx-keyLength+1, glue)
		sp.idx = keyLength - 1
	}

	// move forward. Strictly speaking, it's better to calculate "grow" byte and call
	// byteBuffer.Grow() function to save memory copy. But it wastes cpu resource so I
	// didn't do that, maybe latter when memory becomes the bottleneck. todo
	for ; sp.idx < keyLength; sp.idx++ {
		if sp.idx != 0 {
			sp.byteBuffer.WriteByte(glue)
		}
		sp.byteBuffer.WriteString(keyPath[sp.idx])
	}

	// prune if type is array
	if valueType == whatson.ARRAY {
		return errors.New(whatson.CB_PATH_PRUNE)
	}

	// continue if type isn't needed or key filtered
	if !neededType(valueType) {
		return nil
	}

	keyByte := sp.byteBuffer.Bytes()
	key := convertKey(keyByte) // shallow copy
	if val, err := sp.Ds.GetValue(key); err == nil {
		v := sp.JsonParser.ValueType2Interface(valueType, value)

		if valInt, err := util.RepString2Int(val); err == nil {
			// sp.mp[valInt] = expand(v, valueType, key)
			sp.mp[valInt] = v
		} else {
			return fmt.Errorf("step[%s] instance-name[%s] with service[%s]: convert long-key[%s] with short-key[%s] to int error[%v]",
				sp.Id, sp.Instance.Addr, sp.Instance.DBType, key, val, err)
		}
	}
	// do nothing when getValue return error because dict-server will add it later
	return nil
}

func neededType(valueType whatson.ValueType) bool {
	return valueType == whatson.INTEGER || valueType == whatson.FLOAT ||
		valueType == whatson.BOOL
}

func convertKey(input []byte) string {
	s := unsafe.Bytes2String(input)
	if strings.Contains(s, ".") == false {
		return s
	}
	return util.ConvertDot2Underline(s)
}

// func expand(v interface{}, tp whatson.ValueType, key string) interface{} {
// 	if tp != whatson.FLOAT {
// 		return v
// 	}
//
// 	idx := strings.LastIndex(key, string(glue))
// 	val := v.(float64)
// 	if idx == -1 {
// 		// if can't find glue, convert to int64
// 		return int64(val)
// 	}
// 	if _, ok := expandItems[key[idx+1:]]; !ok {
// 		// if not in the expand item lists, convert to int64
// 		return int64(val)
// 	}
//
// 	return int64(val * expandTimes)
// }
