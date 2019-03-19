package mysqlSteps

import (
	"fmt"
	"strconv"
	"strings"

	"inspector/collector_server/metric"
	"inspector/collector_server/model"
	"inspector/dict_server"
	"inspector/util"
	"inspector/util/whatson"

	"github.com/golang/glog"
)

const (
	TYPE_INT = iota
	TYPE_FLOAT
	TYPE_STRING
	TYPE_UNKNOWN
)

func NewStepParse(id, serviceName string, instance *model.Instance, ds *dictServer.DictServer) *StepParse {
	return &StepParse{
		Id:          id,
		ServiceName: serviceName,
		Instance:    instance,
		Ds:          ds,
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
	mp map[int]interface{} // map
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

	var save = func(key string, value interface{}) error {
		key = convertKey(key) // shallow copy
		if val, err := sp.Ds.GetValue(key); err == nil {
			if valInt, err := util.RepString2Int(val); err == nil {
				sp.mp[valInt] = value
			} else {
				return fmt.Errorf("step[%s] instance-name[%s] with service[%s]: convert long-key[%s] with short-key[%s] to int error[%v]",
					sp.Id, sp.Instance.Addr, sp.Instance.DBType, key, val, err)
			}
		}
		return nil
	}

	// handler each kv
	kv := input.([][]string)
	sp.mp = make(map[int]interface{}) // regenerate every time
	for i, v := range kv[1] {
		if len(v) == 0 {
			continue
		}
		var realValue, valueType = parseValueType(v)
		switch valueType {
		case TYPE_INT:
			fallthrough
		case TYPE_FLOAT:
			save(kv[0][i], realValue)
		case TYPE_STRING:
			break
		default:
			break
		}
	}

	return sp.mp, nil
}

func (sp *StepParse) After(input interface{}, params ...interface{}) (bool, error) {
	return true, nil
}

func convertKey(input string) string {
	if strings.Contains(input, ".") == false {
		return input
	}
	return util.ConvertDot2Underline(input)
}

func parseValueType(value string) (interface{}, int) {
	var err error
	var i64 int64
	var f64 float64
	if i64, err = strconv.ParseInt(value, 10, 64); err == nil {
		return i64, TYPE_INT
	}
	if f64, err = strconv.ParseFloat(value, 64); err == nil {
		return f64, TYPE_FLOAT
	}
	return value, TYPE_STRING
}
