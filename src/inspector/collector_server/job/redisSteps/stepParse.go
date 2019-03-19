package redisSteps

import (
	"bytes"
	"fmt"
	"strconv"
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
)

const (
	TYPE_INT = iota
	TYPE_FLOAT
	TYPE_STRING
	TYPE_SUBSTRUCTURE
	TYPE_UNKNOWN
)

var (
	expandItems = map[string]struct{}{
		"usage_cpu":      {},
		"usage_cpu_sys":  {},
		"usage_cpu_user": {},
	}
)

func NewStepParse(id, serviceName string, instance *model.Instance, ds *dictServer.DictServer) *StepParse {
	return &StepParse{
		Id:          id,
		ServiceName: serviceName,
		Instance:    instance,
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

	infos := input.([][]byte)
	sp.mp = make(map[int]interface{}) // regenerate every time
	for _, info := range infos {
		if len(info) == 0 {
			glog.Errorf("input info data is empty")
			return sp.mp, nil
		}

		metric.GetMetric(sp.ServiceName).AddBytesGet(uint64(len(info))) // metric

		// handle bytes data
		var infoStr = unsafe.Bytes2String(info)
		var index = 0
		for index < len(infoStr) {
			var t = strings.IndexByte(infoStr[index:], '\n')
			if t < 0 {
				break
			}

			// handle each line
			var line = infoStr[index : index+t]
			if len(line) <= 1 || line[0] == '#' { // just '\n' or comment
				index += t + 1
				continue
			}
			line = util.StringTrim(line)
			sp.byteBuffer.Truncate(0) // init

			// get key
			var k = strings.IndexByte(line, ':')
			var key = line[:k]

			// get value
			var value = line[k+1:]

			// parse value type
			var realValue interface{}
			var valueType int
			realValue, valueType = parseValueType(value)
			switch valueType {
			case TYPE_INT:
				fallthrough
			case TYPE_FLOAT:
				save(key, realValue)
			case TYPE_SUBSTRUCTURE:
				sp.byteBuffer.WriteString(key)
				sp.byteBuffer.WriteByte(glue)
				for _, it := range realValue.([]string) {
					// get subkey
					var sindex = strings.IndexByte(it, '=')
					var skey = it[:sindex]
					// get subvalue
					var svalue = it[sindex+1:]
					var sreal, stype = parseValueType(svalue)
					// handler sub structure
					switch stype {
					case TYPE_INT:
						fallthrough
					case TYPE_FLOAT:
						sp.byteBuffer.WriteString(skey)
						save(sp.byteBuffer.String(), sreal)
						sp.byteBuffer.Truncate(len(key) + 1) // reset
						break
					case TYPE_SUBSTRUCTURE:
						break
					case TYPE_STRING:
						break
					default:
						break
					}
				}
			case TYPE_STRING:
				break
			default:
				break
			}

			index += t + 1
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
	var subItems []string
	if i64, err = strconv.ParseInt(value, 10, 64); err == nil {
		return i64, TYPE_INT
	}
	if f64, err = strconv.ParseFloat(value, 64); err == nil {
		return f64, TYPE_FLOAT
	}
	if i := strings.IndexByte(value, ','); i != -1 {
		subItems = strings.Split(value, ",")
		return subItems, TYPE_SUBSTRUCTURE
	}
	return value, TYPE_STRING
}
