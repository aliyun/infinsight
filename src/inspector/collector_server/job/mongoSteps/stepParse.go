package mongoSteps

import (
	"bytes"
	"encoding/binary"
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
	glue   byte = 124 // "|"
	joiner      = "+"
)

var (
	arrayKeys = []string{"stateStr", "self"} // todo, move to flag or config server
)

func NewStepParse(id, serviceName string, instance *model.Instance, parser whatson.Parser,
	ds *dictServer.DictServer) *StepParse {
	return &StepParse{
		Id:          id,
		ServiceName: serviceName,
		Instance:    instance,
		BsonParser:  parser,
		Ds:          ds,
		byteBuffer:  new(bytes.Buffer),
		idx:         0,
		idxPrefix:   0,
	}
}

// parse data
type StepParse struct {
	Id          string                 // id == name
	ServiceName string                 // name: mongo3.4, redis4.0
	Instance    *model.Instance        // ip:port
	errG        error                  // global error
	BsonParser  whatson.Parser         // bson parser
	Ds          *dictServer.DictServer // dict server, not owned

	// below variables are used in callback
	idx         int
	idxPrefix   int                 // prefix of idx, used for nested array
	mp          map[int]interface{} // map
	byteBuffer  *bytes.Buffer       // store prefix key: a|b|c
	arrayKeys   []string            // array key list
	arrayValues []interface{}       // the value match to key
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
 * Input: bson data: [][]byte
 * Output: map int(dict-server) -> value
 * Generally speaking, it maybe better to combine parsing and storing step together to
 * eliminate copy. But I prefer to separate them to make code clean.
 */
func (sp *StepParse) DoStep(input interface{}, params ...interface{}) (interface{}, error) {
	glog.V(2).Infof("step[%s] instance-name[%s] with service[%s] called",
		sp.Id, sp.Instance.Addr, sp.Instance.DBType)

	// update metric
	metric.GetMetric(util.Mongo).AddStepCount(sp.Id)

	// callback
	sp.mp = make(map[int]interface{}) // regenerate every time
	if input == nil {                 // return map to next step if input is nil
		return sp.mp, nil
	}
	sp.idx = 0
	sp.byteBuffer.Truncate(0) // init

	// parse data
	data := input.([][]byte)
	var totLen uint64
	for _, ele := range data {
		le := len(ele)
		if le == 0 { // don't parse when empty
			continue
		}
		totLen += uint64(le)
		if err := sp.BsonParser.Parse(ele, sp.callback); err != nil {
			glog.Errorf("step[%s] instance-name[%s] with service[%s]: parse data error[%v]",
				sp.Id, sp.Instance.Addr, sp.Instance.DBType, err)
			sp.errG = err
			return sp.mp, err // still return empty map if error happens
		}
	}
	metric.GetMetric(util.Mongo).AddBytesGet(totLen) // metric

	// glog.V(2).Infof("Step[%s] instance-name[%s] mp[%v]", sp.Id, sp.InstanceName, mp)
	return sp.mp, nil
}

func (sp *StepParse) After(input interface{}, params ...interface{}) (bool, error) {
	//if sp.errG != nil {
	//	return false, nil
	//}
	return true, nil
}

func (sp *StepParse) callback(keyPath []string, value []byte, valueType whatson.ValueType) error {
	// fmt.Println(keyPath)
	// back tracking
	keyLength := len(keyPath)
	if keyLength == 0 {
		// return immediately if input is []
		return nil
	}
	// fmt.Println("sp.idxPrefix: ", sp.idxPrefix, " idx: ", sp.idx, " keyLength: ", keyLength)
	keyLength += sp.idxPrefix

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
		sp.byteBuffer.WriteString(keyPath[sp.idx-sp.idxPrefix])
	}

	if filterKey(keyPath) {
		return nil
	}

	// if has the same key, the latter will cover the former
	if valueType == whatson.ARRAY {
		// parse array: length1+content1;length2+content2;....
		totLen := len(value)
		for i := 0; i < totLen; {
			length := int(binary.LittleEndian.Uint32(value[i : i+4]))
			tp := value[i+4]
			i += 5
			if i+length > totLen {
				return fmt.Errorf("parse array failed")
			}
			if isDocOrArray(whatson.ValueType(tp)) == false {
				i += length
				continue
			}

			// get keys
			sp.arrayValues = make([]interface{}, len(arrayKeys))
			if err := sp.BsonParser.Parse(value[i:i+length], sp.callbackKeyList); err != nil {
				return err
			}

			// generate key
			generateKey := new(bytes.Buffer)
			for j, v := range sp.arrayValues {
				if j != 0 {
					generateKey.WriteString(joiner)
				}
				if v == nil {
					v = "" // convert to empty string if nil
				}
				generateKey.WriteString(fmt.Sprintf("%v", v))
			}

			// do recurse
			if sp.idx != 0 {
				sp.byteBuffer.WriteByte(glue)
			}
			preIdx := sp.idx
			sp.byteBuffer.WriteString(generateKey.String()) // append generatedKey
			// fmt.Println(sp.byteBuffer.String())
			// fmt.Println("$$$ ", keyLength, )
			sp.idxPrefix += len(keyPath) + 1
			sp.idx++
			if err := sp.BsonParser.Parse(value[i:i+length], sp.callback); err != nil {
				return err
			}

			// back tracking
			util.BackTracking(sp.byteBuffer, sp.idx-preIdx, glue)
			sp.idxPrefix -= len(keyPath) + 1
			sp.idx = preIdx

			// fmt.Println("after: ", sp.byteBuffer.String(), sp.idxPrefix, sp.idx)

			i += length
		}

		return errors.New(whatson.CB_PATH_PRUNE)
	}

	// fmt.Println(keyPath, valueType)
	// continue if type isn't needed or key filtered
	if !neededType(valueType) {
		return nil
	}

	keyByte := sp.byteBuffer.Bytes()
	key := convertKey(keyByte) // shallow copy
	// key := sp.byteBuffer.String()
	if val, err := sp.Ds.GetValue(key); err == nil {
		v := sp.BsonParser.ValueType2Interface(valueType, value)
		if valInt, err := util.RepString2Int(val); err == nil {
			sp.mp[valInt] = v
		} else {
			return fmt.Errorf("step[%s] instance-name[%s] with service[%s]: convert long-key[%s] with short-key[%s] to int error[%v]",
				sp.Id, sp.Instance.Addr, sp.Instance.DBType, key, val, err)
		}
	}
	// do nothing when getValue return error because dict-server will add it later
	return nil
}

func (sp *StepParse) callbackKeyList(keyPath []string, value []byte, valueType whatson.ValueType) error {
	if len(keyPath) > 0 {
		key := keyPath[0]
		for i, match := range arrayKeys {
			if key == match {
				sp.arrayValues[i] = sp.BsonParser.ValueType2Interface(valueType, value)
				break
			}
		}
		return errors.New(whatson.CB_PATH_PRUNE)
	}
	return nil
}

func neededType(valueType whatson.ValueType) bool {
	return valueType == whatson.INTEGER || valueType == whatson.INT64 ||
		valueType == whatson.FLOAT || valueType == whatson.UINT64 || valueType == whatson.BOOL ||
		valueType == whatson.DATETIME || valueType == whatson.TIMESTAMP
}

/*
 * hard code, filter key:
 * 1. key[0][0] == '$'
 */
func filterKey(input []string) bool {
	le := len(input)
	if le == 0 {
		return false
	}

	return len(input[0]) > 0 && input[0][0] == '$'
}

func isDocOrArray(tp whatson.ValueType) bool {
	return tp == whatson.DOCUMENT || tp == whatson.ARRAY
}

func convertKey(input []byte) string {
	// s := string(input)
	s := unsafe.Bytes2String(input)
	if strings.Contains(s, ".") == false {
		return s
	}
	return util.ConvertDot2Underline(s)
}
