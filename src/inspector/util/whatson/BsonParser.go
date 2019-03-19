/*
// =====================================================================================
//
//       Filename:  BsonParser.go
//
//    Description:  BsonParser提供基础Bson解析功能，格式统一，外部需要配合回调函数进行使用
//
//        Version:  1.0
//        Created:  07/05/2018 19:50:11 PM
//       Revision:  none
//       Compiler:  go1.10.3
//
//         Author:  zhuzhao.cx, zhuzhao.cx@alibaba-inc.com
//        Company:  Alibaba Group
//
// =====================================================================================
*/

package whatson

import(
	"errors"
	"encoding/binary"
	"fmt"
	"strings"
	"math"

	"inspector/util/unsafe"
)

const (
	ROOT_KEY = "ROOT-KEY" // useless now
)

type BsonParser struct {
}

// convert bson type to ValueType
func (bp *BsonParser) mongoType2ValueType(t byte) ValueType {
	switch t {
	case 1:
		return FLOAT
	case 2:
		return STRING
	case 3:
		return DOCUMENT
	case 4:
		return ARRAY
	case 5:
		return BINARY
	case 6:
		return UNKNOWN
	case 7:
		return OBJECT
	case 8:
		return BOOL
	case 9: // datetime
		return DATETIME
	case 10:
		return NULL
	case 11: // regular expression - not supported
		fallthrough
	case 12: // DBPointer - deprecated
		fallthrough
	case 13: // JavaScript code - not supported
		fallthrough
	case 14: // Symbol - deprecated
		fallthrough
	case 15: // JavaScript code w/ scope - not supported
		return UNKNOWN
	case 16:
		return INTEGER
	case 17:
		return TIMESTAMP
	case 18:
		return INT64
	case 19:
		return DECIMAL128
	case 255: // "\xFF" Min key - not supported
		fallthrough
	case 127: // "\x7F" Max key - not supported
		return UNKNOWN
	default:
		return UNKNOWN
	}
}

func (bp *BsonParser) ValueType2Interface(t ValueType, value []byte) interface{} {
	switch t {
	case FLOAT:
		return math.Float64frombits(binary.LittleEndian.Uint64(value))
	case STRING:
		return string(value)
	case BOOL:
		return value[0] == 1
	case DATETIME:
		fallthrough
	case INT64:
		//var res int64
		// binary.Read(bytes.NewBuffer(value), binary.LittleEndian, &res)
		// return res
		return int64(binary.LittleEndian.Uint64(value))
	case TIMESTAMP:
		return uint32(binary.LittleEndian.Uint64(value) >> 32)
	case UINT64:
		return binary.LittleEndian.Uint64(value)
	case INTEGER:
		//var res int32
		//binary.Read(bytes.NewBuffer(value), binary.LittleEndian, &res)
		//return res
		return int32(binary.LittleEndian.Uint32(value))
	case DOCUMENT:
		return map[string]interface{}{}
	case ARRAY:
		return []string{}
	case OBJECT:
		fallthrough
	case BINARY:
		fallthrough
	case UNKNOWN:
		return value
	case NULL:
		return nil
	default:
		return nil
	}
}

// find the end of string in bson
func (bp *BsonParser) cstringEnd(data []byte, idx int) int {
	for ; idx < len(data); idx++ {
		if data[idx] == 0 {
			return idx
		}
	}
	// return 0, fmt.Errorf("string exceeds the data boundary")
	return -1
}

func (bp *BsonParser) parseType(data []byte, begIndex int, valueType ValueType) (
		retIdx int, result []byte) {
	switch valueType {
	case FLOAT:
		result = data[begIndex: begIndex + 8]
		retIdx = begIndex + 8
	case STRING:
		totLen := int(binary.LittleEndian.Uint32(data[begIndex: begIndex + 4]))
		result = data[begIndex + 4: begIndex + 4 + totLen - 1]
		retIdx = begIndex + 4 + totLen
	case OBJECT:
		result = data[begIndex: begIndex + 12]
		retIdx = begIndex + 12
	case BOOL:
		result = data[begIndex: begIndex + 1]
		retIdx = begIndex + 1
	case BINARY:
		totLen := int(binary.LittleEndian.Uint32(data[begIndex: begIndex + 4]))
		// data[4] is subtype which is useless here
		result = data[begIndex + 5: begIndex + 5 + totLen]
		retIdx = begIndex + 5 + totLen
	case INTEGER:
		result = data[begIndex: begIndex + 4]
		retIdx = begIndex + 4
	case DATETIME:
		fallthrough
	case INT64:
		result = data[begIndex: begIndex + 8]
		retIdx = begIndex + 8
	case TIMESTAMP:
		fallthrough
	case UINT64:
		result = data[begIndex: begIndex + 8]
		retIdx = begIndex + 8
	case DECIMAL128:
		result = data[begIndex: begIndex + 16]
		retIdx = begIndex + 16
	case DOCUMENT:
		fallthrough
	case ARRAY:
		totLen := int(binary.LittleEndian.Uint32(data[begIndex: begIndex + 4]))
		result = data[begIndex: begIndex + totLen]
		retIdx = begIndex + totLen
	default:

	}
	return retIdx, result
}

// get all the array element
func (bp *BsonParser) parseArray(data []byte, begIndex, endIndex int) (content []byte, err error) {
	var element []byte
	var idxEnd int
	for i := begIndex + 4; i < endIndex - 1; {
		// parse type
		t := data[i]
		i++

		// parse key which is type cstring
		if idxEnd = bp.cstringEnd(data, i); idxEnd == -1 {
			return nil, fmt.Errorf("invalid cstring")
		}

		tp := bp.mongoType2ValueType(t)
		i, element = bp.parseType(data, idxEnd + 1, tp)
		length := make([]byte, 4)
		binary.LittleEndian.PutUint32(length, uint32(len(element)))
		content = append(content, length...)  // body length
		content = append(content, byte(tp))   // type
		content = append(content, element...) // body
	}
	return content, nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  dfs
//  Description:  采用深度搜索方式遍历整棵bson树
//   @Parameter:  data: 传输的数组
//                callback: 回调函数，每个结点都会触发
//                callback: 回调函数，每个结点都会触发
//                begIndex: 当次遍历data数组的起始坐标
//                valueType: 当次遍历的结点类型
//      @return:  int: 返回本次迭代最终位置
//                error: 错误原因
// =====================================================================================
*/
// todo, 考虑优化，用byte.Buffer代替parentKey []string减少拷贝
func (bp *BsonParser) dfs(data []byte, parentKey []string, callback ParseCallBack,
		begIndex int, valueType ValueType) (retIdx int, err error) {
	var content []byte
S:
	switch valueType {
	case FLOAT:
		fallthrough
	case STRING:
		fallthrough
	case OBJECT:
		fallthrough
	case BOOL:
		fallthrough
	case BINARY:
		fallthrough
	case INTEGER:
		fallthrough
	case INT64:
		fallthrough
	case UINT64:
		fallthrough
	case DATETIME:
		fallthrough
	case TIMESTAMP:
		fallthrough
	case DECIMAL128:
		retIdx, content = bp.parseType(data, begIndex, valueType)
		if err = callback(parentKey, content, valueType); err != nil && err.Error() == CB_PATH_PRUNE {
			err = nil
		}
	case DOCUMENT:
		totLen := int(binary.LittleEndian.Uint32(data[begIndex: begIndex + 4]))
		endIndex := begIndex + totLen
		// fmt.Println(begIndex, endIndex, totLen, len(data))
		if err = callback(parentKey, data[begIndex: endIndex], DOCUMENT); err != nil {
			if err.Error() == CB_PATH_PRUNE { // branch prune
				return endIndex, nil
			}
			break S
		}
		for i := begIndex + 4; i < endIndex - 1; {
			// parse type
			t := data[i]
			i++

			// parse key which is type cstring
			idxEnd := bp.cstringEnd(data, i)
			if idxEnd == -1 {
				break S
			}

			// pay more attention, key is shallow copy
			key := unsafe.Bytes2String(data[i: idxEnd])
			// key := string(data[i: idxEnd])
			parentKey = append(parentKey, key)

			i, err = bp.dfs(data, parentKey, callback, idxEnd + 1,
				bp.mongoType2ValueType(t))

			// backtracking
			parentKey = parentKey[: len(parentKey) - 1]

			if err != nil {
				retIdx = 0
				break S
			}
		}
		// document end with 0
		retIdx = endIndex

	case ARRAY:
		totLen := int(binary.LittleEndian.Uint32(data[begIndex: begIndex + 4]))
		endIndex := begIndex + totLen
		// to make it more clear, I separate the parseArray for-loop and dfs for-loop
		content, err = bp.parseArray(data, begIndex, endIndex)
		if err != nil {
			break S
		}
		if err = callback(parentKey, content, ARRAY); err != nil {
			if err.Error() == CB_PATH_PRUNE { // branch prune
				return endIndex, nil
			}
			break S
		}
		for i := begIndex + 4; i < endIndex - 1; {
			// parse type
			t := data[i]
			i++

			// parse key which is type cstring
			idxEnd := bp.cstringEnd(data, i)
			if idxEnd == -1 {
				break S
			}

			key := fmt.Sprintf("[%s]", unsafe.Bytes2String(data[i: idxEnd]))
			parentKey = append(parentKey, key)

			i, err = bp.dfs(data, parentKey, callback, idxEnd + 1,
				bp.mongoType2ValueType(t))

			// backtracking
			parentKey = parentKey[: len(parentKey) - 1]

			if err != nil {
				retIdx = 0
				break S
			}
		}
		// document end with 0
		retIdx = endIndex
	default:
		retIdx, err = 0, fmt.Errorf("type[%d] not supported", valueType)
	}

	return retIdx, err
}

func (bp *BsonParser) Parse(data []byte, callback ParseCallBack) error {
	_, err := bp.dfs(data, []string{}, callback, 0, DOCUMENT)
	return err
}

func (bp *BsonParser) Get(data []byte, path... string) ([]byte, error) {
	var result []byte // little-endian default
	if len(path) == 0 {
		return data, nil
	}

	idx := 0
	match := false
	path = append([]string{}, path...)
	err := bp.Parse(data, func(keyPath []string, value []byte, valueType ValueType) error {
		// fmt.Printf("keyPath: %v, path: %v\n", keyPath, path)
		if match {
			return nil
		}
		depthKey := len(keyPath)
		if idx == depthKey - 1 &&
			strings.Compare(keyPath[idx], path[idx]) == 0 {
			idx++
			if idx == len(path) {
				result = value
				match = true
				return errors.New(CB_PATH_FOUND)
			}
		}
		return nil
	})

	if err == nil {
		return nil, errors.New(CB_PATH_NOTFOUND)
	} else if err.Error() == CB_PATH_FOUND {
		return result, nil
	}
	return result, err
}
