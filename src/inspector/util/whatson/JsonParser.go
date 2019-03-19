/*
// =====================================================================================
//
//       Filename:  JsonParse.go
//
//    Description:  JsonParse提供基础Json解析功能，外部需要配合回调函数进行使用
//
//        Version:  1.0
//        Created:  06/23/2018 12:19:31 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package whatson

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"inspector/util/unsafe"
)

type JsonParser struct {
}

func (jp *JsonParser) ValueType2Interface(t ValueType, value []byte) interface{} {
	switch t {
	case FLOAT:
		var output float64
		fmt.Sscanf(string(value), "%f", &output)
		return output
	case INTEGER:
		output, _ := strconv.ParseInt(unsafe.Bytes2String(value), 10, 64)
		return output
	case BOOL:
		return string(value) == "true"
	case STRING:
		return string(value)
	case OBJECT:
		fallthrough
	case ARRAY:
		fallthrough
	case UNKNOWN:
		return value
	case NULL:
		fallthrough
	default:
		return nil
	}
	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  parseKey
//  Description:  解析Key，目前暂不支持解析带有转义字符的key
// =====================================================================================
*/
func (jp *JsonParser) parseKey(jsonData []byte, index int) (string, int, error) {
	// 跳过前双引号
	index++

	idx := bytes.IndexByte(jsonData[index:], byte('"'))
	if idx == -1 {
		return "", -1, errors.New(fmt.Sprintf("invalid json: can't find paired quotes of key at offset[%d]", index))
	}
	key := unsafe.Bytes2String(jsonData[index : index+idx])
	index += idx

	// 跳过后双引号
	index++

	return key, index, nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  parseValue
//  Description:  解析简单Value，目前暂不支持科学记数法和转义字符，也不对数字的合法性进行验证
//                如果遇到OBJECT或ARRAY类型，不解析直接返回
// =====================================================================================
*/
func (jp *JsonParser) parseValue(jsonData []byte, index int) ([]byte, ValueType, int, error) {
	switch jsonData[index] {
	// case STRING
	case '"':
		index++ // 跳过前双引号
		idx := bytes.IndexByte(jsonData[index:], byte('"'))
		if idx == -1 {
			return nil, UNKNOWN, -1, errors.New(fmt.Sprintf("invalid json: can't find paired quotes of value at offset[%d]", index))
		}
		value := jsonData[index : index+idx]
		index += idx
		index++ // 跳过后双引号

		return value, STRING, index, nil

	// case INTEGER or FLOAT
	case '-':
		fallthrough
	case '0':
		fallthrough
	case '1':
		fallthrough
	case '2':
		fallthrough
	case '3':
		fallthrough
	case '4':
		fallthrough
	case '5':
		fallthrough
	case '6':
		fallthrough
	case '7':
		fallthrough
	case '8':
		fallthrough
	case '9':
		var value []byte
		valueType := INTEGER
		head := index
		index++ // 跳过首字符
		for index < len(jsonData) {
			if jsonData[index] >= '0' && jsonData[index] <= '9' {
				index++
			} else if jsonData[index] == '.' {
				valueType = FLOAT
				index++
			} else {
				value = jsonData[head:index]
				break
			}
		}
		return value, valueType, index, nil

	// case OBJECT
	case '{':
		return nil, OBJECT, index, nil
	// case ARRAY
	case '[':
		return nil, ARRAY, index, nil
	// case BOOL
	case 't':
		if index+4 > len(jsonData) ||
			bytes.Compare(jsonData[index:index+4], []byte{'t', 'r', 'u', 'e'}) != 0 {
			return nil, UNKNOWN, index, errors.New(fmt.Sprintf("invalid json: expect bool[true] at offset[%d]", index))
		}
		return jsonData[index : index+4], BOOL, index + 4, nil
	case 'f':
		if index+5 > len(jsonData) ||
			bytes.Compare(jsonData[index:index+5], []byte{'f', 'a', 'l', 's', 'e'}) != 0 {
			return nil, UNKNOWN, index, errors.New(fmt.Sprintf("invalid json: expect bool[false] at offset[%d]", index))
		}
		return jsonData[index : index+5], BOOL, index + 5, nil
	// case NULL
	case 'n':
		if index+4 > len(jsonData) ||
			bytes.Compare(jsonData[index:index+4], []byte{'n', 'u', 'l', 'l'}) != 0 {
			return nil, UNKNOWN, index, errors.New(fmt.Sprintf("invalid json: expect null at offset[%d]", index))
		}
		return jsonData[index : index+4], NULL, index + 4, nil
	default:
		return nil, UNKNOWN, index, errors.New(fmt.Sprintf("invalid json: unknown value type at offset[%d]", index))
	}
	panic("invalid value type")
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  doParseObjectRecursion
//  Description:  递归解析Object，并返回"}"下一个字符的绝对下标位置
// =====================================================================================
*/
func (jp *JsonParser) doParseObjectRecursion(jsonData []byte, index int, parentKey []string, callback ParseCallBack) (int, error) {
	var key string
	var value []byte
	var valueType ValueType
	var keyPath []string
	var keyIndex int
	var err error

	// 跳过左空白符，并判断是否已经结束
	leftTrim := func() error {
		for index < len(jsonData) {
			if unicode.IsSpace(rune(jsonData[index])) {
				index++
			} else {
				break
			}
		}
		if index >= len(jsonData) {
			return errors.New(fmt.Sprint("invalid json: unexpect end"))
		}
		return nil
	}

	keyIndex = len(parentKey)
	keyPath = append(parentKey, "")

	if jsonData[index] == '{' {
		index++
		if err = leftTrim(); err != nil {
			return index, errors.New(fmt.Sprintf("%s: expect valid key or \"}\" at offset[%d]", err.Error(), index))
		}
		if jsonData[index] == '}' {
			return index + 1, nil
		}
		for index < len(jsonData) {
			// find key
			if err = leftTrim(); err != nil {
				return index, errors.New(fmt.Sprintf("%s: expect valid key or \"}\" at offset[%d]", err.Error(), index))
			}
			if jsonData[index] == '"' {
				var idx int

				// parse key
				if key, index, err = jp.parseKey(jsonData, index); err != nil {
					return index, err
				}
				keyPath[keyIndex] = key

				// parse value
				if err = leftTrim(); err != nil {
					return index, errors.New(fmt.Sprintf("%s: expect \":\" at offset[%d]", err.Error(), index))
				}
				if jsonData[index] == ':' {
					index++
				} else {
					return index, errors.New(fmt.Sprintf("invalid json: expect \":\" at offset[%d]", index))
				}
				if err = leftTrim(); err != nil {
					return index, errors.New(fmt.Sprintf("%s: expect value at offset[%d]", err.Error(), index))
				}
				if value, valueType, index, err = jp.parseValue(jsonData, index); err != nil {
					return index, err
				}
				switch valueType {
				case STRING:
					fallthrough
				case INTEGER:
					fallthrough
				case FLOAT:
					fallthrough
				case BOOL:
					fallthrough
				case NULL:
					if err = callback(keyPath, value, valueType); err != nil {
						return index, err
					}
				case OBJECT:
					// 递归之前先做一次回调，但由于value尚未解析完成，所以返回nil
					if err = callback(keyPath, nil, valueType); err != nil {
						return index, err
					}
					// 递归解析子Object
					if idx, err = jp.doParseObjectRecursion(jsonData, index, keyPath, callback); err != nil {
						return index, err
					}
					value = jsonData[index:idx]
					// 递归之后再做一次回调，此时value已经解析完成，明确的知道value的内容
					if err = callback(keyPath, value, valueType); err != nil {
						return index, err
					}
					index = idx
				case ARRAY:
					// 递归之前先做一次回调，但由于value尚未解析完成，所以返回nil
					err = callback(keyPath, nil, valueType)
					if err != nil {
						// 处理剪枝
						if err.Error() == CB_PATH_PRUNE {
							idx = index
							var pair int = 0
							if jsonData[idx] != '[' {
								panic("expact '[' here")
							}
							pair = 1
							idx++
							for idx < len(jsonData) {
								if jsonData[idx] == '[' {
									pair++
									idx++
								} else if jsonData[idx] == ']' {
									pair--
									idx++
								} else {
									idx++
								}
								if pair == 0 {
									break
								}
							}
						} else {
							return index, err
						}
					} else {
						// 递归解析子Array
						if idx, err = jp.doParseArrayRecursion(jsonData, index, keyPath, callback); err != nil {
							return index, err
						}
						// 递归之后再做一次回调，此时value已经解析完成，明确的知道value的内容
						value = jsonData[index:idx]
						if err = callback(keyPath, value, valueType); err != nil {
							return index, err
						}
					}
					index = idx
				case UNKNOWN:
					return index, errors.New(fmt.Sprintf("invalid json: unknown value type at offset[%d]", index))
				default:
					panic("invalid value type")
				}

				// find next
				if err = leftTrim(); err != nil {
					return index, errors.New(fmt.Sprintf("%s: expect \",\" or \"}\" at offset[%d]", err.Error(), index))
				}
				if jsonData[index] == ',' {
					index++
				} else if jsonData[index] == '}' {
					index++
					break
				} else {
					return index, errors.New(fmt.Sprintf("invalid json: expect \",\" or \"}\" at offset[%d]", index))
				}
			} else {
				return index, errors.New(fmt.Sprintf("invalid json: expect valid key at offset[%d]", index))
			}
		}
		if index >= len(jsonData) && jsonData[len(jsonData)-1] != '}' {
			return index, errors.New(fmt.Sprintf("invalid json: expect valid key or \"}\" at offset[%d]", index))
		}
	} else {
		return index, errors.New(fmt.Sprintf("invalid json: expect \"{\" at offset[%d]", index))
	}
	return index, nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  doParseArrayRecursion
//  Description:  递归解析Array，并返回"}"下一个字符的绝对下标位置
// =====================================================================================
*/
func (jp *JsonParser) doParseArrayRecursion(jsonData []byte, index int, parentKey []string, callback ParseCallBack) (int, error) {
	var key string
	var value []byte
	var valueType ValueType
	var keyPath []string
	var keyIndex int
	var buf strings.Builder
	var err error

	// 跳过左空白符
	leftTrim := func() error {
		for index < len(jsonData) {
			if unicode.IsSpace(rune(jsonData[index])) {
				index++
			} else {
				break
			}
		}
		if index >= len(jsonData) {
			return errors.New(fmt.Sprint("invalid json: unexpect end"))
		}
		return nil
	}

	keyIndex = len(parentKey)
	keyPath = append(parentKey, "")
	buf.Grow(8) // set cap

	if jsonData[index] == '[' {
		index++
		var count int = 0

		if err = leftTrim(); err != nil {
			return index, errors.New(fmt.Sprintf("%s: expect valid value of \"]\" at offset[%d]", err.Error(), index))
		}
		if jsonData[index] == ']' {
			return index + 1, nil
		}

		// parse array
		for index < len(jsonData) {
			var idx int

			// create key
			buf.Reset()
			buf.WriteRune('[')
			buf.WriteString(strconv.Itoa(count))
			buf.WriteRune(']')
			key = buf.String()
			keyPath[keyIndex] = key

			// parse value
			if err = leftTrim(); err != nil {
				return index, errors.New(fmt.Sprintf("%s: expect valid value at offset[%d]", err.Error(), index))
			}
			if value, valueType, index, err = jp.parseValue(jsonData, index); err != nil {
				return index, err
			}
			switch valueType {
			case STRING:
				fallthrough
			case INTEGER:
				fallthrough
			case FLOAT:
				fallthrough
			case BOOL:
				fallthrough
			case NULL:
				if err = callback(keyPath, value, valueType); err != nil {
					return index, err
				}
			case OBJECT:
				// 递归之前先做一次回调，但由于value尚未解析完成，所以返回nil
				if err = callback(keyPath, nil, valueType); err != nil {
					return index, err
				}
				// 递归解析子Object
				if idx, err = jp.doParseObjectRecursion(jsonData, index, keyPath, callback); err != nil {
					return index, err
				}
				value = jsonData[index:idx]
				// 递归之后再做一次回调，此时value已经解析完成，明确的知道value的内容
				if err = callback(keyPath, value, valueType); err != nil {
					return index, err
				}
				index = idx
			case ARRAY:
				// 递归之前先做一次回调，但由于value尚未解析完成，所以返回nil
				if err = callback(keyPath, nil, valueType); err != nil {
					return index, err
				}
				// 递归解析子Array
				if idx, err = jp.doParseArrayRecursion(jsonData, index, keyPath, callback); err != nil {
					return index, err
				}
				// 递归之后再做一次回调，此时value已经解析完成，明确的知道value的内容
				value = jsonData[index:idx]
				if err = callback(keyPath, value, valueType); err != nil {
					return index, err
				}
				index = idx
			case UNKNOWN:
				return index, errors.New(fmt.Sprintf("invalid json: unknown value type at offset[%d]", index))
			default:
				panic("invalid value type")
			}

			// find next
			if err = leftTrim(); err != nil {
				return index, errors.New(fmt.Sprintf("%s: expect \",\" or \"]\" at offset[%d]", err.Error(), index))
			}
			if jsonData[index] == ',' {
				index++
				count++
			} else if jsonData[index] == ']' {
				index++
				break
			} else {
				return index, errors.New(fmt.Sprintf("invalid json: expect \",\" or \"]\" at offset[%d]", index))
			}
		}
		if index >= len(jsonData) && jsonData[len(jsonData)-1] != ']' {
			return index, errors.New(fmt.Sprintf("invalid json: expect valid value at offset[%d]", index))
		}
	} else {
		return index, errors.New(fmt.Sprintf("invalid json: expect \"[\" at offset[%d]", index))
	}
	return index, nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  JsonParse
//  Description:  Json解析器，每解析到一个key（无论是否是叶子节点）都会触发回调函数
// =====================================================================================
*/
func (jp *JsonParser) Parse(jsonData []byte, callback ParseCallBack) error {
	var index int = 0
	var err error
	var path []string

	// 跳过左空白符
	leftTrim := func() {
		for index < len(jsonData) {
			if unicode.IsSpace(rune(jsonData[index])) {
				index++
			} else {
				break
			}
		}
	}

	leftTrim()
	if index < len(jsonData) {
		path = make([]string, 0)
		switch jsonData[index] {
		case '{':
			index, err = jp.doParseObjectRecursion(jsonData, index, path, callback)
		case '[':
			index, err = jp.doParseArrayRecursion(jsonData, index, path, callback)
		default:
			return errors.New(fmt.Sprintf("invalid json: expect \"{\" or \"[\" at offset[%d]", index))
		}

		if err != nil {
			return err
		}

		leftTrim()
		if index != len(jsonData) {
			return errors.New(fmt.Sprintf("json parse ok, but data not finish, invalid data at offset[%d]", index))
		}

		return nil
	} else {
		return errors.New(fmt.Sprint("invalid json: data is empty"))
	}
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  JsonGet
//  Description:  根据json path语法，获取指定path的值
// =====================================================================================
*/
func (jp *JsonParser) Get(jsonData []byte, path ...string) ([]byte, error) {
	var result []byte
	var err error
	if len(path) == 0 {
		return jsonData, nil
	}
	err = jp.Parse(jsonData, func(keyPath []string, value []byte, valueType ValueType) error {
		if len(path) != len(keyPath) {
			return nil
		}
		for i, _ := range keyPath {
			if path[i] != keyPath[i] {
				return nil
			}
		}
		if value == nil {
			return nil
		}
		result = value
		return errors.New("found")
	})
	if err == nil {
		return nil, errors.New("path not found")
	}
	if err.Error() == "found" {
		return result, nil
	}
	return result, err
}
