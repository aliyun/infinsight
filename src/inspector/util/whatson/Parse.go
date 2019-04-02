/*
// =====================================================================================
//
//       Filename:  Parse.go
//
//    Description:  Parse提供基础Json、Bson、KV解析功能，格式统一，外部需要配合回调函数进行使用
//
//        Version:  1.0
//        Created:  07/05/2018 17:41:31 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package whatson

import (
	"github.com/golang/glog"
)

type ValueType byte

// all data type, no need to including all of the them in every parse method
const (
	STRING ValueType = iota
	INTEGER
	FLOAT
	BOOL
	NULL
	OBJECT
	ARRAY
	UNKNOWN
	DOCUMENT
	BINARY
	UINT64
	INT64
	DECIMAL128
	DATETIME
	TIMESTAMP

	CB_PATH_FOUND    string = "path found"
	CB_PATH_NOTFOUND string = "path not found"
	CB_PARSE_ERRROR  string = "parse error"
	CB_PATH_PRUNE    string = "path prune" // no need to continue current branch

	Bson = "bson"
	Json = "json"
)

/*
// ===  FUNCTION  ======================================================================
//         Name:  ParseCallBack
//  Description:  解析的回调函数（顶层元素parentKey为空字符串）
// =====================================================================================
*/
type ParseCallBack func(keyPath []string, value []byte, valueType ValueType) error

type Parser interface {
	/*
	 * Iterate all the tree and call callback function for every node it traverse.
	 * It'll return immediate when meets error.
	 */
	Parse(data []byte, callback ParseCallBack) error

	/*
	 * Get the value of given path.
	 * Return value and error if has.
	 */
	Get(data []byte, path ...string) ([]byte, error)

	/*
	 * Convert valueType to interface{}
	 */
	ValueType2Interface(t ValueType, value []byte) interface{}
}

func NewParser(name string) Parser {
	switch name {
	case Json:
		return &JsonParser{}
	case Bson:
		return &BsonParser{}
	default:
		glog.Errorf("parser's name[%s] can't be recognized", name)
		return nil
	}
}
