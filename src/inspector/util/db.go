package util

import (
	"fmt"
	"reflect"
	"strings"
)

// todo, move all database type to here
const (
	// DataBases
	Mysql = "mysql"
	Redis = "redis"
	Mongo = "mongodb"

	// Common
	HttpJson = "http_json"
	File     = "file"

	Unknown = "unknown"

	// the string start with this character will only be used inner
	InnerLeadingMark = '~'

	// Minute = 60 // 60 points per minute

	MetaCollection           = "meta"
	TaskListCollection       = "taskList"
	TaskDistributeCollection = "taskDistribute"

	TaskDistributeName = "distribute"
	Md5Name            = string(InnerLeadingMark) + "key_md5"
	CommandName        = "cmds"
	IntervalName       = "interval"
	CountName          = "count"
	MetaSelectorName   = "selector"
	MetaTargetName     = "target"
	MetaBaseName       = "base"
)

// input maybe redis4.0, mongo3.4 and so on
func GetDbType(input string) string {
	if strings.HasPrefix(input, Mysql) {
		return Mysql
	}
	if strings.HasPrefix(input, Redis) {
		return Redis
	}
	if strings.HasPrefix(input, Mongo) {
		return Mongo
	}
	if strings.HasPrefix(input, HttpJson) {
		return HttpJson
	}
	return Unknown
}

func ConvertDot2Underline(input string) string {
	return strings.Replace(input, ".", "_", -1)
}

func ConvertUnderline2Dot(input string) string {
	return strings.Replace(input, "_", ".", -1)
}

// convert interface to int type
func ConvertInterface2Int(input interface{}) (int, error) {
	switch src := input.(type) {
	case int64:
		return int(src), nil
	case uint64:
		return int(src), nil
	case int32:
		return int(src), nil
	case uint32:
		return int(src), nil
	case int16:
		return int(src), nil
	case uint16:
		return int(src), nil
	case int8:
		return int(src), nil
	case uint8:
		return int(src), nil
	case int:
		return int(src), nil
	case uint:
		return int(src), nil
	case float32:
		return int(src), nil
	case float64:
		return int(src), nil
	default:
		return 0, fmt.Errorf("unsupport type[%v]", reflect.TypeOf(input))
	}
}

// filter key
func FilterName(key string) bool {
	// both these fields shouldn't be added
	return len(key) == 0 || key[0] == InnerLeadingMark
}
