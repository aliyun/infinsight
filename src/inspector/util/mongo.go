/*
// =====================================================================================
//
//       Filename:  mongo.go
//
//    Description:  基本函数用于mongodb相关处理
//
//        Version:  1.0
//        Created:  08/07/2018 15:14:56 PM
//       Compiler:  go1.10.1
//
// =====================================================================================
*/

package util

import (
	"inspector/util/whatson"

	"github.com/vinllen/mgo"
)

func IsNotFound(err error) bool {
	return err.Error() == whatson.CB_PATH_NOTFOUND || err.Error() == mgo.ErrNotFound.Error()
}
