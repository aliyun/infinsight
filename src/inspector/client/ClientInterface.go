/*
// =====================================================================================
//
//       Filename:  ClientInterface.go
//
//    Description:  client接口
//
//        Version:  1.0
//        Created:  07/05/2018 08:32:04 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package client

import (
	"fmt"
	"time"

	"inspector/util"
)

/*
// ===  INTERFACE  =====================================================================
//         Name:  ClientInterface
//  Description:
// =====================================================================================
*/
type ClientInterface interface {
	// ===  Method  ========================================================================
	// Description: 用于设置参数并创建连接，采用Fluent风格
	//              for example:
	//              var cfg ClientInterface = NewClient()
	//                 .ConnectString("127.0.0.1:8080")
	//                 .Username("admin")
	//                 .Password("admin)
	//                 .EstablishConnect()
	// =====================================================================================
	ConnectString(string) ClientInterface
	Username(string) ClientInterface
	Password(string) ClientInterface
	UseDB(string) ClientInterface
	SetOpt(opts map[string]interface{}) ClientInterface
	EstablishConnect() (ClientInterface, error)
	Close()

	// ===  Method  ========================================================================
	// Description: 直接获取对应client的实际数据结构，可进行灵活的操作
	// =====================================================================================
	GetSession() interface{}
}

// todo, consider remove username and password
func NewClient(tp, address, username, password string) (ClientInterface, error) {
	switch tp {
	case util.Mysql:
		return NewMysqlClient().
			ConnectString(address).
			Username(username).
			Password(password).
			UseDB("mysql").
			SetOpt(map[string]interface{}{"MaxOpenConns": 1}).
			EstablishConnect()
	case util.Redis:
		return NewRedisClient().
			ConnectString(address).
			Username(username).
			Password(password).
			UseDB("0").
			SetOpt(map[string]interface{}{"PoolSize": 0, "AuthCommand": "auth"}).
			EstablishConnect()
	case util.Mongo:
		return NewMongoClient().
			ConnectString(address).
			Username(username).
			Password(password).
			UseDB("admin").
			SetOpt(map[string]interface{}{
				// "SafeMode":        "majority",
				"ConsistencyMode": "Monotonic",
				"PoolLimit":       0,
				"Timeout":         time.Duration(5 * time.Second),
			}).
			EstablishConnect()
	case util.File:
		return NewFileClient().
			ConnectString(address).
			EstablishConnect()
	default:
		return nil, fmt.Errorf("database type[%s] not support", tp)
	}
}
