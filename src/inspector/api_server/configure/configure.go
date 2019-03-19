/*
// =====================================================================================
//
//       Filename:  configure.go
//
//    Description:
//
//        Version:  1.0
//        Created:  06/10/2018 10:18:31 AM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package configure

import (
	"inspector/config"
	"inspector/heartbeat"
	"sync"
)

// =====================================================================================
//       Struct:  ApiServerGlobalConfigure
//  Description:  Api Server 全局配置信息
// =====================================================================================
type ApiServerGlobalConfigure struct {
	ConfigServerAddress  string
	ConfigServerUsername string
	ConfigServerPassword string
	ConfigServerDB       string
	ConfigServerInterval int
	ConfigServer         config.ConfigInterface
	DictServerMap        sync.Map // map[string]*dictServer.DictServer

	HeartbeatInterval int
	HeartbeatServer   *heartbeat.Heartbeat

	ServicePort   int
	SystemProfile int // profiling port

	CollectorTimeout int
	StoreTimeout     int

	LocalConfigCache map[string]map[string]interface{}
}

var Options ApiServerGlobalConfigure
