/*
// =====================================================================================
//
//       Filename:  configure.go
//
//    Description:
//
//        Version:  1.0
//        Created:  06/10/2018 10:23:48 AM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package configure

import (
	"inspector/cache"
	"inspector/client"
	"inspector/config"
	"inspector/util/pool"

	"github.com/vinllen/mgo"
)

// =====================================================================================
//       Struct:  StoreServerGlobalConfigure
//  Description:  Store Server 基础元数据
// =====================================================================================
type StoreServerGlobalConfigure struct {
	ConfigServerAddress  string
	ConfigServerUsername string
	ConfigServerPassword string
	ConfigServerDB       string
	ConfigServerInterval int
	HeartbeatInterval    int
	ConfigServer         config.ConfigInterface

	StoreServerAddress  string
	StoreServerUsername string
	StoreServerPassword string
	StoreServerDB       string
	StorageClient       client.ClientInterface
	StoreReadTimeout    int
	StoreWriteTimeout   int

	MongoQuerySession          *mgo.Session
	MongoStoreSessionListCount int
	MongoStoreSessionList      []*mgo.Session
	MongoStorePool             *pool.GoroutinePool

	ServicePort   int
	MonitorPort   int
	SystemProfile int // profiling port

	CacheConcurrence int
	CacheDataReserve int
	TimeCache        *cache.TimeCache
}

var Options StoreServerGlobalConfigure
