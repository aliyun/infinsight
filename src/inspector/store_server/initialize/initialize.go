/*
// =====================================================================================
//
//       Filename:  initialize.go
//
//    Description:
//
//        Version:  1.0
//        Created:  09/06/2018 06:08:52 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package initialize

import (
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"strings"
	"time"

	"inspector/cache"
	"inspector/client"
	"inspector/config"
	"inspector/heartbeat"
	"inspector/proto/store"
	"inspector/store_server/configure"
	"inspector/store_server/handler"
	"inspector/util"
	"inspector/util/pool"

	"github.com/golang/glog"
	"github.com/vinllen/mgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

/*
// ===  FUNCTION  ======================================================================
//         Name:  StartConfigServer
//  Description:
// =====================================================================================
*/
func StartConfigServer() error {
	glog.V(1).Infoln("[Trace][StartConfigServer] start")

	var factory = config.ConfigFactory{Name: config.MongoConfigName}
	var handler config.ConfigInterface
	var err error
	if handler, err = factory.Create(
		configure.Options.ConfigServerAddress,
		configure.Options.ConfigServerUsername,
		configure.Options.ConfigServerPassword,
		configure.Options.ConfigServerDB,
		configure.Options.ConfigServerInterval*1000 /* ms */); err != nil {
		var errStr = fmt.Sprintf("create config server error: %s", err.Error())
		glog.Errorf(errStr)
		return errors.New(errStr)
	}
	configure.Options.ConfigServer = handler

	glog.V(1).Infoln("[Trace][StartConfigServer] success")
	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  StartHeartBeat
//  Description:
// =====================================================================================
*/
func StartHeartBeat() error {
	glog.V(1).Infoln("[Trace][StartHeartBeat] start")

	var ips []string
	var err error
	if ips, err = util.GetAllNetAddr(); err != nil {
		var errStr = "get ip from system error"
		glog.Errorf(errStr)
		return errors.New(errStr)
	}
	var ip = strings.Replace(ips[0], ".", "_", -1)
	var hb = heartbeat.NewHeartbeat(&heartbeat.Conf{
		Module:   heartbeat.ModuleStore,
		Service:  fmt.Sprintf("%s:%d", ip, configure.Options.ServicePort),
		Interval: configure.Options.HeartbeatInterval, /* second */
		Address:  configure.Options.ConfigServerAddress,
		Username: configure.Options.ConfigServerUsername,
		Password: configure.Options.ConfigServerPassword,
		DB:       configure.Options.ConfigServerDB,
	})
	if hb == nil {
		var errStr = "create heart beat error"
		glog.Errorf(errStr)
		return errors.New(errStr)
	}

	if err := hb.Start(); err != nil {
		var errStr = fmt.Sprintf("start heart beat error[%s]", err.Error())
		glog.Errorf(errStr)
		return errors.New(errStr)
	}

	glog.V(1).Infoln("[Trace][StartHeartBeat] success")
	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  InitPersistentStorage
//  Description:
// =====================================================================================
*/
func InitPersistentStorage() client.ClientInterface {
	glog.V(1).Infoln("[Trace][InitPersistentStorage] start")

	// split connection string
	var connList = strings.Split(configure.Options.StoreServerAddress, ",")

	// init store session list
	for i := 0; i < configure.Options.MongoStoreSessionListCount; i++ {
		// establish connection
		newClient, err := client.NewMongoClient().
			ConnectString(connList[i%len(connList)]).
			Username(configure.Options.StoreServerUsername).
			Password(configure.Options.StoreServerPassword).
			UseDB(configure.Options.StoreServerDB).
			SetOpt(map[string]interface{}{
				"SafeMode":        "majority",
				"ConsistencyMode": "Monotonic",
				"Timeout":         time.Duration(configure.Options.StoreWriteTimeout) * time.Second,
			}).
			EstablishConnect()

		if err != nil {
			glog.Errorf("new mongo client error: ", err)
			return nil
		}

		session := newClient.GetSession()
		if session == nil || reflect.ValueOf(session).IsNil() {
			glog.Errorf("mongo session is nil: ", err)
			return nil
		}
		if mongoSession, ok := session.(*mgo.Session); !ok {
			glog.Errorf("get mongo session error: ", err)
			return nil
		} else {
			configure.Options.MongoStoreSessionList = append(configure.Options.MongoStoreSessionList, mongoSession)
		}
	}
	glog.V(1).Infoln("[Trace][InitPersistentStorage] create store session list success")

	// init store pool
	configure.Options.MongoStorePool = new(pool.GoroutinePool)
	configure.Options.MongoStorePool.Init()
	go configure.Options.MongoStorePool.Run()
	glog.V(1).Infoln("[Trace][InitPersistentStorage] create store pool success")

	// init query session list
	newClient, err := client.NewMongoClient().
		ConnectString(configure.Options.StoreServerAddress).
		Username(configure.Options.StoreServerUsername).
		Password(configure.Options.StoreServerPassword).
		UseDB(configure.Options.StoreServerDB).
		SetOpt(map[string]interface{}{
			"SafeMode":        "majority",
			"ConsistencyMode": "Monotonic",
			"Timeout":         time.Duration(configure.Options.StoreReadTimeout) * time.Second,
		}).
		EstablishConnect()

	if err != nil {
		glog.Errorf("new mongo client error: ", err)
		return nil
	}

	session := newClient.GetSession()
	if session == nil || reflect.ValueOf(session).IsNil() {
		glog.Errorf("mongo session is nil: ", err)
		return nil
	}
	if mongoSession, ok := session.(*mgo.Session); !ok {
		glog.Errorf("get mongo session error: ", err)
		return nil
	} else {
		configure.Options.MongoQuerySession = mongoSession
	}
	glog.V(1).Infoln("[Trace][InitPersistentStorage] create query session list success")

	return newClient
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  StartRPCServer
//  Description:
// =====================================================================================
*/
func StartRPCServer() error {
	glog.V(1).Infoln("[Trace][StartRPCServer] start")

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", configure.Options.ServicePort))
	if err != nil {
		var errStr = fmt.Sprintf("listen error: ", err.Error())
		glog.Error(errStr)
		return errors.New(errStr)
	}

	// init cache
	if configure.Options.TimeCache = cache.NewTimeCache(uint32(configure.Options.CacheConcurrence), uint32(configure.Options.CacheDataReserve)); configure.Options.TimeCache == nil {
		var errStr = "new TimeCache error"
		glog.Error(errStr)
		return errors.New(errStr)
	}

	// new mongo client for persistent store
	if configure.Options.StorageClient = InitPersistentStorage(); configure.Options.StorageClient == nil {
		var errStr = "initialize persistent storage error"
		glog.Error(errStr)
		return errors.New(errStr)
	}

	s := grpc.NewServer(grpc.MaxRecvMsgSize(64 * 1024 * 1024))
	store.RegisterStoreServiceServer(s, &handler.RpcServer{})

	reflection.Register(s)
	go func() {
		if err := s.Serve(l); err != nil {
			var errStr = fmt.Sprintf("start rpc server error: %s", err.Error())
			glog.Error(errStr)
			os.Exit(127)
		}
	}()

	glog.V(1).Infoln("[Trace][StartRPCServer] success")
	return nil
}
