/*
// =====================================================================================
//
//       Filename:  serviceInitialize.go
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
	"strings"

	"inspector/api_server/configure"
	"inspector/config"
	"inspector/dict_server"
	"inspector/heartbeat"
	"inspector/util"

	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/glog"
)

/*
// ===  FUNCTION  ======================================================================
//         Name:  startConfigServer
//  Description:
// =====================================================================================
*/
func StartConfigServer() error {
	glog.V(1).Infoln("[Trace][StartConfigServer] start")

	var factory = config.ConfigFactory{Name: config.MongoConfigName}
	var handler config.ConfigInterface
	var err error

	// create config server
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

	// register watcher for refesh dict server
	// when add new server in config server dynamicly
	dictServerWatcher := &config.Watcher{
		Event:   config.NODEALL,
		Handler: refreshDictServer,
	}
	if err := handler.RegisterGlobalWatcher("dict_server", "", dictServerWatcher); err != nil {
		var errStr = fmt.Sprintf("register watcher for dict server error: %s", err.Error())
		glog.Errorf(errStr)
		return errors.New(errStr)
	}

	configure.Options.ConfigServer = handler

	glog.V(1).Infoln("[Trace][StartConfigServer] success")
	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  startHeartBeat
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
		Module:   heartbeat.ModuleApi,
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

	configure.Options.HeartbeatServer = hb

	glog.V(1).Infoln("[Trace][StartHeartBeat] success")
	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  refreshDictServer
//  Description:
// =====================================================================================
*/
func refreshDictServer(event config.WatcheEvent) error {
	glog.V(1).Infoln("[Trace][refreshDictServer] start")

	// get dict name list
	var dictNameList []string
	var err error
	if dictNameList, err = configure.Options.ConfigServer.GetKeyList("dict_server"); err != nil {
		var errStr = fmt.Sprintf("get dict server name list error: %s", err.Error())
		glog.Errorf(errStr)
		return errors.New(errStr)
	}

	// start each dict server
	for _, it := range dictNameList {
		var dictServerConf = dictServer.Conf{
			Address:    configure.Options.ConfigServerAddress,
			Username:   configure.Options.ConfigServerUsername,
			Password:   configure.Options.ConfigServerPassword,
			DB:         configure.Options.ConfigServerDB,
			ServerType: it,
		}
		var dict *dictServer.DictServer
		if dict = dictServer.NewDictServer(&dictServerConf, configure.Options.ConfigServer); dict == nil {
			var errStr = fmt.Sprintf("new dict server[it] error")
			glog.Errorf(errStr)
			return errors.New(errStr)
		}
		if _, ok := configure.Options.DictServerMap.Load(it); !ok {
			configure.Options.DictServerMap.Store(it, dict)
		}
	}

	glog.V(1).Infoln("[Trace][refreshDictServer] success")
	return nil
}
