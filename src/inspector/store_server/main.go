/*
// =====================================================================================
//
//       Filename:  main.go
//
//    Description:
//
//        Version:  1.0
//        Created:  06/10/2018 10:23:48 AM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

//go:generate protoc -I ../../ --go_out=plugins=grpc:../../ inspector/proto/core/core.proto
//go:generate protoc -I ../../ --go_out=plugins=grpc:../../ inspector/proto/store/store.proto

package main

import (
	"flag"
	"fmt"
	"inspector/store_server/configure"
	"inspector/store_server/initialize"
	"inspector/util"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"syscall"

	"github.com/golang/glog"
	"github.com/gugemichael/nimo4go"
)

/*
// ===  FUNCTION  ======================================================================
//         Name:  indexHandler
//  Description:  handler for monitor
// =====================================================================================
*/
func indexHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "inspector store is running")
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  main
//  Description:
// =====================================================================================
*/
func main() {
	defer glog.Flush()
	defer util.Goodbye()
	defer handleExit()

	// parse params
	flag.StringVar(&configure.Options.ConfigServerAddress, "config_address", "127.0.0.1:27017/admin", "config server address")
	flag.StringVar(&configure.Options.ConfigServerUsername, "config_username", "", "config server username")
	flag.StringVar(&configure.Options.ConfigServerPassword, "config_password", "", "config server password")
	flag.StringVar(&configure.Options.ConfigServerDB, "config_db", "MonitorConfig", "config server db")
	flag.IntVar(&configure.Options.ConfigServerInterval, "cs_interval", 1, "config auto update interval")
	flag.IntVar(&configure.Options.HeartbeatInterval, "hb_interval", 1, "heartbeat auto update interval")

	flag.StringVar(&configure.Options.StoreServerAddress, "store_address", "127.0.0.1:27017/admin", "store server address")
	flag.StringVar(&configure.Options.StoreServerUsername, "store_username", "", "store server username")
	flag.StringVar(&configure.Options.StoreServerPassword, "store_password", "", "store server password")
	flag.StringVar(&configure.Options.StoreServerDB, "store_db", "MonitorData", "store server db")
	flag.IntVar(&configure.Options.StoreReadTimeout, "store_read_timeout", 3, "read timeout for save to mongodb")
	flag.IntVar(&configure.Options.StoreWriteTimeout, "store_write_timeout", 5, "write timeout for save to mongodb")

	flag.IntVar(&configure.Options.ServicePort, "port", 6200, "port of store server")
	flag.IntVar(&configure.Options.SystemProfile, "profiling_port", 9200, "http profiling port")
	flag.IntVar(&configure.Options.CacheConcurrence, "concurrence", 1, "concurrence of cache")
	flag.IntVar(&configure.Options.CacheDataReserve, "reserve", 3600, "data reserve")

	flag.IntVar(&configure.Options.MongoStoreSessionListCount, "session_count", 10, "mongo session count")

	flag.IntVar(&configure.Options.MonitorPort, "monitor_port", 9096, "monitor port of store server")

	var version bool
	flag.BoolVar(&version, "version", false, "show version")

	flag.Parse()

	// 显示编译信息
	if version {
		var l = strings.Split(util.VERSION, ";")
		for _, it := range l {
			fmt.Println(strings.Replace(it, ":", ":\t", 1))
		}
		return
	}

	glog.V(2).Infoln("[Trace][configure.Options]: ", configure.Options)

	// profiling
	nimo.Profiling(configure.Options.SystemProfile)
	nimo.RegisterSignalForProfiling(syscall.SIGUSR2)
	nimo.RegisterSignalForPrintStack(syscall.SIGUSR1, func(bytes []byte) {
		glog.Info(string(bytes))
	})

	// service initialize
	var err error
	if err = initialize.StartConfigServer(); err != nil {
		panic(err)
	}
	if err = initialize.StartHeartBeat(); err != nil {
		panic(err)
	}
	if err = initialize.StartRPCServer(); err != nil {
		panic(err)
	}

	// http server
	http.HandleFunc("/", indexHandler)
	http.ListenAndServe(fmt.Sprintf(":%d", configure.Options.MonitorPort), nil)
}

func handleExit() {
	if e := recover(); e != nil {
		glog.Errorf(fmt.Sprintf("%v", e))
		util.PrintStack()
	}
}
