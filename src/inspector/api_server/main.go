/*
// =====================================================================================
//
//       Filename:  main.go
//
//    Description:
//
//        Version:  1.0
//        Created:  06/10/2018 10:18:31 AM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package main

import (
	"flag"
	"fmt"
	"inspector/api_server/configure"
	"inspector/api_server/handler"
	"inspector/api_server/initialize"
	"inspector/util"
	"net/http"
	_ "net/http/pprof"
	"strings"

	"github.com/golang/glog"
	"github.com/gugemichael/nimo4go"
)

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

	flag.IntVar(&configure.Options.ServicePort, "port", 6100, "port of api server")
	flag.IntVar(&configure.Options.SystemProfile, "profiling_port", 9100, "http profiling port")

	flag.IntVar(&configure.Options.CollectorTimeout, "collector_timeout", 3, "timeout of query from collector")
	flag.IntVar(&configure.Options.StoreTimeout, "store_timeout", 5, "timeout of query from store")

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

	// service initialize
	var err error
	if err = initialize.StartConfigServer(); err != nil {
		panic(err)
	}
	if err = initialize.StartHeartBeat(); err != nil {
		panic(err)
	}
	// if err = initialize.StartDictServer(); err != nil {
	// 	panic(err)
	// }

	// profiling
	nimo.Profiling(configure.Options.SystemProfile)

	// http.HandleFunc("/", handler.IndexHandler)
	// http.HandleFunc("/api/v1/query_range", handler.QueryRangeHandler)
	// http.HandleFunc("/api/v1/label/__name__/values", handler.SuggestionHandler)
	// http.HandleFunc("/api/v1/series", handler.SeriesHandler)

	http.HandleFunc("/", handler.IndexHandler)
	http.HandleFunc("/api/v1/query_range", func(w http.ResponseWriter, r *http.Request) {
		new(handler.ApiHandler).QueryRangeHandler(w, r)
	})
	http.HandleFunc("/api/v1/label/__name__/values", func(w http.ResponseWriter, r *http.Request) {
		new(handler.ApiHandler).SuggestionHandler(w, r)
	})
	http.HandleFunc("/api/v1/series", func(w http.ResponseWriter, r *http.Request) {
		new(handler.ApiHandler).SeriesHandler(w, r)
	})

	http.ListenAndServe(fmt.Sprintf(":%d", configure.Options.ServicePort), nil)
}

func handleExit() {
	if e := recover(); e != nil {
		glog.Errorf(fmt.Sprintf("%v", e))
		util.PrintStack()
	}
}
