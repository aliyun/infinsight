/*
// =====================================================================================
//
//       Filename:  main.go
//
//    Description:
//
//        Version:  1.0
//        Created:  08/22/2018 16:55:41 AM
//       Revision:  none
//       Compiler:  go1.10.1
//
//         Author:  zhuzhao.cx, zhuzhao.cx@alibaba-inc.com
//        Company:  Alibaba Group
//
// =====================================================================================
*/

//go:generate protoc -I ../../ --go_out=plugins=grpc:../../ inspector/proto/core/core.proto
//go:generate protoc -I ../../ --go_out=plugins=grpc:../../ inspector/proto/collector/collector.proto
//go:generate protoc -I ../../ --go_out=plugins=grpc:../../ inspector/proto/store/store.proto

package main

import (
	"flag"
	"fmt"
	_ "net/http/pprof"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"inspector/collector_server/collector_manager"
	"inspector/collector_server/configure"
	"inspector/collector_server/restful"
	"inspector/config"
	"inspector/heartbeat"
	"inspector/util"

	"github.com/golang/glog"
	"github.com/gugemichael/nimo4go"
)

func main() {
	defer glog.Flush()
	defer util.Goodbye()
	defer handleExit()

	runtime.GOMAXPROCS(runtime.NumCPU() * 2)

	// 1. get input parameters
	flag.StringVar(&conf.Options.ConfigServerAddress, "config_address", "127.0.0.1:27017/admin", "config server address")
	flag.StringVar(&conf.Options.ConfigServerUsername, "config_username", "", "config server username")
	flag.StringVar(&conf.Options.ConfigServerPassword, "config_password", "", "config server password")
	flag.StringVar(&conf.Options.ConfigServerDB, "config_db", "MonitorConfig", "config server db")
	flag.IntVar(&conf.Options.ConfigServerInterval, "cs_interval", 1, "config auto update interval")
	flag.IntVar(&conf.Options.HeartbeatInterval, "hb_interval", 1, "heartbeat auto update interval")
	flag.IntVar(&conf.Options.CollectorServerPort, "port", 6300, "port of collector server")
	flag.IntVar(&conf.Options.SystemProfile, "profiling_port", 9300, "http profiling port")
	flag.IntVar(&conf.Options.MonitorPort, "monitor_port", 7300, "http monitor listen port")
	flag.StringVar(&conf.Options.WorkPath, "work_path", "./", "work path")

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

	var (
		err error
		cs  config.ConfigInterface // config server
		hb  *heartbeat.Conf        // heartbeat conf
	)

	// 2. sanitize options
	if err = sanitizeOptions(); err != nil {
		crash(fmt.Sprintf("Conf.Options check failed: %s", err.Error()), -2)
	}
	glog.Infoln("configuration: ", conf.Options)

	util.InitHttpApi(conf.Options.MonitorPort)

	// 3. init variables
	if cs, hb, err = initVariables(); err != nil {
		crash(fmt.Sprintf("init variables failed: %s", err.Error()), -3)
	}

	// 4. start collector manager
	cm := collectorManager.NewCollectorManager(cs, hb)
	go func() {
		if err = cm.Run(); err != nil { // won't exit unless meets error
			crash(err.Error(), -5)
		}
	}()

	// 5. start http server
	util.Welcome()
	startHttpServer()

	glog.Error("I'm unreachable!")
}

func sanitizeOptions() error {
	if conf.Options.ConfigServerAddress == "" {
		return fmt.Errorf("config server address shouldn't be emty")
	}
	if conf.Options.ConfigServerInterval <= 0 {
		return fmt.Errorf("config server interval[%d] shouldn't <= 0 here", conf.Options.ConfigServerInterval)
	}
	if conf.Options.HeartbeatInterval <= 0 {
		return fmt.Errorf("heartbeat interval[%d] shouldn't <= 0", conf.Options.HeartbeatInterval)
	}
	if conf.Options.CollectorServerPort <= 0 {
		return fmt.Errorf("collector server port[%d] shouldn't <= 0", conf.Options.CollectorServerPort)
	}
	if conf.Options.SystemProfile <= 0 {
		return fmt.Errorf("collector profiling port[%d] shouldn't <= 0", conf.Options.SystemProfile)
	}

	// get local ip and generate collector server address(ip:port)
	if ips, err := util.GetAllNetAddr(); err != nil {
		return fmt.Errorf("get local ip address error[%v]", err)
	} else {
		service := fmt.Sprintf("%s:%d", ips[0], conf.Options.CollectorServerPort)
		conf.Options.CollectorServerAddress = util.ConvertDot2Underline(service) // convert dot to underline
	}

	conf.Options.WorkPathSendFail = filepath.Join(conf.Options.WorkPath, conf.SendFailDirectory)

	return nil
}

func initVariables() (config.ConfigInterface, *heartbeat.Conf, error) {
	// create config server
	factory := config.ConfigFactory{Name: config.MongoConfigName}
	// watcher interval must > 0 which also means enable
	cs, err := factory.Create(
		conf.Options.ConfigServerAddress,
		conf.Options.ConfigServerUsername,
		conf.Options.ConfigServerPassword,
		conf.Options.ConfigServerDB,
		conf.Options.ConfigServerInterval*1000)
	if err != nil {
		return nil, nil, fmt.Errorf("create config server error[%v]", err)
	}

	hb := &heartbeat.Conf{
		Module:   heartbeat.ModuleCollector,
		Service:  conf.Options.CollectorServerAddress,
		Interval: conf.Options.HeartbeatInterval,
		Address:  conf.Options.ConfigServerAddress,
		Username: conf.Options.ConfigServerUsername,
		Password: conf.Options.ConfigServerPassword,
		DB:       conf.Options.ConfigServerDB,
	}

	return cs, hb, nil
}

func startHttpServer() {
	// profiling
	nimo.Profiling(conf.Options.SystemProfile)
	nimo.RegisterSignalForProfiling(syscall.SIGUSR2)
	nimo.RegisterSignalForPrintStack(syscall.SIGUSR1, func(bytes []byte) {
		glog.Info(string(bytes))
	})

	// restful
	util.HttpApi.RegisterAPI("/conf", nimo.HttpGet, func([]byte) interface{} { // register conf
		return &conf.Options
	})
	restful.RestAPI() // register the others

	if err := util.HttpApi.Listen(); err != nil {
		crash(fmt.Sprintf("start http listen error[%v]", err), -4)
	}
}

func crash(msg string, errCode int) {
	panic(msg)
}

func handleExit() {
	if e := recover(); e != nil {
		glog.Errorf(fmt.Sprintf("%v", e))
		util.PrintStack()
	}
}
