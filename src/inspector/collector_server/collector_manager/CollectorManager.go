/*
// =====================================================================================
//
//       Filename:  CollectorManager.go
//
//    Description:  任务管理模块，负责任务的划分，更新，装配与资源管理
//
//        Version:  1.0
//        Created:  08/21/2018 19:06:37 PM
//       Compiler:  go1.10.1
//
// =====================================================================================
*/

package collectorManager

import (
	"fmt"
	"inspector/collector_server/model"
	"inspector/config"
	"inspector/dict_server"
	"inspector/heartbeat"
	"inspector/proto/collector"
	"inspector/proto/core"
	"inspector/util"
	"inspector/util/scheduler"
	"net"
	"reflect"
	"sync"

	"github.com/golang/glog"
	"github.com/gugemichael/nimo4go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"inspector/collector_server/metric"
)

type CollectorManager struct {
	Cs     config.ConfigInterface // config server
	HbConf *heartbeat.Conf        // heartbeat configuration
	Hb     *heartbeat.Heartbeat   // heartbeat

	schedulers   *sync.Map   // scheduler list, key: time interval, value: scheduler // previous: map[int]*scheduler.Scheduler
	jobs         *sync.Map   // job list, key: meta-type(mongo-3.4, mysql-1.0), value: job // previous: map[string]*GeneralJob
	spJob        *SpecialJob // special job
	dictServerMp *sync.Map   // dict server map, job -> dict server
	grpcServer   *GrpcServer // grpc server
}

func NewCollectorManager(cs config.ConfigInterface, heartbeatConf *heartbeat.Conf) *CollectorManager {
	cm := &CollectorManager{
		Cs:           cs,
		HbConf:       heartbeatConf,
		schedulers:   new(sync.Map), // make(map[int]*scheduler.Scheduler),
		jobs:         new(sync.Map), // make(map[string]*GeneralJob),
		dictServerMp: new(sync.Map),
	}
	cm.grpcServer = NewGrpcServer(cm)
	cm.RestAPI() // enable restful
	return cm
}

func (cm *CollectorManager) Run() error {
	// 1. register heartbeat
	cm.Hb = heartbeat.NewHeartbeat(cm.HbConf)
	if cm.Hb == nil {
		return fmt.Errorf("create heart beat error")
	}

	if err := cm.Hb.Start(); err != nil {
		return fmt.Errorf("start heart beat error[%v]", err)
	}

	// 2. start special job
	cm.spJob = NewSpecialJob(cm)
	if err := cm.spJob.Start(); err != nil {
		return fmt.Errorf("start special job error[%v]", err)
	}

	// 3. start grpc
	if err := cm.startGrpcServer(cm.HbConf.Service); err != nil {
		return err
	}
	return nil
}

func (cm *CollectorManager) Close() {
	cm.spJob.Stop()
	cm.Hb.Close()
	cm.Cs.Close()
}

func (cm *CollectorManager) RestAPI() {
	type Info struct {
		Queue map[string]interface{} `json:"queue"`
	}

	// sender queue size
	util.HttpApi.RegisterAPI("/queue", nimo.HttpGet, func([]byte) interface{} {
		mp := make(map[string]interface{}, 3)
		cm.jobs.Range(func(key, val interface{}) bool {
			job := val.(*GeneralJob)
			mp[key.(string)] = job.SenderExecutor.QueueStatus()
			return true
		})

		return &Info{
			Queue: mp,
		}
	})
}

func (cm *CollectorManager) CreateTask(jobName string, ins *model.Instance) error {
	glog.Infof("CreateTask: interval[%d], jobName[%s], ins[%v]", ins.Interval, jobName, *ins)

	schd, ok := cm.schedulers.Load(ins.Interval)
	if !ok {
		newSchd := new(scheduler.Scheduler)
		name := fmt.Sprintf("scheduler_%d_second", ins.Interval)
		if err := newSchd.Init(name, ins.Interval*1000); err != nil { // pass s -> ms
			return fmt.Errorf("scheduler init name[%s] error[%v]", name, err)
		}
		cm.schedulers.Store(ins.Interval, newSchd)
		schd = newSchd
		// create metric
	}

	// judge and create dict server
	if _, ok := cm.dictServerMp.Load(jobName); !ok {
		dictConf := &dictServer.Conf{
			Address:    cm.HbConf.Address,
			Username:   cm.HbConf.Username,
			Password:   cm.HbConf.Password,
			DB:         cm.HbConf.DB,
			ServerType: jobName,
		}
		ds := dictServer.NewDictServer(dictConf, cm.Cs)
		if ds == nil || reflect.ValueOf(ds).IsNil() == true {
			return fmt.Errorf("create dict server error")
		}
		cm.dictServerMp.Store(jobName, ds)
	}
	ds, _ := cm.dictServerMp.Load(jobName)

	job, ok := cm.jobs.Load(jobName)
	if !ok {
		newJob := NewGeneralJob(jobName, ins.Interval, schd.(*scheduler.Scheduler), cm,
			ds.(*dictServer.DictServer), cm.Hb, cm.Cs)
		if newJob == nil {
			return fmt.Errorf("create GeneralJob error")
		}
		cm.jobs.Store(jobName, newJob)
		newJob.Start() // start in goroutine
		job = newJob
		metric.CreateMetric(jobName)
	}

	// for step bench mark only
	//for i := 0; i < 80; i++ {
	//	if ins.Host == "11_163_186_75:3032" && i != 0 {
	//		continue
	//	}
	//	if err := job.(*GeneralJob).AddInstance(ins); err != nil {
	//		return err
	//	}
	//}

	if err := job.(*GeneralJob).AddInstance(ins); err != nil {
		return err
	}
	return nil
}

func (cm *CollectorManager) RemoveTask(interval int, jobName string, taskAddress string) error {
	glog.Infof("RemoveTask: interval[%d], jobName[%s], taskAddress[%s]", interval, jobName, taskAddress)
	// return nil // for debug sp job

	if _, ok := cm.schedulers.Load(interval); !ok {
		return nil
	}

	job, ok := cm.jobs.Load(jobName)
	if !ok {
		return nil
	}

	return job.(*GeneralJob).RemoveInstance(taskAddress)
}

// query data from cache(ring cache), return map, key -> timestamp -> data
func (cm *CollectorManager) QueryData(query *core.Query) (*core.InfoRange, string) {
	glog.Infof("CollectorManager: QueryData %v\n", *query)

	service := query.Header.Service
	gj, ok := cm.jobs.Load(service)
	if !ok {
		return nil, "service not exist in collector manager list"
	}

	info, errMsg := gj.(*GeneralJob).QueryData(query)
	if errMsg != emptyString {
		glog.Errorf("CollectorManager: Query %s error[%s]", *query, errMsg)
	}
	return info, errMsg
}

func (cm *CollectorManager) startGrpcServer(address string) error {
	// start tcp listener
	l, err := net.Listen("tcp", util.ConvertUnderline2Dot(address))
	if err != nil {
		return fmt.Errorf("start grpc server: listen error[%v]", err.Error())
	}

	s := grpc.NewServer()
	collector.RegisterCollectorServer(s, cm.grpcServer)

	reflection.Register(s)
	if err := s.Serve(l); err != nil { // keep running until fail
		return err
	}
	return nil
}
