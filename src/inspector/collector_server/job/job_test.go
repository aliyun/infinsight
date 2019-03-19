package job

import (
	"testing"
	"reflect"
	"fmt"
	"flag"
	"time"
	"net"

	"inspector/util/scheduler"
	"inspector/cache"
	"inspector/config"
	"inspector/dict_server"
	"inspector/heartbeat"
	"inspector/collector_server/connector"
	"inspector/util"
	"inspector/collector_server/sender"
	"inspector/proto/store"
	"inspector/collector_server/model"

	"github.com/stretchr/testify/assert"
	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc"
)

const (
	serviceName          = "mongo3_4"
	hostName             = "127_0_0_1:56780"         // current host address
	// mongoAddress         = "100.81.245.155:20112" // config server
	mongoAddress         = "10.101.72.137:3001" // config server
	mongoUsername        = "admin"
	mongoPassword        = "admin"
	configServerInterval = 5
	heartbeatInterval    = 2
	ringBufferInterval   = 1
	ringBufferCount      = 120 // 120 point

	// monitor db
	monitorAddress  = "100.81.245.155:20111"
	monitorUsername = "admin"
	monitorPassword = "admin"

	instanceName         = monitorAddress

	// copy from collectorServer
	SpecialJobName = "sp_job" // used to watch change and distribute task
	MetaName       = "meta"
	CommandName    = "cmds"
)

type Parameter struct {
	cs        config.ConfigInterface
	ds        *dictServer.DictServer
	hb        *heartbeat.Heartbeat
	connector connector.Connector
	tcb       *scheduler.TCB
	ringCache *cache.RingCache

	// store server
	rpcHandler            *rpcServer
	localAddr             string
	storeServerGrpcServer *grpc.Server
	hbStoreServer         *heartbeat.Heartbeat
}

func NewParameter() (*Parameter, error) {
	// 1. create config server
	factory := config.ConfigFactory{Name: config.MongoConfigName}
	cs, err := factory.Create(mongoAddress, mongoUsername, mongoPassword, "test", configServerInterval*1000)
	if err != nil {
		return nil, err
	}

	// 2. create heartbet
	hbConf := &heartbeat.Conf{
		Module:   heartbeat.ModuleCollector,
		Service:  hostName,
		Interval: heartbeatInterval,
		Address:  mongoAddress,
		Username: mongoUsername,
		Password: mongoPassword,
	}
	hb := heartbeat.NewHeartbeat(hbConf)
	if hb == nil {
		return nil, fmt.Errorf("create heatbeat error")
	}
	if err = hb.Start(); err != nil {
		return nil, fmt.Errorf("start heart beat error[%v]", err)
	}

	// 3. create dictServer
	dictConf := &dictServer.Conf{
		Address:    mongoAddress,
		Username:   mongoUsername,
		Password:   mongoPassword,
		ServerType: serviceName,
	}
	ds := dictServer.NewDictServer(dictConf, cs)
	if ds == nil || reflect.ValueOf(ds).IsNil() == true {
		return nil, fmt.Errorf("create dict server error")
	}

	// 4. create connector

	// 4.3. new connector
	connector := connector.NewConnector(serviceName, mongoAddress, monitorUsername, monitorPassword)
	if connector == nil || reflect.ValueOf(connector).IsNil() {
		return nil, fmt.Errorf("create connector error")
	}

	// 5. create tcb
	tcb := new(scheduler.TCB)
	if err = tcb.Init(instanceName); err != nil {
		return nil, err
	}

	// 6. create ringCache
	ringCache := new(cache.RingCache)
	ringCache.Init(instanceName, ringBufferCount)

	return &Parameter{
		cs:        cs,
		ds:        ds,
		hb:        hb,
		connector: connector,
		tcb:       tcb,
		ringCache: ringCache,
	}, nil
}

// ----------------------- mock store server
type rpcServer struct {
}

func (s *rpcServer) Save(ctx context.Context, req *store.StoreSaveRequest) (*store.StoreSaveResponse, error) {
	glog.Infof("Save called: %v", req.InfoList[0])

	return &store.StoreSaveResponse {
		Error: nil,
	}, nil
}

func (s *rpcServer) Query(ctx context.Context, req *store.StoreQueryRequest) (*store.StoreQueryResponse, error) {
	return nil, nil
}

func grpcServer(p *Parameter, okChan chan struct{}, grpcPort int) {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		panic(fmt.Sprintf("listen error: %v", err))
	}

	s := grpc.NewServer()
	store.RegisterStoreServiceServer(s, p.rpcHandler)
	p.storeServerGrpcServer = s

	reflection.Register(s)
	glog.Info("start grpc")

	okChan<-struct{}{}
	if err := s.Serve(l); err != nil {
		glog.Error(err.Error())
	}
}

func mockStoreServer(p *Parameter, grpcEnable bool, grpcPort int) error {
	ips, err := util.GetAllNetAddr()
	if err != nil {
		return err
	}
	p.localAddr = fmt.Sprintf("%s:%d", util.ConvertDot2Underline(ips[0]), grpcPort)

	hbConf := &heartbeat.Conf{
		Module: heartbeat.ModuleStore,
		Service: p.localAddr,
		Interval: 2,
		Address: mongoAddress,
		Username: mongoUsername,
		Password: mongoPassword,
	}
	hb := heartbeat.NewHeartbeat(hbConf)
	if hb == nil {
		return fmt.Errorf("creat mock store server heartbeat error")
	}
	p.hbStoreServer = hb

	if err := hb.Start(); err != nil {
		return fmt.Errorf("start heart beat error[%v]", err)
	}

	// mock grpc server
	if grpcEnable == true {
		okChan := make(chan struct{})
		go grpcServer(p, okChan, grpcPort)
		<-okChan
	}

	return nil
}
// ----------------------- mock store server end

func TestJob(t *testing.T) {
	var (
		err error
		p   *Parameter
		j   Job
	)
	flag.Set("stderrthreshold", "info")
	flag.Set("v", "2")

	// 1. create parameter used in this test
	p, err = NewParameter()
	assert.Equal(t, nil, err, "should be nil")

	// 2. start store server
	err = mockStoreServer(p, true, 20023)
	// assert library has bug, some time can't throw out
	fmt.Println(err)
	assert.Equal(t, nil, err, "should be equal")

	// 3. create SenderExecutor
	se := sender.NewExecutor(serviceName, p.hb)

	// 4. create job
	j = Create(serviceName, p.tcb, p.connector, p.ringCache, p.cs, p.ds, p.hb,
		&model.Instance{
			Host: instanceName,
			Hid: 123,
			Interval: ringBufferInterval,
		}, se.MsgChan)
	assert.NotEqual(t, nil, j, "shouldn't be nil")

	// 5. equip job
	err = j.Equip(false)
	assert.Equal(t, nil, err, "should be nil")

	// 6. manually call tcb
	var cnt int
	for range time.NewTicker(ringBufferInterval * time.Second).C {
		glog.V(2).Infof("%d second", cnt)
		cnt++
		tcb := j.GetTCB()
		assert.Equal(t, scheduler.READY, tcb.Status(), "should be equal")
		tcb.Process()()
	}
}

// push to real store server exists in the remote
func TestJob2(t *testing.T) {
	var (
		err error
		p   *Parameter
		j   Job
	)
	flag.Set("stderrthreshold", "info")
	flag.Set("v", "2")

	// 1. create parameter used in this test
	p, err = NewParameter()
	assert.Equal(t, nil, err, "should be nil")

	// 2. create SenderExecutor
	se := sender.NewExecutor(serviceName, p.hb)

	// 3. create job
	j = Create(serviceName, p.tcb, p.connector, p.ringCache, p.cs, p.ds, p.hb,
		&model.Instance{
			Host: instanceName,
			Interval: ringBufferInterval,
			Count: 60,
		}, se.MsgChan)
	assert.NotEqual(t, nil, j, "shouldn't be nil")

	// 4. equip job
	err = j.Equip(false)
	assert.Equal(t, nil, err, "should be nil")

	// 5. manually call tcb
	var cnt int
	for range time.NewTicker(ringBufferInterval * time.Second).C {
		glog.V(2).Infof("%d second", cnt)
		cnt++
		tcb := j.GetTCB()
		assert.Equal(t, scheduler.READY, tcb.Status(), "should be equal")
		tcb.Process()()
	}
}

//func TestDebug(t *testing.T) { // todo
//	var (
//		err error
//		p   *Parameter
//		j   Job
//	)
//	flag.Set("stderrthreshold", "info")
//	flag.Set("v", "2")
//
//	// 1. create parameter used in this test
//	p, err = NewParameter()
//	assert.Equal(t, nil, err, "should be nil")
//
//	// 2. create SenderExecutor
//	se := sender.NewExecutor(serviceName, p.hb)
//
//	// 3. create file connector
//	connector :=  connector.NewConnector(p.cs, "file", )
//	if connector == nil || reflect.ValueOf(connector).IsNil() {
//		return nil, fmt.Errorf("create connector error")
//	}
//
//	// 3. create job
//	j = Create(serviceName, p.tcb, p.connector, p.ringCache, p.cs, p.ds, p.hb,
//		&model.Instance{
//			Host: instanceName,
//			Interval: ringBufferInterval,
//		}, se.MsgChan)
//	assert.NotEqual(t, nil, j, "shouldn't be nil")
//
//	// 4. equip job
//	err = j.Equip(true)
//	assert.Equal(t, nil, err, "should be nil")
//
//	// 5. manually call tcb
//	var cnt int
//	for range time.NewTicker(ringBufferInterval * time.Second).C {
//		glog.V(2).Infof("%d second", cnt)
//		cnt++
//		tcb := j.GetTCB()
//		assert.Equal(t, scheduler.READY, tcb.Status(), "should be equal")
//		tcb.Process()()
//	}
//}