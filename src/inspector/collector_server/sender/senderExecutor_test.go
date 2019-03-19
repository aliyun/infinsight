package sender

import(
	"inspector/heartbeat"
	"fmt"
	"testing"
	"github.com/stretchr/testify/assert"
	"net"
	"github.com/golang/glog"
	"os"
	"inspector/proto/store"
	"inspector/proto/core"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc"
	"golang.org/x/net/context"
	"inspector/collector_server/model"
	"inspector/util"
	"flag"
	"time"
	"bytes"
	"io/ioutil"
	"encoding/json"
	"sync"
	"sync/atomic"
	"math/rand"
)

const (
	serviceName          = "mongo3_4"
	mongoAddress         = "100.81.245.155:20111" // config server
	// mongoAddress         = "10.101.72.137:3001"
	mongoUsername        = "admin"
	mongoPassword        = "admin"
	instanceName         = mongoAddress
	configServerInterval = 1
	heartbeatInterval    = 1
)

var (
	sendFailLocalDirectory = "send_fail"
)

type Parameter struct {
	hb                    *heartbeat.Heartbeat
	hbStoreServer         *heartbeat.Heartbeat
	rpcHandler            *rpcServer
	localAddr             string
	storeServerGrpcServer *grpc.Server
}

func NewParameter(hostName string) (*Parameter, error) {
	// 1. create heartbet
	hbConf := &heartbeat.Conf{
		Module:   heartbeat.ModuleCollector,
		Service:  util.ConvertDot2Underline(hostName),
		Interval: heartbeatInterval,
		Address:  mongoAddress,
		Username: mongoUsername,
		Password: mongoPassword,
	}
	hb := heartbeat.NewHeartbeat(hbConf)
	if hb == nil {
		return nil, fmt.Errorf("create heatbeat error")
	}
	if err := hb.Start(); err != nil {
		return nil, fmt.Errorf("start heart beat error[%v]", err)
	}

	return &Parameter{
		hb: hb,
		rpcHandler: &rpcServer{
			outputChan: make(chan *core.Info, 100),
		},
	}, nil
}

type rpcServer struct {
	outputChan chan *core.Info
}

func (s *rpcServer) Save(ctx context.Context, req *store.StoreSaveRequest) (*store.StoreSaveResponse, error) {
	defer func() {
		s.outputChan<- req.InfoList[0]
	}()

	glog.Info("Save called")

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

// fuck! remove most test case because the code is adjusted to asynchronous
func TestSenderExecutor1(t *testing.T) {
	var (
		err error
		info *core.Info
		nr int
	)
	flag.Set("stderrthreshold", "info")
	flag.Set("v", "2")

	if _, err = os.Stat(sendFailLocalDirectory); !os.IsNotExist(err) {
		err = os.RemoveAll(sendFailLocalDirectory)
		assert.Equal(t, nil, err, "should be equal")
	}

	p, err := NewParameter("1.2.3.4:5678")
	assert.Equal(t, nil, err, "should be equal")

	err = mockStoreServer(p, true, 20023)
	// assert library has bug, some time can't throw out
	fmt.Println(err)
	assert.Equal(t, nil, err, "should be equal")

	senderExecutor := NewExecutor(serviceName, p.hb)
	assert.NotEqual(t, nil, senderExecutor, "should be equal")
	senderMsgChan := senderExecutor.MsgChan

	{
		nr++
		fmt.Printf("TestSenderExecutor1 case %d.\n", nr)
		senderMsgChan<-&model.SenderContext {
			Mp: map[string][]byte{
				"cpu": []byte{0, 0, 0, 0},
				"memory": []byte{0, 0, 0, 20},
			},
			Timestamp: 234,
			Count: 1,
			Step: 1,
			InstanceName: "random1",
		}

		info = <-p.rpcHandler.outputChan
		assert.Equal(t, uint32(1), info.Count, "should be equal")
		assert.Equal(t, uint32(1), info.Step, "should be equal")
		for _, item := range info.Items {
			switch item.Key {
			case "cpu":
				assert.Equal(t, []byte{0, 0, 0, 0}, item.Value, "should be equal")
			case "memory":
				assert.Equal(t, []byte{0, 0, 0, 20}, item.Value, "should be equal")
			default:
				assert.Equal(t, 1, 2, "")
			}
		}
	}

	//{
	//	nr++
	//	fmt.Printf("TestSenderExecutor1 case %d.\n", nr)
	//	// close store server grpc
	//	time.Sleep((sendGrpcTimeout + 1) * time.Second)
	//	p.storeServerGrpcServer.Stop()
	//
	//	// send
	//	senderMsgChan<-&model.SenderContext {
	//		Mp: map[string][]byte{
	//			"cpu": []byte{0, 0, 0, 1},
	//			"memory": []byte{0, 0, 0, 21},
	//		},
	//		Timestamp: 567,
	//		Count: 1,
	//		Step: 1,
	//		InstanceName: "random2",
	//	}
	//
	//	time.Sleep((sendGrpcTimeout + 1) * time.Second)
	//
	//	// read local directory
	//	files, err := ioutil.ReadDir(sendFailLocalDirectory)
	//	assert.Equal(t, nil, err, "should be equal")
	//	assert.Equal(t, 1, len(files), "should be equal")
	//
	//	senderMsgChan<-&model.SenderContext {
	//		Mp: map[string][]byte{
	//			"cpu": []byte{0, 0, 0, 2},
	//			"memory": []byte{0, 0, 0, 22},
	//		},
	//		Timestamp: 789,
	//		Count: 1,
	//		Step: 1,
	//		InstanceName: "random3",
	//	}
	//
	//	time.Sleep((sendGrpcTimeout * 2 + 1) * time.Second)
	//
	//	// read local directory
	//	files, err = ioutil.ReadDir(sendFailLocalDirectory)
	//	assert.Equal(t, nil, err, "should be equal")
	//	assert.Equal(t, 2, len(files), "should be equal")
	//}

	senderExecutor.Close()
	p.hb.Close()
	p.hbStoreServer.Close()
}

// test based on mock channel
func TestSenderExecutor2(t *testing.T) {
	var (
		err error
		nr int
	)
	// flag.Set("stderrthreshold", "info")
	// flag.Set("v", "2")

	r := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	p, err := NewParameter(fmt.Sprintf("6.6.6.6:%d", r.Intn(10000) + 1)) // use random port
	assert.Equal(t, nil, err, "should be equal")

	// send 1 file successfully
	{
		nr++
		fmt.Printf("TestSenderExecutor2 case %d.\n", nr)

		// unit test variables used in senderExecutor
		unitTestSwitch = true
		unitTestChannel = make(chan *store.StoreSaveRequest, 10000)
		unitTestSendFail = false // need send fail?

		senderExecutor := NewExecutor(serviceName, p.hb)
		assert.NotEqual(t, nil, senderExecutor, "should be equal")
		senderMsgChan := senderExecutor.MsgChan

		input := &model.SenderContext {
			Mp: map[string][]byte{
				"cpu": []byte{1, 123, 33, 123, 55, 6},
				"memory": []byte{2},
			},
			Timestamp: 100,
			Count: 70,
			Step: 5,
			InstanceName: "random1",
		}
		senderMsgChan <-input

		time.Sleep(10 * time.Second)
		msg := <-unitTestChannel

		assert.Equal(t, 1, len(msg.InfoList), "should be equal")
		assert.Equal(t, input.Count, msg.InfoList[0].Count, "should be equal")
		assert.Equal(t, input.Step, msg.InfoList[0].Step, "should be equal")
		assert.Equal(t, input.Timestamp, msg.InfoList[0].Timestamp, "should be equal")
		assert.Equal(t, len(input.Mp), len(msg.InfoList[0].Items), "should be equal")
		for _, p := range msg.InfoList[0].Items {
			assert.Equal(t, 0, bytes.Compare(p.Value, input.Mp[p.Key]), "should be equal")
		}

		senderExecutor.Close()
	}

	// send 5,000,000 file successfully
	{
		nr++
		fmt.Printf("TestSenderExecutor2 case %d.\n", nr)

		unitTestSwitch = true
		unitTestChannel = make(chan *store.StoreSaveRequest, 10000)
		unitTestSendFail = false // need send fail?

		senderExecutor := NewExecutor(serviceName, p.hb)
		assert.NotEqual(t, nil, senderExecutor, "should be equal")
		senderMsgChan := senderExecutor.MsgChan

		// read from tunnel
		var j uint32 = 0
		var tot uint32 = 10000000
		var wait sync.WaitGroup
		wait.Add(1)
		go func() { // read in goroutine
			for msg := range unitTestChannel {
				atomic.AddUint32(&j, uint32(len(msg.InfoList)))
				if j >= tot {
					break
				}
			}
			assert.Equal(t, int(j), int(tot), "should be equal")
			wait.Done()
		}()

		// write into tunnel
		for i := 0; uint32(i) < tot; i++ {
			input := &model.SenderContext{
				Mp: map[string][]byte{
					"cpu":    []byte{byte(i % 255), byte((i + 10) % 255), byte((i + 99) % 255)},
					"memory": []byte{byte((i * 2 + 987) % 255)},
				},
				Timestamp:    uint32(i) * 10000,
				Count:        uint32(i),
				Step:         5,
				InstanceName: "random1",
			}

			senderMsgChan <- input
			// fmt.Println("write tunnel ", i)
		}
		fmt.Println("write finish")

		wait.Wait()

		senderExecutor.Close()
	}
}

func TestSenderExecutor3(t *testing.T) {
	var (
		err error
		nr int
	)
	flag.Set("stderrthreshold", "info")
	flag.Set("v", "2")

	p, err := NewParameter("6.6.6.6:67")
	assert.Equal(t, nil, err, "should be equal")

	// send 1 file fail
	{
		nr++
		fmt.Printf("TestSenderExecutor3 case %d.\n", nr)

		if _, err = os.Stat(sendFailLocalDirectory); !os.IsNotExist(err) {
			err = os.RemoveAll(sendFailLocalDirectory)
			assert.Equal(t, nil, err, "should be equal")
		}

		unitTestSwitch = true
		unitTestChannel = make(chan *store.StoreSaveRequest, 10000)
		unitTestSendFail = true // need send fail?

		senderExecutor := NewExecutor(serviceName, p.hb)
		assert.NotEqual(t, nil, senderExecutor, "should be equal")
		senderMsgChan := senderExecutor.MsgChan

		input := &model.SenderContext {
			Mp: map[string][]byte{
				"cpu": []byte{1, 123, 33, 123, 55, 6},
				"memory": []byte{2},
			},
			Timestamp: 200,
			Count: 70,
			Step: 5,
			InstanceName: "random1",
		}
		senderMsgChan <-input

		time.Sleep(10 * time.Second)

		// read file
		var files []os.FileInfo
		var i int
		for ; i < 30; i++ {
			files, err = ioutil.ReadDir(sendFailLocalDirectory)
			if len(files) != 1 {
				time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
			} else {
				break
			}
		}
		assert.Equal(t, true, i < 10, "should be equal")

		fileName := fmt.Sprintf("%s/%s", sendFailLocalDirectory, files[0].Name())
		content, err := ioutil.ReadFile(fileName)
		assert.Equal(t, nil, err, "should be equal")
		msg := new(store.StoreSaveRequest)
		err = json.Unmarshal(content, msg)
		assert.Equal(t, nil, err, "should be equal")

		assert.Equal(t, 1, len(msg.InfoList), "should be equal")
		assert.Equal(t, input.Count, msg.InfoList[0].Count, "should be equal")
		assert.Equal(t, input.Step, msg.InfoList[0].Step, "should be equal")
		assert.Equal(t, input.Timestamp, msg.InfoList[0].Timestamp, "should be equal")
		assert.Equal(t, len(input.Mp), len(msg.InfoList[0].Items), "should be equal")
		for _, p := range msg.InfoList[0].Items {
			assert.Equal(t, 0, bytes.Compare(p.Value, input.Mp[p.Key]), "should be equal")
		}

		senderExecutor.Close()
	}

	var tot uint32 = 10000 // used in the next two cases

	// send 10,000 file fail
	{
		nr++
		fmt.Printf("TestSenderExecutor3 case %d.\n", nr)

		if _, err = os.Stat(sendFailLocalDirectory); !os.IsNotExist(err) {
			err = os.RemoveAll(sendFailLocalDirectory)
			assert.Equal(t, nil, err, "should be equal")
		}

		unitTestSwitch = true
		unitTestChannel = make(chan *store.StoreSaveRequest, 10000)
		unitTestSendFail = true // need send fail?

		senderExecutor := NewExecutor(serviceName, p.hb)
		assert.NotEqual(t, nil, senderExecutor, "should be equal")
		senderMsgChan := senderExecutor.MsgChan

		// write into tunnel
		for i := 0; uint32(i) < tot; i++ {
			input := &model.SenderContext{
				Mp: map[string][]byte{
					"cpu":    []byte{byte(i % 255), byte((i + 10) % 255), byte((i + 99) % 255)},
					"memory": []byte{byte((i * 2 + 987) % 255)},
				},
				Timestamp:    uint32(i) * 10000,
				Count:        uint32(i),
				Step:         5,
				InstanceName: "random1",
			}

			senderMsgChan <- input
			fmt.Println("write tunnel ", i)
		}

		time.Sleep(10 * time.Second)

		// read file
		var files []os.FileInfo
		var i int
		files, err = ioutil.ReadDir(sendFailLocalDirectory)
		for _, file := range files {
			fileName := fmt.Sprintf("%s/%s", sendFailLocalDirectory, file.Name())
			content, err := ioutil.ReadFile(fileName)
			assert.Equal(t, nil, err, "should be equal")
			msg := new(store.StoreSaveRequest)
			err = json.Unmarshal(content, msg)
			assert.Equal(t, nil, err, "should be equal")

			i += len(msg.InfoList)
			if i >= int(tot) {
				break
			}
		}
		assert.Equal(t, int(tot), i, "should be equal")

		senderExecutor.Close()
	}

	// resend previous case failed files successfully
	{
		nr++
		fmt.Printf("TestSenderExecutor3 case %d.\n", nr)

		unitTestSwitch = true
		unitTestChannel = make(chan *store.StoreSaveRequest, 10000)
		unitTestSendFail = false // need send fail?

		senderExecutor := NewExecutor(serviceName, p.hb)
		assert.NotEqual(t, nil, senderExecutor, "should be equal")

		var i uint32
		for msg := range unitTestChannel {
			atomic.AddUint32(&i, uint32(len(msg.InfoList)))
			if i >= tot {
				break
			}
		}
		assert.Equal(t, int(i), int(tot), "should be equal")

		files, err := ioutil.ReadDir(sendFailLocalDirectory)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 0, len(files), "should be equal")
		senderExecutor.Close()
	}

	// resend files fail
	{
		nr++
		fmt.Printf("TestSenderExecutor3 case %d.\n", nr)

		if _, err = os.Stat(sendFailLocalDirectory); !os.IsNotExist(err) {
			err = os.RemoveAll(sendFailLocalDirectory)
			assert.Equal(t, nil, err, "should be equal")
		}

		unitTestSwitch = true
		unitTestChannel = make(chan *store.StoreSaveRequest, 10000)
		unitTestSendFail = true // need send fail?

		senderExecutor := NewExecutor(serviceName, p.hb)
		assert.NotEqual(t, nil, senderExecutor, "should be equal")
		senderMsgChan := senderExecutor.MsgChan

		// write into tunnel
		for i := 0; uint32(i) < tot; i++ {
			input := &model.SenderContext{
				Mp: map[string][]byte{
					"cpu":    []byte{byte(i % 255), byte((i + 10) % 255), byte((i + 99) % 255)},
					"memory": []byte{byte((i * 2 + 987) % 255)},
				},
				Timestamp:    uint32(i) * 10000,
				Count:        uint32(i),
				Step:         5,
				InstanceName: "random1",
			}

			senderMsgChan <- input
			fmt.Println("write tunnel ", i)
		}

		time.Sleep(10 * time.Second)

		// read file
		var files []os.FileInfo
		var i int
		files, err = ioutil.ReadDir(sendFailLocalDirectory)
		for _, file := range files {
			fileName := fmt.Sprintf("%s/%s", sendFailLocalDirectory, file.Name())
			content, err := ioutil.ReadFile(fileName)
			assert.Equal(t, nil, err, "should be equal")
			msg := new(store.StoreSaveRequest)
			err = json.Unmarshal(content, msg)
			assert.Equal(t, nil, err, "should be equal")

			i += len(msg.InfoList)
			if i >= int(tot) {
				break
			}
		}
		assert.Equal(t, int(tot), i, "should be equal")
		senderExecutor.Close()

		// -----------------splitter------------------
		// resend
		unitTestSwitch = true
		unitTestChannel = make(chan *store.StoreSaveRequest, 10000)
		unitTestSendFail = false // need send fail?

		senderExecutor2 := NewExecutor(serviceName, p.hb)
		assert.NotEqual(t, nil, senderExecutor2, "should be equal")

		var j uint32
		for msg := range unitTestChannel {
			atomic.AddUint32(&j, uint32(len(msg.InfoList)))
			if j >= tot {
				break
			}
		}
		assert.Equal(t, int(j), int(tot), "should be equal")

		files, err := ioutil.ReadDir(sendFailLocalDirectory)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 0, len(files), "should be equal")
		senderExecutor2.Close()
	}
}
//
//// only used in real grpc server
//func TestRealGrpc(t *testing.T) {
//	var (
//		err error
//		nr int
//	)
//	flag.Set("stderrthreshold", "info")
//	flag.Set("v", "2")
//
//	p, err := NewParameter(fmt.Sprintf("6.6.6.6:%d", 7777)) // use random port
//	assert.Equal(t, nil, err, "should be equal")
//
//	{
//		nr++
//		fmt.Printf("TestRealGrpc case %d.\n", nr)
//
//		// send to real store_server
//
//		senderExecutor := NewExecutor(serviceName, p.hb)
//		assert.NotEqual(t, nil, senderExecutor, "should be equal")
//		senderMsgChan := senderExecutor.MsgChan
//
//		// read from tunnel
//		var j uint32 = 0
//		var tot uint32 = 10
//		var wait sync.WaitGroup
//		wait.Add(1)
//		go func() { // read in goroutine
//			for msg := range unitTestChannel {
//				atomic.AddUint32(&j, uint32(len(msg.InfoList)))
//				if j >= tot {
//					break
//				}
//			}
//			assert.Equal(t, int(j), int(tot), "should be equal")
//			wait.Done()
//		}()
//
//		// write into tunnel
//		for i := 0; uint32(i) < tot; i++ {
//			input := &model.SenderContext{
//				Mp: map[string][]byte{
//					"cpu":    []byte{byte(i % 255), byte((i + 10) % 255), byte((i + 99) % 255)},
//					"memory": []byte{byte((i * 2 + 987) % 255)},
//				},
//				Timestamp:    uint32(i) * 10000,
//				Count:        uint32(i),
//				Step:         5,
//				InstanceName: "random1",
//			}
//
//			senderMsgChan <- input
//			// fmt.Println("write tunnel ", i)
//		}
//		fmt.Println("write finish")
//
//		wait.Wait()
//
//		senderExecutor.Close()
//	}
//}