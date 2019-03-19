package sender

import (
	"context"
	"fmt"
	"time"
	"io/ioutil"
	"os"
	"encoding/json"
	"sync/atomic"

	"inspector/collector_server/model"
	"inspector/proto/store"
	"inspector/proto/core"
	"inspector/heartbeat"
	"inspector/util/grpc2"
	"inspector/util"
	"inspector/collector_server/configure"
	"inspector/collector_server/metric"

	"github.com/golang/glog"
)

const (
	executorChanSize        = 8192
	sendGrpcTimeout         = 5  // seconds
	checkConnectionInterval = 20 // seconds

	batchThreshold       = 1024               // the max batch number
	senderChanSize       = batchThreshold * 8 // 2048
	maxFilenameSuffix    = 1 << 20
	resendFileReadLimit  = 20 // re-send file maximum number every time
	resendFileWriteLimit = 50 // at most {resendFileWriteLimit} files will be stored

	senderQueue   = "SenderQueue"
	executorQueue = "ExecutorQueue"
)

var (
	suffix           uint32                       // filename suffix to prevent duplicate naming

	// below variables are only used in unit test
	unitTestSwitch   bool                         // unit test switch open?
	unitTestSendFail bool                         // need send fail?
	unitTestChannel  chan *store.StoreSaveRequest // only open for unit test
)

// grpc client
type sender struct {
	client  *grpc2.Connection // grpc connection
	msgChan chan *core.Info   // send context info
}

func newSender(address string) *sender {
	// create new connection
	client := grpc2.NewConnection(address)

	if client.EnsureNetwork() == false {
		glog.Errorf("ensureNetwork store_server[%s] fail", address)
		client.Close()
		return nil
	}

	s := &sender{
		client:  client,
		msgChan: make(chan *core.Info, senderChanSize),
	}
	go s.Run() // run as goroutine
	return s
}

// read until pass the batchThreshold
func (s *sender) readBatch(batch []*core.Info) []*core.Info {
	for len(batch) < batchThreshold {
		select {
		case msg, ok := <-s.msgChan:
			if !ok {
				return batch // still send because data exist in the channel
			}
			batch = append(batch, msg)
		case <-time.After(1 * time.Second):
			// wait 1 second
			return batch
		}
	}
	return batch
}

// sender main function
func (s *sender) Run() {
	for {
		msg, ok := <-s.msgChan
		if !ok {
			glog.Infof("Sender connecting to store_server address[%s] closes", s.client.Addr)
			return
		}
		batch := make([]*core.Info, 0, batchThreshold)
		batch = append(batch, msg)

		batch = s.readBatch(batch)

		glog.Infof("Sender: send with batch size: %d", len(batch))

		// generate request
		request := &store.StoreSaveRequest{
			InfoList: batch,
		}
		// t := time.Now()
		if err := s.send(request); err != nil {
			metric.GetMetric(util.Mongo).AddBytesSendClient(s.client.Addr, uint64(len(batch)))
			glog.Error(err)
		} else {
			glog.Infof("Sender: send to address[%s] successfully", s.client.Addr)
		}
		// fmt.Println("ssss ", time.Since(t))
	}

	glog.Info("Sender: I'm unreachable")
}

func (s *sender) send(request *store.StoreSaveRequest) error {
	// do send
	if unitTestSwitch { // for unit test only
		glog.Infof("unit test open, send request to channel")
		if unitTestSendFail { // mock send fail
			glog.Infof("unit test open, mock send request to channel fail")
			newPath := generateFilename()
			storeLocal(request, newPath)
			return fmt.Errorf("send to unit test failed")
		} else {
			unitTestChannel <- request
		}
	} else { // normal send
		var err error
		var ret *store.StoreSaveResponse
		ctx, _ := context.WithTimeout(context.Background(), sendGrpcTimeout * time.Second)
		if ret, err = s.client.Client.Save(ctx, request); err == nil {
			if errRet := ret.GetError(); errRet == nil || errRet.Errno == 0 {
				return nil
			} else {
				//err = fmt.Errorf("send to address[%s] return error[%v] with errno[%v]",
				//	s.client.Addr, errRet.Errmsg, errRet.Errno)
			}
			return nil // todo, return nil directly
		}
		// store local file: directory/time(int32)_nanoseconds_suffix
		newPath := generateFilename()
		storeLocal(request, newPath)
		return fmt.Errorf("send to address[%s] failed[%v]", s.client.Addr, err)
	}

	return nil
}

func (s *sender) Close() {
	glog.Infof("sender closed")
	// close all grpc client
	s.client.Close()
	close(s.msgChan) // close channel
}

//---------------------------------splitter---------------------------------

// send data to store server
type Executor struct {
	MsgChan  chan *model.SenderContext // sender channel // todo, add into metric
	ctrlChan chan struct{}

	serviceName string
	hb          *heartbeat.Heartbeat   // heart beat server, not owned

	grpcClientMap   map[int32]*sender       // grpc client used to send
	storeServerList []*heartbeat.NodeStatus // heartbeat service list
}

func NewExecutor(serviceName string, hb *heartbeat.Heartbeat) *Executor {
	se := &Executor{
		MsgChan:       make(chan *model.SenderContext, executorChanSize),
		ctrlChan:      make(chan struct{}),
		serviceName:   serviceName,
		hb:            hb,
		grpcClientMap: make(map[int32]*sender), // set capacity to 3 by default
	}

	// os.IsExist has bug, use os.IsNotExist instead
	if err := os.Mkdir(conf.Options.WorkPathSendFail, util.DirPerm); err != nil && !os.IsExist(err) {
		glog.Errorf("mkdir dir[%s] fail[%v]", conf.Options.WorkPathSendFail, err)
		return nil
	}
	go se.run()

	return se
}

func (e *Executor) Close() {
	glog.Infof("Executor closed")
	close(e.ctrlChan)

	for _, c := range e.grpcClientMap {
		c.Close()
	}
}

// used in restful api
func (e *Executor) QueueStatus() interface{} {
	mp := make(map[string]interface{}, 2)
	mp[executorQueue] = fmt.Sprintf("%d/%d", len(e.MsgChan), executorChanSize)

	senderMp := make(map[int32]interface{}, len(e.grpcClientMap))
	for key, val := range e.grpcClientMap {
		senderMp[key] = fmt.Sprintf("%d/%d", len(val.msgChan), senderChanSize)
	}
	mp[senderQueue] = senderMp

	return mp
}

func (e *Executor) run() {
	checker := time.NewTicker(checkConnectionInterval * time.Second)
	e.checkConnection()
	for {
		select {
		case _, ok := <-e.ctrlChan:
			if !ok {
				glog.Infof("Executor exit")
				return
			}
		case msg := <-e.MsgChan:
			e.handle(msg)
		case <-checker.C:
			e.checkConnection()
			e.resend() // trigger re-send
		}
	}

	glog.Info("senderExecutor exit")
}

func (e *Executor) handle(msg *model.SenderContext) {
	// generate and send current info
	sender := e.pickSender(msg.Hid)
	newInfo := e.generateStoreSaveInfo(msg)
	if sender == nil {
		glog.Errorf("Executor: pick sender of hid[%d] error", msg.Hid)
		storeLocalInfo(newInfo, generateFilename())
		return
	}
	sender.msgChan <- newInfo
}

func (e *Executor) resend() {
	// load local files if exist
	files, err := ioutil.ReadDir(conf.Options.WorkPathSendFail)
	if err != nil {
		// discard all if read fail
		glog.Errorf("open directory[%s] fail[%v]", conf.Options.WorkPathSendFail, err)
		return
	}

	if len(files) != 0 {
		var file os.FileInfo
		nr := 0
		for _, file = range files {
			nr++
			if nr > resendFileReadLimit {
				glog.Infof("Executor: re-send pass the threshold[%d]", resendFileReadLimit)
				break
			}

			fileName := fmt.Sprintf("%s/%s", conf.Options.WorkPathSendFail, file.Name())
			content, err := ioutil.ReadFile(fileName)
			if err != nil {
				err = fmt.Errorf("read file[%s] fail[%v]", fileName, err)
				glog.Error(err)
				continue // do not delete file if error
			}

			// load local content
			oldRequest := new(store.StoreSaveRequest)
			if err = json.Unmarshal(content, oldRequest); err != nil {
				// ignore this file if fail
				err = fmt.Errorf("decoding file[%s] fail[%v]", fileName, err)
				deleteLocal(fileName) // delete file if decode fail
				glog.Error(err)
				continue // do not delete file if error
			}

			shouldDelete := true
			for _, info := range oldRequest.InfoList {
				sender := e.pickSender(info.Header.Hid)
				if sender == nil {
					glog.Errorf("Executor: pick sender of hid[%d] error", info.Header.Hid)
					// storeLocalInfo(info, generateFilename())
					shouldDelete = false
					break
				}
				sender.msgChan <- info
			}

			// todo, 假设此时重启，将会导致内存部分的数据丢失
			if shouldDelete {
				/*
				 * strictly speaking, it isn't a good strategy to delete file here because
				 * re-sending may also fail. The only reason I delete file here is for easy
				 * coding. todo
				 * some data may be re-send several times but acceptable.
				 */
				deleteLocal(fileName) // delete file
			}
		}
	}
}

func (e *Executor) checkConnection() {
	// get all services
	storeServerList, err := e.hb.GetServices(heartbeat.ModuleStore, heartbeat.ServiceBoth)
	if err != nil {
		glog.Errorf("checkConnection get all services[store] error[%v]", err)
		return
	}

	e.storeServerList = storeServerList
	glog.Infof("checkConnection: alive store[%v]", parseAliveList(e.storeServerList))

	for _, val := range e.storeServerList {
		// check client works
		if e.grpcClientMap[val.Gid] != nil {
			continue
		}

		sender := newSender(util.ConvertUnderline2Dot(val.Name))
		if sender == nil {
			glog.Errorf("Executor: create sender with store_server address[%s] fail", val.Name)
			continue
		}
		// create new connection
		e.grpcClientMap[val.Gid] = sender
	}
}

// generate request based on the input
func (e *Executor) generateStoreSaveInfo(input *model.SenderContext) *core.Info {
	items := make([]*core.KVPair, 0, len(input.Mp))
	for key, val := range input.Mp {
		items = append(items, &core.KVPair{
			Key:   key,
			Value: val,
		})
	}

	info := &core.Info{
		Header: &core.Header{
			Service: e.serviceName,
			Hid:     input.Hid,
			Host:    util.ConvertUnderline2Dot(input.InstanceName),
		},
		Timestamp: input.Timestamp,
		Count:     input.Count,
		Step:      input.Step,
		IndexList: nil, // todo
		Items:     items,
	}

	return info
}

func (e *Executor) pickSender(hid int32) *sender {
	if n := len(e.storeServerList); n == 0 {
		glog.Errorf("no store server exist including dead")
		return nil
	} else {
		idx := util.HashInstanceByHid(hid, n)
		picked := e.storeServerList[idx]

		return e.grpcClientMap[picked.Gid]
	}
}

//---------------------------------splitter---------------------------------

/*
 * all the file name is format in timestamp of 10 digits int with leading 0.
 * each file stores 1 minute data.
 * we store the json marsh data inside the file.
 * remote store: mixCollectionName -> sendFailKey -> {sendFailStartKey, sendFailEndKey}
 * sendFailStartKey and sendFailEndKey mean the left and right boundary
 */
func storeLocal(request *store.StoreSaveRequest, filename string) bool {
	data, err := json.Marshal(request)
	if err != nil {
		glog.Errorf("storeLocal: encoding data[%v] by json error[%v]", *request, err)
		return false
	}

	if files, err := ioutil.ReadDir(conf.Options.WorkPathSendFail); err != nil {
		glog.Errorf("storeLocal: read directory[%s] error[%v]",
			conf.Options.WorkPathSendFail, err)
		return false
	} else if len(files) >= resendFileWriteLimit {
		glog.Warningf("storeLocal: local files number[%v] exceed threshold[%v], discard current file",
			len(files), resendFileWriteLimit)
		return false
	}

	if err = ioutil.WriteFile(filename, data, util.FilePerm); err != nil {
		glog.Errorf("storeLocal: write file[%s] error[%v]",
			filename, err)
		return false
	}
	return true
}

// store core.Info
func storeLocalInfo(info *core.Info, filename string) bool {
	if info == nil {
		return true
	}
	request := &store.StoreSaveRequest{
		InfoList: []*core.Info{info},
	}

	return storeLocal(request, filename)
}

func deleteLocal(filename string) {
	if err := os.Remove(filename); err != nil {
		glog.Errorf("remove file[%s] fail", filename)
	}
}

// convert filename from int to string with at most one path
func generateFilename() string {
	newPath := fmt.Sprintf("%s/%d_%d", conf.Options.WorkPathSendFail, time.Now().Unix(),
		suffix)
	forwardCas(&suffix, 1)
	return newPath
}

func forwardCas(v *uint32, add uint32) {
	var current, newVal uint32
	for current = atomic.LoadUint32(v); ; {
		newVal = (current + add) % maxFilenameSuffix
		if atomic.CompareAndSwapUint32(v, current, newVal) {
			break
		}
		current = atomic.LoadUint32(v)
	}
}

func parseAliveList(aliveList []*heartbeat.NodeStatus) []string {
	ans := make([]string, len(aliveList))
	for _, alive := range aliveList {
		ans = append(ans, alive.Name)
	}
	return ans
}