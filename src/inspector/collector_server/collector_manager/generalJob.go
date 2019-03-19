package collectorManager

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"

	"inspector/cache"
	"inspector/collector_server/connector"
	"inspector/collector_server/job"
	"inspector/collector_server/model"
	"inspector/collector_server/sender"
	"inspector/compress"
	"inspector/config"
	"inspector/dict_server"
	"inspector/heartbeat"
	"inspector/proto/core"
	"inspector/util"
	"inspector/util/scheduler"

	"github.com/golang/glog"
	"inspector/collector_server/metric"
)

const (
	addTCBInterval = 1 // s
	emptyString    = ""

	ringBufferCount            = 120 // 120 point
	tcbChanSize                = 65536
	tcbChanMinReadSize float64 = 100
)

// control all job task
type GeneralJob struct {
	Name      string                 // name: mongo3.4, redis4.0
	Frequency int                    // time interval
	Cs        config.ConfigInterface // config server, not owned
	Ds        *dictServer.DictServer // dict server, not owned
	Hb        *heartbeat.Heartbeat   // heart beat server, not owned

	scheduler      *scheduler.Scheduler // hold the scheduler passed by collectorManager
	taskList       *sync.Map            // task list. task address(ip:port) -> job.Job
	cm             *CollectorManager    // now owned
	tcbChan        chan *scheduler.TCB  // tcb channel
	SenderExecutor *sender.Executor     // sender executor, send message to store server
}

func NewGeneralJob(name string, interval int, schd *scheduler.Scheduler, cm *CollectorManager,
	ds *dictServer.DictServer, hb *heartbeat.Heartbeat, cs config.ConfigInterface) *GeneralJob {
	gj := &GeneralJob{
		Name:           name,
		Frequency:      interval,
		scheduler:      schd,
		taskList:       new(sync.Map),
		cm:             cm,
		Ds:             ds,
		Hb:             hb,
		Cs:             cs,
		tcbChan:        make(chan *scheduler.TCB, tcbChanSize),
		SenderExecutor: sender.NewExecutor(name, hb),
	}

	if gj.SenderExecutor == nil {
		return nil
	}
	return gj
}

func (gj *GeneralJob) Start() {
	// todo, need chan close control?
	go gj.scheduler.Run()
	go gj.addTCBExecutor() // fetch tcb from list and add into scheduler
}

func (gj *GeneralJob) Stop() {
	// close sender executor
	gj.SenderExecutor.Close()
}

// may execute concurrently
func (gj *GeneralJob) AddInstance(ins *model.Instance) error {
	glog.Infof("GeneralJob AddInstance: [%s]", ins.Addr)

	if _, ok := gj.taskList.Load(ins.Addr); ok {
		return fmt.Errorf("instance[%s] is already exist in the gerneral job task list", ins.Addr)
	}

	// new connector
	connector := connector.NewConnector(gj.Name, ins)
	if connector == nil || reflect.ValueOf(connector).IsNil() {
		return fmt.Errorf("create connector error")
	}

	// create TCB
	tcb := new(scheduler.TCB)
	if err := tcb.Init(ins.Addr); err != nil {
		return err
	}
	tcb.SetWorkflowTimeoutWarning(gj.Frequency)

	// create ring cache
	ringCache := new(cache.RingCache)
	ringCache.Init(ins.Addr, ins.Count*2) // ring cache size == count * 2

	tcb.AtExit(connector.Close)
	tcb.AtExit(ringCache.Close)

	// create job
	jobTask := job.Create(gj.Name, tcb, connector, ringCache, gj.Cs, gj.Ds, gj.Hb, ins,
		gj.SenderExecutor.MsgChan)
	if jobTask == nil {
		return fmt.Errorf("create job fail")
	}

	// equip job: add steps into TCB
	if err := jobTask.Equip(false); err != nil {
		return fmt.Errorf("equip job error[%v]", err)
	}

	// store into task list
	gj.taskList.Store(ins.Addr, jobTask)

	// put into tcb pending list to call running asynchronously
	gj.tcbChan <- tcb
	return nil
}

// may execute concurrently
func (gj *GeneralJob) RemoveInstance(taskAddress string) error {
	glog.Infof("GeneralJob RemoveInstance: %s", taskAddress)

	jobTask, ok := gj.taskList.Load(taskAddress)
	if !ok {
		return fmt.Errorf("instance[%s] is not exist in the gerneral job task list", taskAddress)
	}
	job := jobTask.(job.Job)

	// remove tcb
	tcb := job.GetTCB()
	gj.scheduler.DelTCB([]*scheduler.TCB{tcb})

	// remove from the task list
	gj.taskList.Delete(taskAddress)

	// update metric
	metric.GetMetric(gj.Name).AddInstanceNumber(-1)
	return nil
}

// query data from cache(ring cache), return map, key -> timestamp -> data
func (gj *GeneralJob) QueryData(query *core.Query) (*core.InfoRange, string) {
	host := util.ConvertDot2Underline(query.Header.Host)
	jobTask, ok := gj.taskList.Load(host)
	if !ok {
		return nil, fmt.Sprintf("host[%s] doesn't exist in general job task list", host)
	}

	jobInfo := jobTask.(job.Job)
	ringCache := jobInfo.GetRingCache()
	_, count := jobInfo.GetBaseInfo()
	result := make([]int64, count*2)

	// write data
	var err error
	retByte := new(bytes.Buffer)
	// 1. write outer list count
	if err = binary.Write(retByte, binary.BigEndian, int32(len(query.KeyList))); err != nil {
		return nil, fmt.Sprintf("write outer list count into buffer error[%v]", err)
	}

	// query in the ring cache
	for _, key := range query.KeyList {
		longKey, err := gj.Ds.GetKey(key)
		if err != nil {
			glog.Warningf("query long-key with short-key[%s] error", key)
		}

		// 2. write key size
		if err = binary.Write(retByte, binary.BigEndian, int32(len(key))); err != nil {
			return nil, fmt.Sprintf("write short-key[%s](long-key[%s]) size into buffer error[%v]",
				key, longKey, err)
		}

		// 3. write key
		if _, err = retByte.WriteString(key); err != nil {
			return nil, fmt.Sprintf("write short-key[%s](long-key[%s]) into buffer error[%v]",
				key, longKey, err)
		}

		valInt, err := util.RepString2Int(key)
		if err != nil {
			glog.Errorf("convert short-key[%s](long-key[%s],int-key[%d]) to int error[%v]",
				key, longKey, valInt, err)
			binary.Write(retByte, binary.BigEndian, int32(0)) // 4. write inner list count
			continue
		}

		if valInt > ringCache.GetMaxOffset() {
			// return nil, fmt.Sprintf("invalid short-key[%s](long-key[%s],int-key[%d])", key, longKey, valInt)
			glog.Errorf("invalid short-key[%s](long-key[%s],int-key[%d])", key, longKey, valInt)
			binary.Write(retByte, binary.BigEndian, int32(0)) // 4. write inner list count
			continue
		}

		// input start timestamp is useless here
		// queryEnd := query.TimeEnd / uint32(gj.Frequency)
		queryEnd := query.TimeEnd
		queryStart, usedArr := ringCache.Query(valInt, queryEnd, result)
		if usedArr == nil {
			glog.Errorf("ringCache query short-key[%s](long-key[%s],int-key[%d]) queryEnd[%v] step[%d] not found",
				key, longKey, valInt, queryEnd, gj.Frequency)
			binary.Write(retByte, binary.BigEndian, int32(0)) // 4. write inner list count
			continue
		}

		// 120 seconds
		var data [2][]byte
		if data[0], err = compress.Compress(compress.NoCompress, usedArr[:count]); err != nil {
			return nil, fmt.Sprintf("compress short-key[%s](long-key[%s],int-key[%d]) data1[%v] error[%v]",
				key, longKey, valInt, usedArr[:count], err)
		}
		if data[1], err = compress.Compress(compress.NoCompress, usedArr[count:]); err != nil {
			return nil, fmt.Sprintf("compress short-key[%s](long-key[%s],int-key[%d]) data2[%v] error[%v]",
				key, longKey, valInt, usedArr[count:], err)
		}

		var beginTimestamp [2]uint32
		//beginTimestamp[0] = queryStart * uint32(gj.Frequency)
		//beginTimestamp[1] = (queryStart + uint32(count)) * uint32(gj.Frequency)
		beginTimestamp[0] = queryStart
		beginTimestamp[1] = queryStart + uint32(count)

		// 4. write inner list count
		if err = binary.Write(retByte, binary.BigEndian, int32(2)); err != nil {
			return nil, fmt.Sprintf("write short-key[%s](long-key[%s],int-key[%d]) inner list count into buffer error[%v]",
				key, longKey, valInt, err)
		}

		for i := 0; i < 2; i++ {
			// 5. write timestamp
			if err = binary.Write(retByte, binary.BigEndian, beginTimestamp[i]); err != nil {
				return nil, fmt.Sprintf("write short-key[%s](long-key[%s],int-key[%d]) timestamp into buffer error[%v]",
					key, longKey, valInt, err)
			}

			// 6. write data size
			if err = binary.Write(retByte, binary.BigEndian, uint32(len(data[i]))); err != nil {
				return nil, fmt.Sprintf("write short-key[%s](long-key[%s],int-key[%d]) data size into buffer error[%v]",
					key, longKey, valInt, err)
			}

			// 7. write data
			if err = binary.Write(retByte, binary.BigEndian, data[i]); err != nil {
				return nil, fmt.Sprintf("write short-key[%s](long-key[%s],int-key[%d]) point into buffer error[%v]",
					key, longKey, valInt, err)
			}
		}
	}

	return &core.InfoRange{
		Header: query.Header,
		Count:  uint32(count),
		Step:   uint32(gj.Frequency),
		Error:  nil,
		Data:   retByte.Bytes(),
	}, emptyString
}

func (gj *GeneralJob) addTCBExecutor() {
	var msg *scheduler.TCB
	var ok bool
	interval := float64(gj.Frequency * 60.0 / addTCBInterval)
	for range time.Tick(addTCBInterval * time.Second) {
		select {
		case msg, ok = <-gj.tcbChan:
			if !ok {
				return
			}
		default:
			ok = false
		}
		if !ok {
			continue
		}

		/*
		 * balance to reduce sending pressure. Strictly speaking, this is not a
		 * completely evenly distributed algorithm because the channel size is
		 * changing every second.
		 */
		size := int(math.Max(float64(len(gj.tcbChan))/interval, tcbChanMinReadSize))
		arr := make([]*scheduler.TCB, 0, size)
		arr = append(arr, msg)
		arr = gj.readBatch(arr, size)
		gj.scheduler.AddTCB(arr)

		// update metric
		metric.GetMetric(gj.Name).AddInstanceNumber(int32(len(arr)))
		glog.Infof("addTCBExecutor: arr[%v], length[%v]", arr, len(arr))
	}
}

func (gj *GeneralJob) readBatch(batch []*scheduler.TCB, size int) []*scheduler.TCB {
	for len(batch) < size {
		select {
		case msg, ok := <-gj.tcbChan:
			if !ok {
				return batch // still send because data exist in the channel
			}
			batch = append(batch, msg)
		default:
			return batch
		}
	}
	return batch
}
