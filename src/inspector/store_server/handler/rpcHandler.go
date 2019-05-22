/*
// =====================================================================================
//
//       Filename:  rpcHandler.go
//
//    Description:
//
//        Version:  1.0
//        Created:  09/06/2018 06:03:51 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package handler

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"inspector/proto/core"
	"inspector/proto/store"
	"inspector/store_server/configure"
	"inspector/util"

	"github.com/golang/glog"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
)

// =====================================================================================
//       Struct:  RpcServer
//  Description:  struct for grpc
// =====================================================================================
type RpcServer struct {
}

// =====================================================================================
//       Struct:  storeModel
//  Description:  model for store
// =====================================================================================
type storeModel struct {
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  doStoreToMongo
//  Description:
// =====================================================================================
*/
func (h *RpcHandler) doStoreToMongo(input []*core.Info) error {
	glog.V(1).Infof("[Trace][doStoreToMongo] called: info count[%d] ", len(input))

	var doSendMongo = func(index int, service string, iflist []interface{}, errChan chan error) func() {
		return func() {
			var err = configure.Options.MongoStoreSessionList[index].
				DB(configure.Options.StoreServerDB).
				C(service).
				Insert(iflist...)
			if err != nil {
				glog.Errorf("store service[%s] to mongodb error: %s", service, err.Error())
				errChan <- err
			}
			errChan <- nil
		}
	}

	var msgListMap map[string][]bson.M = make(map[string][]bson.M)

	for _, it := range input {
		// check service
		var service = it.GetHeader().GetService()

		// get msgList
		var msgList []bson.M = nil
		var ok bool
		if msgList, ok = msgListMap[service]; !ok {
			msgList = make([]bson.M, 0)
		}

		// new msg
		var msg bson.M = bson.M{}

		// build msg header
		msg["i"] = util.Int32Reverse(it.Header.Hid)
		msg["h"] = it.Header.Host
		msg["t"] = it.Timestamp
		msg["c"] = fmt.Sprintf("%d:%d", it.Count, it.Step)
		msg["e"] = time.Unix(int64(it.Timestamp*it.Step), 0)

		// build msg body
		var body = make(map[string][]byte)
		for _, it := range it.Items {
			body[it.Key] = it.Value
			// fmt.Printf("debug store each key: hid[%v] host[%v] timestamp[%v] cs[%v] key[%v] value[%v]\n",
			// 	msg["i"], msg["h"], msg["t"], msg["c"], it.Key, it.Value)
		}
		msg["d"] = body

		msgList = append(msgList, msg)
		msgListMap[service] = msgList
	}

	// save to mongodb
	var errChan chan error = make(chan error, configure.Options.MongoStoreSessionListCount*2)
	var errLen int = 0
	var iflist = make([]interface{}, 0)
	for service, msgList := range msgListMap {
		var batch int = len(msgList) / configure.Options.MongoStoreSessionListCount
		if batch < 10 { // 10是拍脑袋写的，给一个最小批次，为了避免batch太小导致模0错误
			batch = 10
		}
		for i, it := range msgList {
			iflist = append(iflist, it)
			if i != 0 && i%batch == 0 {
				configure.Options.MongoStorePool.AsyncRun(doSendMongo((i/batch-1)%configure.Options.MongoStoreSessionListCount, service, iflist, errChan))
				errLen++
				iflist = make([]interface{}, 0)
			}
		}
		// send last batch
		if len(iflist) != 0 {
			configure.Options.MongoStorePool.AsyncRun(doSendMongo(0, service, iflist, errChan))
			errLen++
		}
	}

	// wait for all AsyncRun return
	for i := 0; i < errLen; i++ {
		if err := <-errChan; err != nil {
			return err
		}
	}

	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  doStoreToCache
//  Description:
// =====================================================================================
*/
func (h *RpcHandler) doStoreToCache(input []*core.Info) error {
	glog.V(1).Infof("[Trace][doStoreToCache] called: info count[%d] ", len(input))

	for _, it := range input {
		var err = configure.Options.TimeCache.Set(it)
		if err != nil {
			glog.Errorln("store to cache error: ", err)
		}
	}

	return nil
}

type valueItem struct {
	timestamp uint32
	value     []byte
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  query2infoRange
//  Description:
// =====================================================================================
*/
func (h *RpcHandler) query2infoRange(input *core.Query, query *mgo.Query) *core.InfoRange {
	var result *core.InfoRange = &core.InfoRange{}
	var valueMap map[string][]valueItem = make(map[string][]valueItem)

	var iter = query.Iter()

	// save first values
	row := bson.M{}
	if iter.Next(row) == true {
		// write header
		result.Header = &core.Header{
			Service: input.Header.Service,
			// bug: mongo use type(int32) as type(int)
			// Hid:     int32(row["i"].(int)),
			Host: row["h"].(string),
		}
		// 由于mongo存储int32，取出后变成int，所以这里对i值的类型做多重判断
		switch row["i"].(type) {
		case int:
			result.Header.Hid = int32(row["i"].(int))
		case int32:
			result.Header.Hid = row["i"].(int32)
		}
		// count是联合类型，结构为："count:step"，需要对string进行拆解
		fmt.Sscanf(row["c"].(string), "%d:%d", &result.Count, &result.Step)

		// save first value
		for k, v := range row["d"].(bson.M) {
			valueMap[k] = make([]valueItem, 0)
			valueMap[k] = append(valueMap[k], valueItem{
				timestamp: uint32(row["t"].(int)),
				value:     v.([]byte),
			})
		}

		// save other values
		row = bson.M{}
		for iter.Next(row) {
			// save values temporay
			for k, v := range row["d"].(bson.M) {
				valueMap[k] = append(valueMap[k], valueItem{
					timestamp: uint32(row["t"].(int)),
					value:     v.([]byte),
				})
			}
		}

	} else {
		return nil
	}

	// write to infoRange
	bytesBuffer := bytes.NewBuffer([]byte{})
	// write key list size
	binary.Write(bytesBuffer, binary.BigEndian, uint32(len(valueMap)))
	for k, v := range valueMap {
		// write key size
		binary.Write(bytesBuffer, binary.BigEndian, uint32(len(k)))
		// write key
		bytesBuffer.WriteString(k)
		// write data size
		binary.Write(bytesBuffer, binary.BigEndian, uint32(len(v)))
		for _, it := range v {
			binary.Write(bytesBuffer, binary.BigEndian, it.timestamp)
			binary.Write(bytesBuffer, binary.BigEndian, uint32(len(it.value)))
			bytesBuffer.Write(it.value)
			// fmt.Printf("debug query mongo: key[%v] timestamp[%v] value[%v]\n",
			// 	k, it.timestamp, it.value)
		}
	}
	result.Data = bytesBuffer.Bytes()

	return result
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  doQuery
//  Description:
// =====================================================================================
*/
func (h *RpcHandler) doQuery(input *core.Query) ([]*core.InfoRange, error) {
	glog.V(1).Infof("[Trace][doQuery] called: info[%v] ", input)

	var res []*core.InfoRange
	var cacheRes []*core.InfoRange
	var mongoRes []*core.InfoRange

	var timeBegin uint32
	var timeEnd uint32

	var err error

	// findInMongo closure
	var selector = bson.M{"i": 1, "h": 1, "t": 1, "c": 1}
	var findInMongo = func(keyList []string, start, end uint32) {
		var condition = bson.M{
			"i": util.Int32Reverse(input.Header.Hid),
			"h": input.Header.Host,
			"t": bson.M{
				"$gte": start,
				"$lte": end,
			},
		}
		for _, it := range keyList {
			selector["d."+it] = 1
		}
		var query = configure.Options.MongoQuerySession.DB(configure.Options.StoreServerDB).C(input.Header.Service).
			Find(condition).Select(selector).Sort("+t")
		if infoRange := h.query2infoRange(input, query); infoRange != nil {
			mongoRes = append(mongoRes, infoRange)
		}
		h.timeTick(fmt.Sprintf("doQuery[%v] from mongo", input.Header.Host))
		// for _, it := range mongoRes {
		// 	fmt.Println("debug query from db data: ", start, end, it.GetHeader(), util.ShowData(it.GetData()))
		// }
	}

	if len(input.KeyList) == 0 {
		selector["d"] = 1
	} else {
		buff := bytes.NewBuffer([]byte{})
		for _, it := range input.KeyList {
			buff.Reset()
			buff.WriteString("d.")
			buff.WriteString(it)
			selector[buff.String()] = 1
		}
	}

	cacheRes, timeBegin, timeEnd, err = configure.Options.TimeCache.Get(input)
	h.timeTick(fmt.Sprintf("doQuery[%v] from cache", input.Header.Host))
	glog.V(3).Infof("[Debug][doQuery] query from cache ret: timeBegin[%v], timeEnd[%v], err[%v]", timeBegin, timeEnd, err)
	// fmt.Println("debug query from cache ret: ", timeBegin, timeEnd, err)
	// for _, it := range cacheRes {
	// 	fmt.Println("debug query from cache data: ", timeBegin, timeEnd, it.GetHeader(), util.ShowData(it.GetData()))
	// }
	// findInMongo(input.GetKeyList(), input.TimeBegin, input.TimeEnd)
	// mongoRes = nil

	if err != nil {
		// get all data from mongo store
		// fmt.Println("debug: get all data from mongo store")
		glog.V(3).Infof("[Debug][doQuery] get all data from mongo store")
		findInMongo(input.GetKeyList(), input.TimeBegin, input.TimeEnd)

	} else {
		// get miss data from mongo store
		if timeBegin > input.TimeBegin {
			// fmt.Println("debug: get previous miss data from mongo store")
			glog.V(3).Infof("[Debug][doQuery] get previous miss data from mongo store")
			findInMongo(input.GetKeyList(), input.TimeBegin, timeBegin-1)
		}

		// 一般最近一段时间的数据，不可能在cache里没有
		// 这里为了解决数据以外丢失的情况，例如刚刚重启
		// if timeEnd < input.TimeEnd {
		// 	fmt.Println("debug query tail data in mongo")
		// 	findInMongo(input.GetKeyList(), timeEnd, input.TimeEnd)
		// }
		_ = timeEnd
	}

	if cacheRes != nil {
		res = append(res, cacheRes...)
	}
	if mongoRes != nil {
		res = append(res, mongoRes...)
	}
	glog.V(3).Infof("[Debug][doQuery] cacheResSize[%v], mongoResSize[%v]", len(cacheRes), len(mongoRes))

	if res == nil {
		return nil, errors.New("data not exist")
	}

	return res, nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Save
//  Description:  save handler for grpc
// =====================================================================================
*/
func (h *RpcHandler) Save(ctx context.Context, req *store.StoreSaveRequest) (*store.StoreSaveResponse, error) {
	glog.V(1).Infof("[Trace][RPC StoreSave] called: info list size[%v] ", len(req.GetInfoList()))

	res := &store.StoreSaveResponse{
		Error:       new(core.Error),
		SuccessList: make([]*core.ResponseItem, 0),
		FailureList: make([]*core.ResponseItem, 0),
	}

	var err error
	var infoList = req.InfoList

	h.timeReset()
	if err = h.doStoreToMongo(infoList); err != nil {
		res.Error.Errno = 255
		res.Error.Errmsg = err.Error()
	}
	h.timeTick(fmt.Sprintf("store[%d] to mongo", len(infoList)))

	h.doStoreToCache(infoList)
	h.timeTick(fmt.Sprintf("store[%d] to cache", len(infoList)))

	if glog.V(2) {
		var durationAll, durationList = h.getTimeConsumeResult()
		bytesBuffer := bytes.NewBuffer([]byte{})
		bytesBuffer.WriteString("[Perf][Save]: ")
		for _, it := range durationList {
			bytesBuffer.WriteString(
				fmt.Sprintf("step[%v](%v) time duration[%v]|",
					it.name, it.step, it.duration))
		}
		bytesBuffer.WriteString(fmt.Sprintf("all(hostcount[%v]) time duration[%v]\n", len(durationList)/2, durationAll))
		glog.Infof(bytesBuffer.String())
	}

	glog.Flush()
	return res, nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Query
//  Description:  query handler for grpc
// =====================================================================================
*/
func (h *RpcHandler) Query(ctx context.Context, req *store.StoreQueryRequest) (*store.StoreQueryResponse, error) {
	glog.V(1).Infof("[Trace][RPC StoreQuery] called: request[%v] ", req)

	res := &store.StoreQueryResponse{
		Error:       new(core.Error),
		SuccessList: make([]*core.InfoRange, 0),
		FailureList: make([]*core.InfoRange, 0),
	}

	queryList := req.QueryList

	h.timeReset()
	for _, query := range queryList {
		host := query.Header.Host
		if infoRange, err := h.doQuery(query); err != nil {
			glog.Errorf("doQuery error with host[%s]: %s", host, err.Error())
			res.FailureList = append(res.FailureList, &core.InfoRange{
				Header: query.Header,
				Error: &core.Error{
					Errno:  255,
					Errmsg: err.Error(),
				},
			})
		} else {
			res.SuccessList = append(res.SuccessList, infoRange...)
		}
		// h.timeTick(fmt.Sprintf("doQuery[%v]", host))
	}

	if glog.V(2) {
		bytesBuffer := bytes.NewBuffer([]byte{})
		var durationAll, durationList = h.getTimeConsumeResult()

		bytesBuffer.WriteString("[Perf][Query]: ")
		for _, it := range durationList {
			bytesBuffer.WriteString(
				fmt.Sprintf("step[%v](%v) time duration[%v]|",
					it.name, it.step, it.duration))
		}
		bytesBuffer.WriteString(fmt.Sprintf("all time duration[%v]\n", durationAll))
		glog.Infof(bytesBuffer.String())
	}

	if len(res.FailureList) == 0 {
		res.Error.Errno = 0
		res.Error.Errmsg = "OK"
	} else {
		res.Error.Errno = 255
		res.Error.Errmsg = "Fail"
	}

	glog.Flush()
	return res, nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Store
//  Description:  store handler for grpc
// =====================================================================================
*/
func (s *RpcServer) Save(ctx context.Context, req *store.StoreSaveRequest) (*store.StoreSaveResponse, error) {
	var h = new(RpcHandler)
	return h.Save(ctx, req)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Query
//  Description:  query handler for grpc
// =====================================================================================
*/
func (s *RpcServer) Query(ctx context.Context, req *store.StoreQueryRequest) (*store.StoreQueryResponse, error) {
	var h = new(RpcHandler)
	return h.Query(ctx, req)
}
