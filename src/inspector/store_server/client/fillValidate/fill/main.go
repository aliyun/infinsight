//go:generate protoc -I ../../../ --go_out=plugins=grpc:../../../ proto/core/core.proto
//go:generate protoc -I ../../../ --go_out=plugins=grpc:../../../ proto/store/store.proto

package main

import(
    "encoding/binary"
    "fmt"
    "time"

    "inspector/storeserver/fillValidate/connection"
    "inspector/proto/core"
    "inspector/proto/store"

    LOG "github.com/vinllen/log4go"
    "github.com/vinllen/mgo/bson"
)

const (
    DbUrl = "mongodb://root:root@10.101.72.137:65533/admin"
    CacheUrl = "10.101.72.137:60005"
    StartTime = "2018-02-15T08:00:00Z"
    EndTime = "2018-02-15T09:00:00Z"
)

type data struct {
    name string
    timestap uint32
    count uint32
    step uint32
    items bson.M
}

func initLog() bool {
    fileLogger := LOG.NewFileLogWriter("log.out", true)
    fileLogger.SetRotateDaily(true)
    fileLogger.SetFormat("[%D %T] [%L] [%s] %M")
    fileLogger.SetRotateMaxBackup(7)
    LOG.AddFilter("file", LOG.INFO, fileLogger)
    return true
}

func parseToData(raw *bson.M) *data {
    d := &data{}
    d.name = (*raw)["h"].(string)
    if t, ok := (*raw)["t"].(time.Time); ok {
        d.timestap = uint32(t.Unix())
    }

    if countAndStep, ok := (*raw)["c"].([]byte); ok {
        if len(countAndStep) > 8 {
            LOG.Error("parse 'c' error")
            return nil
        }
        tmp := make([]byte, len(countAndStep))
        for i, b := range countAndStep {
            tmp[i] = b
        }
        tcount, nr := binary.Uvarint(tmp)
        tstep, _ := binary.Uvarint(tmp[nr:])
        d.count = uint32(tcount)
        d.step = uint32(tstep)
        if d.step == 0 {
            d.step = 1
        }
    }

    d.items = (*raw)["d"].(bson.M)

    return d
}

func readFromDB(dataChan chan<- *data) {
    defer close(dataChan)

    c := connection.NewDbConnection(DbUrl, "monitor", "wiredTiger", StartTime, EndTime)
    if c == nil {
        LOG.Error("connection to mongodb error")
        return
    }
    LOG.Info("start reading from db")
    cnt := 0
    for {
        LOG.Info("read from db: %d", cnt)
        cnt = cnt + 1
        raw, err := c.Next()
        if err != nil {
            LOG.Error("read data from db meets error: %s", err.Error())
            break
        }

        if data := parseToData(raw); data != nil {
            dataChan <-data
        }
    }
    LOG.Warn("finish reading")
}

func writeToCache(dataChan <-chan *data) {
    g := connection.NewGrpcConnection(CacheUrl)
    if g == nil {
        LOG.Error("connection to cache error")
        return
    }
    defer g.Close()

    LOG.Info("start writting to cache")
    cnt := 0
    for data := range dataChan {
        LOG.Info("write into cache: %d, name[%s], time[%d][%s], count[%d], step[%d], len[%d]",
                cnt, data.name, data.timestap, time.Unix(int64(data.timestap), 0), data.count, data.step, len(data.items))
        cnt = cnt + 1
        // LOG.Info("receiver data: %v", data)
        var request *store.StoreRequest = new(store.StoreRequest)
		request.InfoList = make([]*core.Info, 1)
		request.InfoList[0] = new(core.Info)
		request.InfoList[0].Header = new(core.Header)
		request.InfoList[0].Header.Name = data.name
		request.InfoList[0].Header.Timestamp = data.timestap
		request.InfoList[0].Header.Count = data.count
		request.InfoList[0].Header.Step = data.step
		request.InfoList[0].Items = make([]*core.KVPair, 0)
        for key, item := range data.items {
			kv := new(core.KVPair)
			kv.Key = key
			kv.Value = item.([]byte)
			request.InfoList[0].Items = append(request.InfoList[0].Items, kv)
		}

		rsp, err := g.SendStore(request)
		if err != nil {
			LOG.Error("send store error: %v", err)
		} else if rsp.Errno != 0 {
            LOG.Error("store response with errno[%d] errmsg[%s]", rsp.Errno, rsp.Errmsg)
        }
    }
    LOG.Warn("finish writting")
}

func main() {
    if !initLog() {
        fmt.Println("init log error")
        return
    }

    dataChan := make(chan *data, 1000)
    go readFromDB(dataChan)
    writeToCache(dataChan)
}
