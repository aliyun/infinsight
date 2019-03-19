//go:generate protoc -I ../../../ --go_out=plugins=grpc:../../../ proto/core/core.proto
//go:generate protoc -I ../../../ --go_out=plugins=grpc:../../../ proto/store/store.proto

package main

import(
    "encoding/binary"
    "fmt"
    "time"
    "math"

    "inspector/storeserver/fillValidate/connection"
    "inspector/proto/core"
    "inspector/proto/store"
    "inspector/storeserver/fillValidate/validate/cmap"

    LOG "github.com/vinllen/log4go"
    "github.com/vinllen/mgo/bson"
)

const (
    DbUrl = "mongodb://root:root@10.101.72.137:65533/admin"
    CacheUrl = "10.101.72.137:60005"
    StartTime = "2018-02-15T08:00:00Z"
    EndTime = "2018-02-15T08:10:00Z"
)

type data struct {
    name string
    timestamp uint32
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
        d.timestamp = uint32(t.Unix())
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

// read from db and add data into map
func readFromDB(left, right string, cacheMap *cmap.CacheMap) bool {
    c := connection.NewDbConnection(DbUrl, "monitor", "wiredTiger", left, right)
    if c == nil {
        LOG.Error("connection to mongodb error")
        return false
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
            ins := fmt.Sprintf("%s;%d;%d", data.name, data.count, data.step)
            cacheMap.PutIns(data.name)
            for key, value := range data.items {
                if err := cacheMap.Put(ins, key, data.timestamp, value.([]byte)); err != nil {
                    return false
                }
            }
        }
    }
    LOG.Warn("finish reading")
    return true
}

// read from cache and delete data from map
func readFromCache(instance string, left, right time.Time, cacheMap *cmap.CacheMap) bool {
    g := connection.NewGrpcConnection(CacheUrl)
    if g == nil {
        LOG.Error("connection to cache error")
        return false
    }
    defer g.Close()

    request := &store.QueryRequest{}
    request.QueryList = make([]*core.Query, 1)
    request.QueryList[0] = new(core.Query)
    request.QueryList[0].Timebegin = uint32(left.Unix())
    request.QueryList[0].Timeend = uint32(right.Unix())
    request.QueryList[0].Name = instance
    // request.QueryList[0].KeyList = make([]string, 1)
    // request.QueryList[0].KeyList[0] = "2v"

    LOG.Info("instance[%s] left[%s][%d], right[%s][%d]", instance, left, uint32(left.Unix()), right, uint32(right.Unix()))

    rsp, err := g.SendQuery(request)
    if err != nil {
        LOG.Error("send query error: %v", err)
    }
    LOG.Warn("the length of response length: %d", len(rsp.Infos))
    for _, info := range rsp.Infos {
        header := info.Header
        body := info.Data
        // LOG.Info("header: %v", header)
        ins := fmt.Sprintf("%s;%d;%d", header.Name, header.Count, header.Step)

        // parse body
        var offset uint32
        nrKeyList := binary.BigEndian.Uint32(body[offset: offset + 4])
        // LOG.Info("nrKeyList: %d [%v]\n", nrKeyList, body[offset: offset + 4])

        offset += 4
        for i := uint32(0); i < nrKeyList; i++ {
            szKey := binary.BigEndian.Uint32(body[offset: offset + 4]) // szKey
            // LOG.Info("szKey:%d\n", szKey)

            offset += 4
            key := string(body[offset: offset + szKey]) // key
            // LOG.Info("key:%s\n", key)

            offset += szKey
            nrValueList := binary.BigEndian.Uint32(body[offset: offset + 4]) // nrValueList

            offset += 4
            for j := uint32(0); j < nrValueList; j++ {
                timestamp := binary.BigEndian.Uint32(body[offset: offset + 4])

                offset += 4
                szValue := binary.BigEndian.Uint32(body[offset: offset + 4])
                // LOG.Info("szValue:%d\n", szValue)

                offset += 4
                value := body[offset: offset + szValue]
                LOG.Info("ins:%s key:%s timestamp:%d, value:%v\n", ins, key, timestamp, value)

                offset += szValue
                if uint32(timestamp) < request.QueryList[0].Timebegin || uint32(timestamp) > request.QueryList[0].Timeend {
                    continue
                }
                if err := cacheMap.Delete(ins, key, uint32(timestamp), value); err != nil {
                    LOG.Error("delete error: %s", err.Error())
                    return false
                }
            }
        }
    }
    LOG.Info("finish reading from cache")
    return true
}

func validate(left, right time.Time, cacheMap *cmap.CacheMap) bool {
    LOG.Info("validate called: left[%v], right[%v]", left, right)

    leftString := left.Format("2006-01-02T15:04:05Z")
    rightString := right.Format("2006-01-02T15:04:05Z")
    if readFromDB(leftString, rightString, cacheMap) == false { // read data from db
        LOG.Error("read from DB error")
        return false
    }
    // LOG.Info("read from cache map: %v", cacheMap)

    for key, _ := range cacheMap.InstanceMap {
        if readFromCache(key, left, right, cacheMap) == false {
            LOG.Error("read from cache error with instance[%s]", key)
            return false
        }
    }
    LOG.Warn("all cache data hit db !!!")

    return cacheMap.Empty()
}

func convertTimeToTime(timestamp string) (time.Time, error) {
    t, err := time.ParseInLocation("2006-01-02T15:04:05Z", timestamp, time.Local)
    if err != nil {
        LOG.Error("convert timestamp[%s] error", timestamp)
        return time.Time{}, err
    }
    LOG.Info(t)
    return t, nil
}

func convertTimeToInt64(timestamp string) (int64, error) {
    t, err := convertTimeToTime(timestamp)
    if err != nil {
        return 0, err
    }
    return t.Unix(), nil
}

func convertInt64ToTime(timestamp int64) (time.Time) {
    return time.Unix(timestamp, 0)
}

func main() {
    if !initLog() {
        fmt.Println("init log error")
        return
    }

    startInt, err := convertTimeToInt64(StartTime)
    if err != nil {
        LOG.Error("convert time[%v] to int64 error: %s", StartTime, err.Error())
        return
    }
    endInt, err := convertTimeToInt64(EndTime)
    if err != nil {
        LOG.Error("convert time[%v] to int64 error: %s", EndTime, err.Error())
        return
    }

    dist := int64(120) // seconds
    for leftInt := startInt; leftInt < endInt; leftInt += dist {
        leftTime := convertInt64ToTime(leftInt)
        rightTime := convertInt64ToTime(int64(math.Min(float64(leftInt + dist), float64(endInt))))

        cacheMap := cmap.NewCacheMap()
        if ok := validate(leftTime, rightTime, cacheMap); !ok {
            LOG.Error("validate failed !")
            return
        }
    }

    LOG.Error("All finish !!!!")
}
