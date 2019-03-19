package main

import (
	"fmt"
	"log"
	"time"

	"sync/atomic"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"inspector/proto/core"
	"inspector/proto/store"
)

const (
	address = "10.101.72.137:60003"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func doSend(c store.StoreServiceClient, timestamp uint32) {
	slot := timestamp % 60
	for i:=0; i<100; i++ {
		t1 := time.Now()
		ts1 := t1.UnixNano()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var request *store.StoreRequest = new(store.StoreRequest)
		request.InfoList = make([]*core.Info, 1)
		request.InfoList[0] = new(core.Info)
		request.InfoList[0].Header = new(core.Header)
		request.InfoList[0].Header.Name = fmt.Sprintf("instance-%d-%d", slot, i)
		request.InfoList[0].Header.Timestamp = timestamp
		request.InfoList[0].Header.Count = 60
		request.InfoList[0].Header.Step = 1
		request.InfoList[0].Items = make([]*core.KVPair, 700)
		for j := 0; j < 700; j++ {
			kv := new(core.KVPair)
			kv.Key = fmt.Sprintf("k%d", j)
			kv.Value = []byte(fmt.Sprintf("========%s,%d========", kv.Key, timestamp))
			request.InfoList[0].Items[j] = kv
		}

		_, err := c.Store(ctx, request)
		if err != nil {
			log.Fatalf("send store error: %v", err)
		}
		atomic.AddUint64(&count, 1)
		t2 := time.Now()
		ts2 := t2.UnixNano()
		atomic.AddUint64(&delay, uint64(ts2 - ts1))
	}
}

var count uint64
var delay uint64

func main() {
	var timeBegin uint32 = 600566400
	for i:=0; i< 1; i++ {
		go func() {
			var n uint32 = 0
			for {
				conn, err := grpc.Dial(address, grpc.WithInsecure())
				if err != nil {
					log.Fatalf("connect error: %v", err)
				}
				defer conn.Close()

				c := store.NewStoreServiceClient(conn)
				doSend(c, timeBegin + n)
				n++
				continue

				t := time.NewTicker(1 * time.Second)
				for {
					select {
					case <-t.C:
						cur := time.Now()
						timestamp := uint32(cur.Unix())
						go doSend(c, timestamp)
					}
				}
			}
		}()
	}
	t := time.NewTicker(1 * time.Second)
	var count_old uint64
	var delay_old uint64
	for {
		select {
		case <-t.C:
			countDiff := count - count_old
			fmt.Println("count:", countDiff)
			count_old = count
			if countDiff != 0 {
				fmt.Println("delay:", (delay - delay_old) / countDiff)
				delay_old = delay
			}
		}
	}
}
