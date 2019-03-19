package main

import (
	// "bytes"
	// "encoding/binary"
	"fmt"
	_"net/http/pprof"
	"net/http"
	"github.com/gugemichael/nimo4go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"inspector/proto/core"
	"inspector/proto/store"
	"log"
	"math/rand"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	address = "10.101.72.137:60003"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

var buf [1024 * 1024]byte

func doQuery(c store.StoreServiceClient, timestamp uint32) {
	s := rand.NewSource(int64(timestamp))
	r := rand.New(s)
	// var n int = 1000
	gt1 := time.Now()
	gts1 := gt1.UnixNano()
	//	for i := 0; i < n; i++ {
	for {
		t1 := time.Now()
		ts1 := t1.UnixNano()
		slot := r.Intn(60)
		num := r.Intn(100)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var request *store.QueryRequest = new(store.QueryRequest)
		request.QueryList = make([]*core.Query, 1)
		request.QueryList[0] = new(core.Query)
		request.QueryList[0].Name = fmt.Sprintf("instance-%d-%d", slot, num)
		request.QueryList[0].Timebegin = timestamp - 3600
		request.QueryList[0].Timeend = timestamp

		// // buf := make([]byte, 8*1024, 8*1024)
		// buf := make([]byte, 0)

		// var n int = 700
		// var x int = 0
		// var size []byte = []byte(fmt.Sprintf("%04d", 700))
		// x += 4
		// for i := 0; i < n; i++ {
		// 	data := []byte(fmt.Sprintf("k%d", i))
		// 	size = []byte(fmt.Sprintf("%02d", len(data)))
		// 	buf = append(buf, size...)
		// 	buf = append(buf, data...)
		// 	// copy(buf[x:], size)
		// 	// x += 2
		// 	// copy(buf[x:], data)
		// 	// x += len(data)
		// }
		// request.QueryList[0].KeyList = buf[:x]

		// bytesBuffer := bytes.NewBuffer(make([]byte, 0, 1024))
		// binary.Write(bytesBuffer, binary.BigEndian, 700)
		// var n int = 100
		// binary.Write(bytesBuffer, binary.BigEndian, uint32(n))
		// for i := 0; i < n; i++ {
		// 	data := []byte(keys[i])
		// 	binary.Write(bytesBuffer, binary.BigEndian, uint32(len(data)))
		// 	bytesBuffer.Write(data)
		// }
		// request.QueryList[0].KeyList = bytesBuffer.Bytes()

		var n int = 10
		keyList := make([]string, 0)
		for i := 0; i < n; i++ {
			keyList = append(keyList, keys[i])
		}
		request.QueryList[0].KeyList = keyList

		res, err := c.Query(ctx, request)
		if err != nil {
			log.Fatalf("send query error: %v", err)
		}
		if len(res.Infos) == 0 {
		 	 // fmt.Println("Infos size is 0")
			 continue
		}
		t2 := time.Now()
		ts2 := t2.UnixNano()
		atomic.AddUint64(&count, 1)
		atomic.AddUint64(&delay, uint64(ts2-ts1))
		if ts2-ts1 > 100*1000*1000 {
			// fmt.Println("time used:", ts2-ts1)
		}
		cancel()
	}
	gt2 := time.Now()
	gts2 := gt2.UnixNano()
	if gts2-gts1 > 1000*1000*1000 {
		fmt.Println("global time used:", gts2-gts1)
	}
	return

}

var count uint64
var delay uint64
var keys []string

func main() {
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:60000", nil)) 
	}()
	nimo.RegisterSignalForProfiling(syscall.SIGUSR1)
	for i := 0; i < 700; i++ {
		keys = append(keys, fmt.Sprintf("k%d", i))
	}
	for i := 0; i < 512; i++ {
		go func() {
			conn, err := grpc.Dial(address, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("connect error: %v", err)
			}
			defer conn.Close()

			c := store.NewStoreServiceClient(conn)
			cur := time.Now()
			timestamp := uint32(cur.Unix())
			doQuery(c, timestamp)
			return

			t := time.NewTicker(1 * time.Second)
			for {
				select {
				case <-t.C:
					cur := time.Now()
					timestamp := uint32(cur.Unix())
					go doQuery(c, timestamp)
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
			fmt.Println("delay:", (delay - delay_old) / countDiff)
			delay_old = delay
		}
	}
}
