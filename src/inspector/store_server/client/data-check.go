package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

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

func doQuery(c store.StoreServiceClient, timestamp uint32) {
	t1 := time.Now()
	ts1 := t1.UnixNano()
	s := rand.NewSource(int64(timestamp))
	r := rand.New(s)
	for i := 0; i < 1; i++ {
		slot := r.Intn(60)
		num := r.Intn(100)
		slot = 10
		num = 0
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var req *store.QueryRequest = new(store.QueryRequest)
		req.QueryList = make([]*core.Query, 1)
		req.QueryList[0] = new(core.Query)
		req.QueryList[0].Name = fmt.Sprintf("instance-%d-%d", slot, num)
		req.QueryList[0].Timebegin = timestamp
		req.QueryList[0].Timeend = timestamp + 3600

		n := 1
		fmt.Println(req.QueryList[0].Name, n)
		keyList := make([]string, 0)
		for i := 0; i < n; i++ {
			keyList = append(keyList, fmt.Sprintf("k%d", i))
		}
		req.QueryList[0].KeyList = keyList
		fmt.Println("keyList:", keyList)
		fmt.Println("req.QueryList[0].KeyList:", req.QueryList[0].KeyList)
		fmt.Println("req:", req)

		res, err := c.Query(ctx, req)
		if err != nil {
			log.Fatalf("send query error: %v", err)
		}
		fmt.Println("res:", res)
	}
	t2 := time.Now()
	ts2 := t2.UnixNano()
	fmt.Println("time used:", ts2-ts1)
	fmt.Println("----------------------------------------------------------------")
	return

}

func main() {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("connect error: %v", err)
	}
	defer conn.Close()

	c := store.NewStoreServiceClient(conn)
	var timestamp uint32 = 600566400
	doQuery(c, timestamp)
	return

	//	t := time.NewTicker(1 * time.Second)
	//	for {
	//		select {
	//		case <-t.C:
	//			cur := time.Now()
	//			timestamp := uint32(cur.Unix())
	//			go doQuery(c, timestamp)
	//		}
	//	}
}
