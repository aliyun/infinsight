package main

import (
	"fmt"
	"log"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"inspector/proto/core"
	"inspector/proto/store"
)

const (
	address = "127.0.0.1:46800"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("connect error: %v", err)
	}
	defer conn.Close()

	c := store.NewStoreServiceClient(conn)

	for {
		var host string
		_, err := fmt.Scanf("%s", &host)
		checkErr(err)
		fmt.Printf("host:%s\n", host)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// send "store" command
		r, err := c.Store(ctx, &store.StoreRequest{
			InfoList: []*core.Info{
				&core.Info{
					Header: &core.Header{
						Service: "test",
						Hid:     "1",
						Host:    host,
					},
					Timestamp: uint32(time.Now().Unix()),
					Count:     60,
					Step:      1,
					Items: []*core.KVPair{
						&core.KVPair{
							Key:   "cpu",
							Value: []byte(fmt.Sprintf("cpu-%d", time.Now().Unix())),
						},
					},
				},
			},
		})
		if err != nil {
			log.Fatalf("send store error: %v", err)
		}
		fmt.Println(r.String())
	}

}
