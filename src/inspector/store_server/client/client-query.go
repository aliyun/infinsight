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

	// send "query" command
	var host string
	for {
		_, err := fmt.Scanf("%s", &host)
		checkErr(err)
		fmt.Printf("host:%s\n", host)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		r2, err := c.Query(ctx, &store.QueryRequest{
			QueryList: []*core.Query{
				&core.Query{
					Header: &core.Header{
						Service: "test",
						Hid:     "1",
						Host:    host,
					},
					TimeBegin: uint32(time.Now().Add(-time.Minute * 60).Unix()),
					TimeEnd:   uint32(time.Now().Unix()),
				},
			},
		})
		if err != nil {
			log.Fatalf("send query error: %v", err)
		}

		fmt.Println(r2.String())
	}
}
