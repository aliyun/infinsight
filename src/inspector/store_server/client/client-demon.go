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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// send "store" command
	r, err := c.Store(ctx, &store.StoreRequest{
		InfoList: []*core.Info{
			&core.Info{
				Header: &core.Header{
					Name:      "10.10.10.10:8080",
					Count:     100,
					Timestamp: time.Now().Unix(),
					Step:      60,
				},
				Items: []*core.KVPair{
					&core.KVPair{
						Key:   "cpu",
						Value: []byte("test1"),
					},
				},
			},
		},
	})
	fmt.Println("debug")
	if err != nil {
		log.Fatalf("send store error: %v", err)
	}
	fmt.Println(r.String())

	fmt.Println("send store ok")

	for {
		var name string
		_, err := fmt.Scanf("%s", &name)
		checkErr(err)
		fmt.Printf("name:%s\n", name)

		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		// send "query" command
		r2, err := c.Query(ctx, &store.QueryRequest{
			QueryList: []*core.Query{
				&core.Query{
					Name: name,
				},
			},
		})
		if err != nil {
			log.Fatalf("send query error: %v", err)
		}

		fmt.Println(r2.String())
	}
}
