/*
// =====================================================================================
//
//       Filename:  mock.go
//
//    Description:  mock collector server
//
//        Version:  1.0
//        Created:  06/10/2018 10:21:41 AM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

//go:generate protoc -I ../../ --go_out=plugins=grpc:../../ inspector/proto/core/core.proto
//go:generate protoc -I ../../ --go_out=plugins=grpc:../../ inspector/proto/collector/collector.proto

package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"net"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pbCollector "inspector/proto/collector"
	pbCore "inspector/proto/core"
)

const (
	port = ":46300"
)

// server is used to implement helloworld.GreeterServer.
type server struct{}

func queryRange(key string, begin uint32, end uint32) (uint32, [][]byte) {
	var tBegin = time.Unix(int64(begin), 0)
	tBegin = tBegin.Truncate(time.Minute)
	begin = uint32(tBegin.Unix())

	var tEnd = time.Unix(int64(end), 0)
	tEnd = tEnd.Add(time.Minute)
	tEnd = tEnd.Truncate(time.Minute)
	end = uint32(tEnd.Unix())

	var data = make([][]byte, 0)
	for begin != end {
		data = append(data, make([]byte, 0))
		var buff = bytes.NewBuffer(data[len(data)-1])
		for i := 0; i < 60; i++ {
			binary.Write(buff, binary.BigEndian, uint32(begin))
			begin++
		}
	}

	return begin, data
}

// SayHello implements helloworld.GreeterServer
func (s *server) Query(ctx context.Context, in *pbCollector.QueryRequest) (*pbCollector.QueryResponse, error) {
	var retInfoList []*pbCore.InfoRange = make([]*pbCore.InfoRange, 1)
	retInfoList[0] = &pbCore.InfoRange{}

	// compose header
	var header pbCore.Header = pbCore.Header{
		Name:      "mockCollector",
		Timestamp: 1024,
		Count:     60,
		Step:      1,
	}

	// compose data
	var keyList []string = []string{"key1", "key2", "key3", "key4", "key5"}
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, uint32(len(keyList)))
	for _, key := range keyList {
		// check_t3 := time.Now()
		binary.Write(bytesBuffer, binary.BigEndian, uint32(len(key)))
		bytesBuffer.WriteString(key)
		// var item core.Item = core.Item{Key: key}
		timeBegin, data := queryRange(key, in.QueryList[0].Timebegin, in.QueryList[0].Timeend)
		binary.Write(bytesBuffer, binary.BigEndian, uint32(len(data)))
		if data != nil {
			for i, it := range data {
				binary.Write(bytesBuffer, binary.BigEndian, timeBegin+uint32(i))
				binary.Write(bytesBuffer, binary.BigEndian, uint32(len(it)))
				bytesBuffer.Write(it)
			}
		}
	}
	retInfoList[0].Header = &header
	retInfoList[0].Data = bytesBuffer.Bytes()

	return &pbCollector.QueryResponse{Errno: 0, Errmsg: "suucess", InfoRangeList: retInfoList}, nil
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pbCollector.RegisterCollectorServer(s, &server{})
	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
