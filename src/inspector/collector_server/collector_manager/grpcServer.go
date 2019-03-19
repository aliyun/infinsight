package collectorManager

import (
	"inspector/proto/core"
	"inspector/proto/collector"

	"golang.org/x/net/context"
)

const (
	okCode    = 0
	wrongCode = 1
)

type GrpcServer struct {
	cm *CollectorManager
}

func NewGrpcServer(cm *CollectorManager) *GrpcServer {
	return &GrpcServer{
		cm: cm,
	}
}

func (s *GrpcServer) Query(ctx context.Context, in *collector.CollectorQueryRequest) (*collector.CollectorQueryResponse, error) {
	var successInfoList []*core.InfoRange
	var failureInfoList []*core.InfoRange
	retErr := &core.Error{
		Errno:  okCode,
		Errmsg: "ok",
	}
	for _, query := range in.GetQueryList() {
		ret, errString := s.cm.QueryData(query)
		if errString != "" {
			failureInfoList = append(failureInfoList, &core.InfoRange{
				Header: query.Header,
				Error: &core.Error{
					Errno:  wrongCode,
					Errmsg: errString,
				}})
			retErr.Errno = wrongCode
			retErr.Errmsg = "error exists"
		} else {
			successInfoList = append(successInfoList, ret)
		}
	}

	return &collector.CollectorQueryResponse{
		Error:       retErr,
		SuccessList: successInfoList,
		FailureList: failureInfoList,
	}, nil
}
