package job

import (
	"fmt"

	"inspector/cache"
	"inspector/collector_server/connector"
	"inspector/collector_server/job/httpJsonSteps"
	"inspector/collector_server/model"
	"inspector/config"
	"inspector/dict_server"
	"inspector/heartbeat"
	"inspector/util/scheduler"
	"inspector/util/whatson"
	"inspector/util/workflow"
)

const (
	// these name must equal to the name in the metric
	httpJsonStepCollect  = "Collect"
	httpJsonStepParse    = "Parse"
	httpJsonStepStore    = "Store"
	httpJsonStepCompress = "Compress"
	httpJsonStepSend     = "Send"
)

type HttpJsonJob struct {
	TCB           *scheduler.TCB              // TCB
	Connector     connector.Connector         // used to connect to database
	RingCache     *cache.RingCache            // store cache
	Cs            config.ConfigInterface      // config server, not owned
	Ds            *dictServer.DictServer      // dict server, not owned
	Hb            *heartbeat.Heartbeat        // heart beat server, not owned
	ServiceName   string                      // name: mongo3.4, redis4.0
	Instance      *model.Instance             // service name: ip:port
	SenderMsgChan chan<- *model.SenderContext // message channel
}

func (hjj *HttpJsonJob) Equip(debug bool) error {
	// step 1. collect
	step1 := hjj.CreateStep(httpJsonStepCollect)
	if err := hjj.TCB.AddWorkflowStep(step1); err != nil {
		return fmt.Errorf("add stepCollect error[%v]", err)
	}

	step2 := hjj.CreateStep(httpJsonStepParse)
	if err := hjj.TCB.AddWorkflowStep(step2); err != nil {
		return fmt.Errorf("add stepParse error[%v]", err)
	}

	step3 := hjj.CreateStep(httpJsonStepStore, model.NewTimePoint(hjj.Instance.Interval, hjj.Instance.Count))
	if err := hjj.TCB.AddWorkflowStep(step3); err != nil {
		return fmt.Errorf("add stepStore error[%v]", err)
	}

	step4 := hjj.CreateStep(httpJsonStepCompress)
	if err := hjj.TCB.AddWorkflowStep(step4); err != nil {
		return fmt.Errorf("add stepCompress error[%v]", err)
	}

	step5 := hjj.CreateStep(httpJsonStepSend)
	if err := hjj.TCB.AddWorkflowStep(step5); err != nil {
		return fmt.Errorf("add stepSend error[%v]", err)
	}

	return nil
}

func (hjj *HttpJsonJob) GetTCB() *scheduler.TCB {
	return hjj.TCB
}

func (hjj *HttpJsonJob) GetRingCache() *cache.RingCache {
	return hjj.RingCache
}

func (hjj *HttpJsonJob) GetBaseInfo() (int, int) {
	return hjj.Instance.Interval, hjj.Instance.Count
}

func (hjj *HttpJsonJob) GetConnector() connector.Connector {
	return hjj.Connector
}

func (hjj *HttpJsonJob) CreateStep(name string, params ...interface{}) workflow.StepInterface {
	switch name {
	case httpJsonStepCollect:
		return &httpJsonSteps.StepCollect{Id: httpJsonStepCollect, Instance: hjj.Instance,
			Connector: hjj.Connector, ServiceName: hjj.ServiceName}
	case httpJsonStepParse:
		return httpJsonSteps.NewStepParse(httpJsonStepParse, hjj.ServiceName, hjj.Instance,
			whatson.NewParser(whatson.Json), hjj.Ds)
	case httpJsonStepStore:
		return &httpJsonSteps.StepStore{Id: httpJsonStepStore, Instance: hjj.Instance,
			RingCache: hjj.RingCache, TP: params[0].(*model.TimePoint),
			CompressContext: model.NewCompressContext(0), ServiceName: hjj.ServiceName}
	case httpJsonStepCompress:
		return &httpJsonSteps.StepCompress{Id: httpJsonStepCompress, Instance: hjj.Instance,
			JobName: hjj.Hb.Conf.Service, RingCache: hjj.RingCache, Ds: hjj.Ds,
			TCB: hjj.TCB, ServiceName: hjj.ServiceName}
	case httpJsonStepSend:
		return &httpJsonSteps.StepSend{Id: httpJsonStepSend, Instance: hjj.Instance,
			SenderMsgChan: hjj.SenderMsgChan, ServiceName: hjj.ServiceName}
	default:
		return nil
	}
}
