package job

import (
	"fmt"

	"inspector/cache"
	"inspector/collector_server/connector"
	"inspector/collector_server/job/redisSteps"
	"inspector/collector_server/model"
	"inspector/config"
	"inspector/dict_server"
	"inspector/heartbeat"
	"inspector/util/scheduler"
	"inspector/util/workflow"
)

const (
	// these name must equal to the name in the metric
	redisStepCollect   = "Collect"
	redisStepReadeFile = "ReadeFile" // debug
	redisStepParse     = "Parse"
	redisStepStore     = "Store"
	redisStepCompress  = "Compress"
	redisStepSend      = "Send"
)

type RedisJob struct {
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

func (rj *RedisJob) Equip(debug bool) error {
	// step 1. collect
	var step1 workflow.StepInterface
	if debug {
		step1 = rj.CreateStep(redisStepReadeFile)
	} else {
		step1 = rj.CreateStep(redisStepCollect)
	}

	if err := rj.TCB.AddWorkflowStep(step1); err != nil {
		return fmt.Errorf("add stepCollect error[%v]", err)
	}

	step2 := rj.CreateStep(redisStepParse)
	if err := rj.TCB.AddWorkflowStep(step2); err != nil {
		return fmt.Errorf("add stepParse error[%v]", err)
	}

	step3 := rj.CreateStep(redisStepStore, model.NewTimePoint(rj.Instance.Interval, rj.Instance.Count))
	if err := rj.TCB.AddWorkflowStep(step3); err != nil {
		return fmt.Errorf("add stepStore error[%v]", err)
	}

	step4 := rj.CreateStep(redisStepCompress)
	if err := rj.TCB.AddWorkflowStep(step4); err != nil {
		return fmt.Errorf("add stepCompress error[%v]", err)
	}

	step5 := rj.CreateStep(redisStepSend)
	if err := rj.TCB.AddWorkflowStep(step5); err != nil {
		return fmt.Errorf("add stepSend error[%v]", err)
	}

	return nil
}

func (rj *RedisJob) GetTCB() *scheduler.TCB {
	return rj.TCB
}

func (rj *RedisJob) GetRingCache() *cache.RingCache {
	return rj.RingCache
}

func (rj *RedisJob) GetBaseInfo() (int, int) {
	return rj.Instance.Interval, rj.Instance.Count
}

func (rj *RedisJob) GetConnector() connector.Connector {
	return rj.Connector
}

func (rj *RedisJob) CreateStep(name string, params ...interface{}) workflow.StepInterface {
	switch name {
	case redisStepCollect:
		return &redisSteps.StepCollect{Id: redisStepCollect, Instance: rj.Instance,
			Connector: rj.Connector, ServiceName: rj.ServiceName}
	case redisStepParse:
		return redisSteps.NewStepParse(redisStepParse, rj.ServiceName, rj.Instance, rj.Ds)
	case redisStepStore:
		return &redisSteps.StepStore{Id: redisStepStore, Instance: rj.Instance,
			RingCache: rj.RingCache, TP: params[0].(*model.TimePoint),
			CompressContext: model.NewCompressContext(0), ServiceName: rj.ServiceName}
	case redisStepCompress:
		return &redisSteps.StepCompress{Id: redisStepCompress, Instance: rj.Instance,
			JobName: rj.Hb.Conf.Service, RingCache: rj.RingCache, Ds: rj.Ds,
			TCB: rj.TCB, ServiceName: rj.ServiceName}
	case redisStepSend:
		return &redisSteps.StepSend{Id: redisStepSend, Instance: rj.Instance,
			SenderMsgChan: rj.SenderMsgChan, ServiceName: rj.ServiceName}
	default:
		return nil
	}
}
