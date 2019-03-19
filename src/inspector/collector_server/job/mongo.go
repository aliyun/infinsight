package job

import (
	"fmt"

	"inspector/cache"
	"inspector/collector_server/connector"
	"inspector/collector_server/job/mongoSteps"
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
	mongoStepCollect   = "Collect"
	mongoStepReadeFile = "ReadeFile" // debug
	mongoStepParse     = "Parse"
	mongoStepStore     = "Store"
	mongoStepCompress  = "Compress"
	mongoStepSend      = "Send"
)

type MongoJob struct {
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

func (mj *MongoJob) Equip(debug bool) error {
	// step 1. collect
	var step1 workflow.StepInterface
	if debug {
		step1 = mj.CreateStep(mongoStepReadeFile)
	} else {
		step1 = mj.CreateStep(mongoStepCollect)
	}

	if err := mj.TCB.AddWorkflowStep(step1); err != nil {
		return fmt.Errorf("add stepCollect error[%v]", err)
	}

	step2 := mj.CreateStep(mongoStepParse)
	if err := mj.TCB.AddWorkflowStep(step2); err != nil {
		return fmt.Errorf("add stepParse error[%v]", err)
	}

	step3 := mj.CreateStep(mongoStepStore, model.NewTimePoint(mj.Instance.Interval, mj.Instance.Count))
	if err := mj.TCB.AddWorkflowStep(step3); err != nil {
		return fmt.Errorf("add stepStore error[%v]", err)
	}

	step4 := mj.CreateStep(mongoStepCompress)
	if err := mj.TCB.AddWorkflowStep(step4); err != nil {
		return fmt.Errorf("add stepCompress error[%v]", err)
	}

	step5 := mj.CreateStep(mongoStepSend)
	if err := mj.TCB.AddWorkflowStep(step5); err != nil {
		return fmt.Errorf("add stepSend error[%v]", err)
	}

	return nil
}

func (mj *MongoJob) GetTCB() *scheduler.TCB {
	return mj.TCB
}

func (mj *MongoJob) GetRingCache() *cache.RingCache {
	return mj.RingCache
}

func (mj *MongoJob) GetBaseInfo() (int, int) {
	return mj.Instance.Interval, mj.Instance.Count
}

func (mj *MongoJob) GetConnector() connector.Connector {
	return mj.Connector
}

func (mj *MongoJob) CreateStep(name string, params ...interface{}) workflow.StepInterface {
	switch name {
	case mongoStepCollect:
		return &mongoSteps.StepCollect{Id: mongoStepCollect, Instance: mj.Instance,
			Connector: mj.Connector, ServiceName: mj.ServiceName}
	case mongoStepParse:
		return mongoSteps.NewStepParse(mongoStepParse, mj.ServiceName, mj.Instance,
			whatson.NewParser(whatson.Bson), mj.Ds)
	case mongoStepStore:
		return &mongoSteps.StepStore{Id: mongoStepStore, Instance: mj.Instance,
			RingCache: mj.RingCache, TP: params[0].(*model.TimePoint),
			CompressContext: model.NewCompressContext(0), ServiceName: mj.ServiceName}
	case mongoStepCompress:
		return &mongoSteps.StepCompress{Id: mongoStepCompress, Instance: mj.Instance,
			JobName: mj.Hb.Conf.Service, RingCache: mj.RingCache, Ds: mj.Ds,
			TCB: mj.TCB, ServiceName: mj.ServiceName}
	case mongoStepSend:
		return &mongoSteps.StepSend{Id: mongoStepSend, Instance: mj.Instance,
			SenderMsgChan: mj.SenderMsgChan, ServiceName: mj.ServiceName}
	case mongoStepReadeFile:
		return &mongoSteps.StepReadFile{Id: mongoStepCollect, Instance: mj.Instance,
			Connector: mj.Connector, ServiceName: mj.ServiceName}
	default:
		return nil
	}
}
