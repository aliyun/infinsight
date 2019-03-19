package job

import (
	"fmt"

	"inspector/cache"
	"inspector/collector_server/connector"
	"inspector/collector_server/job/mysqlSteps"
	"inspector/collector_server/model"
	"inspector/config"
	"inspector/dict_server"
	"inspector/heartbeat"
	"inspector/util/scheduler"
	"inspector/util/workflow"
)

const (
	// these name must equal to the name in the metric
	mysqlStepCollect   = "Collect"
	mysqlStepReadeFile = "ReadeFile" // debug
	mysqlStepParse     = "Parse"
	mysqlStepStore     = "Store"
	mysqlStepCompress  = "Compress"
	mysqlStepSend      = "Send"
)

type MysqlJob struct {
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

func (mj *MysqlJob) Equip(debug bool) error {
	// step 1. collect
	var step1 workflow.StepInterface
	if debug {
		step1 = mj.CreateStep(mysqlStepReadeFile)
	} else {
		step1 = mj.CreateStep(mysqlStepCollect)
	}

	if err := mj.TCB.AddWorkflowStep(step1); err != nil {
		return fmt.Errorf("add stepCollect error[%v]", err)
	}

	step2 := mj.CreateStep(mysqlStepParse)
	if err := mj.TCB.AddWorkflowStep(step2); err != nil {
		return fmt.Errorf("add stepParse error[%v]", err)
	}

	step3 := mj.CreateStep(mysqlStepStore, model.NewTimePoint(mj.Instance.Interval, mj.Instance.Count))
	if err := mj.TCB.AddWorkflowStep(step3); err != nil {
		return fmt.Errorf("add stepStore error[%v]", err)
	}

	step4 := mj.CreateStep(mysqlStepCompress)
	if err := mj.TCB.AddWorkflowStep(step4); err != nil {
		return fmt.Errorf("add stepCompress error[%v]", err)
	}

	step5 := mj.CreateStep(mysqlStepSend)
	if err := mj.TCB.AddWorkflowStep(step5); err != nil {
		return fmt.Errorf("add stepSend error[%v]", err)
	}

	return nil
}

func (mj *MysqlJob) GetTCB() *scheduler.TCB {
	return mj.TCB
}

func (mj *MysqlJob) GetRingCache() *cache.RingCache {
	return mj.RingCache
}

func (mj *MysqlJob) GetBaseInfo() (int, int) {
	return mj.Instance.Interval, mj.Instance.Count
}

func (mj *MysqlJob) GetConnector() connector.Connector {
	return mj.Connector
}

func (mj *MysqlJob) CreateStep(name string, params ...interface{}) workflow.StepInterface {
	switch name {
	case mysqlStepCollect:
		return &mysqlSteps.StepCollect{Id: mysqlStepCollect, Instance: mj.Instance,
			Connector: mj.Connector, ServiceName: mj.ServiceName}
	case mysqlStepParse:
		return mysqlSteps.NewStepParse(mysqlStepParse, mj.ServiceName, mj.Instance, mj.Ds)
	case mysqlStepStore:
		return &mysqlSteps.StepStore{Id: mysqlStepStore, Instance: mj.Instance,
			RingCache: mj.RingCache, TP: params[0].(*model.TimePoint),
			CompressContext: model.NewCompressContext(0), ServiceName: mj.ServiceName}
	case mysqlStepCompress:
		return &mysqlSteps.StepCompress{Id: mysqlStepCompress, Instance: mj.Instance,
			JobName: mj.Hb.Conf.Service, RingCache: mj.RingCache, Ds: mj.Ds,
			TCB: mj.TCB, ServiceName: mj.ServiceName}
	case mysqlStepSend:
		return &mysqlSteps.StepSend{Id: mysqlStepSend, Instance: mj.Instance,
			SenderMsgChan: mj.SenderMsgChan, ServiceName: mj.ServiceName}
	default:
		return nil
	}
}
