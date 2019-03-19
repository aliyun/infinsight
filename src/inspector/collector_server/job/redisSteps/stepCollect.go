package redisSteps

import (
	"inspector/collector_server/connector"
	"inspector/collector_server/metric"
	"inspector/collector_server/model"

	"github.com/golang/glog"
)

// collect data
type StepCollect struct {
	Id          string              // id == name
	Instance    *model.Instance     // ip:port
	Connector   connector.Connector // use to connect db, get and parse data
	data        [][]byte            // returned data
	errG        error               // global error
	ServiceName string
}

func (sc *StepCollect) Name() string {
	return sc.Id
}

func (sc *StepCollect) Error() error {
	return sc.errG
}

func (sc *StepCollect) Before(input interface{}, params ...interface{}) (bool, error) {
	return true, nil
}

func (sc *StepCollect) DoStep(input interface{}, params ...interface{}) (interface{}, error) {
	glog.V(2).Infof("step[%s] instance-name[%s] with service[%s] called",
		sc.Id, sc.Instance.Addr, sc.Instance.DBType)

	// update metric
	metric.GetMetric(sc.ServiceName).AddStepCount(sc.Id)

	var ret interface{}
	ret, sc.errG = sc.Connector.Get()
	if sc.errG != nil {
		glog.Errorf("step[%s] instance-name[%s] with service[%s] get data error[%v]",
			sc.Id, sc.Instance.Addr, sc.errG, sc.Instance.DBType)
		return []byte{}, nil
	}
	sc.data = ret.([][]byte)

	return sc.data, nil
}

func (sc *StepCollect) After(input interface{}, params ...interface{}) (bool, error) {
	//if len(sc.data) == 0 {
	//	return false, nil
	//}
	return true, nil
}
