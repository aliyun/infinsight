// read from file, this step only be used in debug
package mongoSteps

import (
	"inspector/collector_server/connector"
	"inspector/collector_server/model"

	"github.com/golang/glog"
)

type StepReadFile struct {
	Id        string              // id == name
	Instance  *model.Instance     // ip:port
	Connector connector.Connector // use to connect db, get and parse data

	data        [][]byte // returned data
	errG        error    // global error
	ServiceName string
}

func (srf *StepReadFile) Name() string {
	return srf.Id
}

func (srf *StepReadFile) Error() error {
	return srf.errG
}

func (srf *StepReadFile) Before(input interface{}, params ...interface{}) (bool, error) {
	return true, nil
}

func (srf *StepReadFile) DoStep(input interface{}, params ...interface{}) (interface{}, error) {
	glog.V(2).Infof("step[%s] instance-name[%s]metric.GetMetric(util.Mongo) called",
		srf.Id, srf.Instance.Addr, srf.Instance.DBType)

	var ret interface{}
	ret, srf.errG = srf.Connector.Get()
	if srf.errG != nil {
		glog.Errorf("step[%s] instance-name[%s]metric.GetMetric(util.Mongo): get data error[%v]",
			srf.Id, srf.Instance.Addr, srf.Instance.DBType, srf.errG)
	}
	srf.data = ret.([][]byte)

	return srf.data, srf.errG
}

func (srf *StepReadFile) After(input interface{}, params ...interface{}) (bool, error) {
	if len(srf.data) == 0 {
		return false, nil
	}
	return true, nil
}
