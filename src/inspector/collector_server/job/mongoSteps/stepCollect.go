package mongoSteps

import (
	"fmt"
	"os"

	"inspector/collector_server/connector"
	"inspector/collector_server/metric"
	"inspector/collector_server/model"
	"inspector/collector_server/restful"
	"inspector/util"

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

	// for debug
	//time.Sleep(3 * time.Second)

	var ret interface{}
	ret, sc.errG = sc.Connector.Get()
	if sc.errG != nil {
		glog.Errorf("step[%s] instance-name[%s] with service[%s] get data error[%v]",
			sc.Id, sc.Instance.Addr, sc.Instance.DBType, sc.errG)
		return [][]byte{}, nil
	}
	sc.data = ret.([][]byte)

	if sc.errG == nil && restful.IsDebugOpen() {
		sc.storeDebugFile(sc.data)
	}

	//return sc.data, sc.errG
	return sc.data, nil
}

func (sc *StepCollect) After(input interface{}, params ...interface{}) (bool, error) {
	//if len(sc.data) == 0 {
	//	return false, nil
	//}
	return true, nil
}

func (sc *StepCollect) storeDebugFile(input [][]byte) {
	directory := restful.DebugPrint.Position
	// create directory if not exists
	if _, err := os.Stat(directory); os.IsNotExist(err) {
		glog.Infof("step[%s] instance-name[%s] with service[%s]: directory[%s] not exists, try to create one",
			sc.Id, sc.Instance.Addr, sc.Instance.DBType, directory)
		if err = os.Mkdir(directory, util.DirPerm); err != nil && !os.IsExist(err) {
			glog.Errorf("step[%s] instance-name[%s] with service[%s]: mkdir directory[%s] fail[%v]",
				sc.Id, sc.Instance.Addr, sc.Instance.DBType, directory, err)
			return
		}
	}

	filename := fmt.Sprintf("%s/%s", directory, sc.Instance.Addr)
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		glog.Errorf("step[%s] instance-name[%s] with service[%s]: open file error[%v]",
			sc.Id, sc.Instance.Addr, sc.Instance.DBType, err)
		return
	}
	defer f.Close()

	for _, data := range input {
		f.Write(data)
		f.Write([]byte{util.Newline})
	}
	f.WriteString(util.DebugFileSpilter)
	f.Write([]byte{util.Newline})
}
