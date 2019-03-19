package httpJsonSteps

import (
	"inspector/collector_server/metric"
	"inspector/collector_server/model"

	"github.com/golang/glog"
)

// send data
type StepSend struct {
	Id            string                      // id == name
	Instance      *model.Instance             // ip:port
	SenderMsgChan chan<- *model.SenderContext // message channel

	// inner value
	errG error // global error, maybe covered

	ServiceName string
}

func (ss *StepSend) Name() string {
	return ss.Id
}

func (ss *StepSend) Error() error {
	return ss.errG
}

func (ss *StepSend) Before(input interface{}, params ...interface{}) (bool, error) {
	return true, nil
}

func (ss *StepSend) DoStep(input interface{}, params ...interface{}) (interface{}, error) {
	glog.V(2).Infof("step[%s] instance-name[%s] with service[%s] called",
		ss.Id, ss.Instance.Addr, ss.Instance.DBType)
	if input == nil {
		// input is null, do nothing
		return nil, nil
	}

	// update metric
	metric.GetMetric(ss.ServiceName).AddStepCount(ss.Id)

	msg := input.(*model.SenderContext)

	glog.Infof("step[%s] instance-name[%s] with service[%s] send file with begin time[%v], items[%d], count[%d], step[%d]",
		ss.Id, ss.Instance.Addr, ss.Instance.DBType, msg.Timestamp, len(msg.Mp), msg.Count, msg.Step)

	ss.SenderMsgChan <- msg

	return nil, nil
}

func (ss *StepSend) After(input interface{}, params ...interface{}) (bool, error) {
	//if ss.errG != nil {
	//	return false, nil
	//}
	return true, nil
}
