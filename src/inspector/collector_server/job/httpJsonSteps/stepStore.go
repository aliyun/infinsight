package httpJsonSteps

import (
	"fmt"
	"math"
	"reflect"
	"time"

	"inspector/cache"
	"inspector/collector_server/metric"
	"inspector/collector_server/model"
	"inspector/util"

	"github.com/golang/glog"
)

const (
	calculatePointOK             = 0
	calculatePointCurrentInvalid = 1
	calculatePointPreviousHole   = 2
)

var (
	deviation uint32 = 2 // the error is considered normal within [-{deviation}, +{deviation}] seconds.
)

// store data
type StepStore struct {
	Id              string                 // id == name
	Instance        *model.Instance        // ip:port
	RingCache       *cache.RingCache       // ring cache used to store data
	TP              *model.TimePoint       // global timestamp mark the start time of every minute(nanosecond)
	CompressContext *model.CompressContext // compress context

	errG error // global error

	ServiceName string
}

func (ss *StepStore) Name() string {
	return ss.Id
}

func (ss *StepStore) Error() error {
	return ss.errG
}

func (ss *StepStore) Before(input interface{}, params ...interface{}) (bool, error) {
	return true, nil
}

// trim float data
func (ss *StepStore) DoStep(input interface{}, params ...interface{}) (interface{}, error) {
	glog.V(2).Infof("step[%s] instance-name[%s] with service[%s] called",
		ss.Id, ss.Instance.Addr, ss.Instance.DBType)

	// update metric
	metric.GetMetric(ss.ServiceName).AddStepCount(ss.Id)

	mp := input.(map[int]interface{})

	now := uint32(time.Now().Unix()) / ss.TP.Frequency

	prevStart := ss.TP.Start
	// calculate store point
	curPoint, prePoint, status := calculatePoint(now, ss.TP)

	// fmt.Printf("curPoint:%v prePoint:%v status:%v\n", curPoint, prePoint, status)
	if status == calculatePointCurrentInvalid {
		glog.Warningf("step[%s] instance-name[%s] with service[%s] invalid point at time[%v]",
			ss.Id, ss.Instance.Addr, ss.Instance.DBType, now)
		// need discard
		return nil, nil
	}

	var ret interface{} // init nil
	curStart := ss.TP.Start

	if prevStart != curStart && prevStart != 0 { // data accumulate more than 1 minute
		ss.CompressContext.TimestampBeg, ss.CompressContext.TimestampEnd =
			chooseRing(prevStart, curStart, ss.TP.RingCount, ss.Instance.Addr)
		ret = ss.CompressContext // return to the next step
		glog.Infof("step[%s] instance-name[%s] with service[%s] data accumulate more than 1 minute[%d, %d]",
			ss.Id, ss.Instance.Addr, ss.Instance.DBType, ss.CompressContext.TimestampBeg, ss.CompressContext.TimestampEnd)

		defer func() {
			// reset CompressContext after return
			ss.CompressContext = model.NewCompressContext(len(mp))
		}()
	}

	// add invalidPoint value when point is a hole
	if status == calculatePointPreviousHole {
		glog.Warningf("step[%s] instance-name[%s] with service[%s]: meets hole curPoint[%d] prePoint[%d]",
			ss.Id, ss.Instance.Addr, ss.Instance.DBType, curPoint, prePoint)
		prev := math.Max(float64(prePoint), float64(curPoint-2*ss.TP.RingCount+1))
		for key := range mp {
			for i := uint32(prev); i < curPoint; i++ {
				if err := ss.RingCache.PushBack(key, i, util.NullData); err != nil {
					glog.Warningf("step[%s] instance-name[%s] with service[%s]: push into ring cache failed[%v], key[%d] value[invalidPoint:%d]",
						ss.Id, ss.Instance.Addr, ss.Instance.DBType, err, key, util.NullData)
					continue
				}

				ss.CompressContext.Update(key, util.NullData)
			}
		}
	}

	for key, val := range mp {
		x, err := ss.parseValue(val)
		if err != nil {
			glog.Warningf("step[%s] instance-name[%s] with service[%s]: parse key[%v] error[%v]",
				ss.Id, ss.Instance.Addr, ss.Instance.DBType, key, err)
			continue
		}

		if err := ss.RingCache.PushBack(key, curPoint, x); err != nil {
			glog.Warningf("step[%s] instance-name[%s] with service[%s]: push into ring cache failed[%v], key[%d] value[%v]",
				ss.Id, ss.Instance.Addr, ss.Instance.DBType, err, key, x)
		}
		ss.CompressContext.Update(key, x)
	}

	// return nil if not cross 1 minute
	return ret, nil
}

func (ss *StepStore) After(input interface{}, params ...interface{}) (bool, error) {
	return true, nil
}

func (ss *StepStore) parseValue(input interface{}) (ret int64, err error) {
	switch v := input.(type) {
	case int:
		ret = int64(v) * util.FloatMultiple
	case uint:
		ret = int64(v) * util.FloatMultiple
	case int32:
		ret = int64(v) * util.FloatMultiple
	case uint32:
		ret = int64(v) * util.FloatMultiple
	case bool:
		if v == true {
			ret = 1
		} else {
			ret = 0
		}
	case int64:
		ret = v * util.FloatMultiple
	case uint64:
		ret = int64(v) * util.FloatMultiple
	case float64:
		ret = int64(v) * util.FloatMultiple
	case float32:
		ret = int64(v) * util.FloatMultiple
	default:
		err = fmt.Errorf("unknown type[%v]", reflect.TypeOf(v))
	}

	return ret, err
}

/*
 * calculate the ring cache point to store data.
 * return current store point and previous store point if current and previous are not continuous
 * return both -1 if current node is illegal
 */
func calculatePoint(now uint32, tp *model.TimePoint) (uint32, uint32, int) {
	if tp.Start == 0 {
		tp.Start = now
		tp.Previous = now
		tp.Step = 0
	}

	// now is in {deviation} seconds latter than previous
	if now <= tp.Previous+deviation {
		if now <= tp.Start+tp.Step-deviation { // now is {deviation} seconds latter
			// current data is invalid
			return 0, 0, calculatePointCurrentInvalid
		} else {
			tp.Previous = tp.Start + tp.Step
			tp.Step = (tp.Step + 1) % tp.RingCount // mark as next
			if tp.Step == 0 {
				tp.Start += tp.RingCount
			}
			return tp.Previous, 0, calculatePointOK
		}
	} else {
		// now is in [{deviation}, 120) seconds latter than previous
		gap := now - tp.Previous
		// current step is lost
		if tp.Step+gap >= tp.RingCount {
			ring := (tp.Step + gap) / tp.RingCount
			tp.Start += ring * tp.RingCount
		}
		tp.Step = (tp.Step + gap) % tp.RingCount

		defer func() {
			tp.Previous = now
		}()
		return now, tp.Previous + 1, calculatePointPreviousHole
	}
}

// choose the return ring, return: the start time of ring, the end time of ring.
func chooseRing(prevStart, curStart uint32, ringCount uint32, ins string) (uint32, uint32) {
	if prevStart+ringCount-1 > curStart {
		glog.Warningf("step[step-store] instance-name[%s] with service[redis-proxy] interval [%d, %d] illegal",
			ins, prevStart, curStart)
		return prevStart, curStart
	}

	if (curStart-prevStart)%ringCount != 0 {
		glog.Errorf("step[step-store] instance-name[%s] with service[redis-proxy] current-ring-start[%d] != previous-ring-start[%d]",
			ins, curStart, prevStart)
		prevStart = curStart - ringCount
	}

	// the [TimestampBeg, TimestampEnd] may bigger than 120 seconds, leave along.
	if prevStart != curStart-ringCount { // gap is 1 ring
		glog.Errorf("step[step-store] instance-name[%s] with service[redis-proxy] the gap of current-ring-start[%d] and previous-ring-start[%d] bigger than 1 ring count",
			ins, curStart, prevStart)
		prevStart = curStart - ringCount
	}

	return prevStart, prevStart + ringCount - 1
}
