package mysqlSteps

import (
	"fmt"
	"math"

	"inspector/cache"
	"inspector/collector_server/metric"
	"inspector/collector_server/model"
	"inspector/compress"
	"inspector/dict_server"
	"inspector/util"
	"inspector/util/scheduler"

	"github.com/golang/glog"
)

// compress data
type StepCompress struct {
	Id        string                 // id == name
	Instance  *model.Instance        // ip:port
	JobName   string                 // current collector server address: 100.1.1.1:2345
	RingCache *cache.RingCache       // ring cache used to store data
	Ds        *dictServer.DictServer // dict server map string <-> int
	TCB       *scheduler.TCB

	// inner value
	errG error // global error

	ServiceName string
}

func (sc *StepCompress) Name() string {
	return sc.Id
}

func (sc *StepCompress) Error() error {
	return sc.errG
}

func (sc *StepCompress) Before(input interface{}, params ...interface{}) (bool, error) {
	return true, nil
}

func (sc *StepCompress) DoStep(input interface{}, params ...interface{}) (interface{}, error) {
	if input == nil {
		// update metric, 0 is not valid
		if duration := sc.TCB.GetWorkflowDuration(); duration != 0 {
			metric.GetMetric(sc.ServiceName).AddWorkflowDuration(uint64(duration))
		}
		return nil, nil
	}
	glog.V(2).Infof("step[%s] instance-name[%s] with service[%s] called",
		sc.Id, sc.Instance.Addr, sc.Instance.DBType)

	// update metric
	metric.GetMetric(sc.ServiceName).AddStepCount(sc.Id)

	/*
	 * the [compressContext.TimestampBeg, compressContext.TimestampEnd] interval may bigger than
	 * 120 seconds(2 rings), so that the senderContext.Timestamp should be set based on the
	 * compressContext.TimestampEnd: max(compressContext.TimestampEnd - 60, compressContext.TimestampBeg)
	 */
	compressContext := input.(*model.CompressContext)

	// do compress
	senderContext := model.NewSenderContext(sc.JobName, sc.Instance.Addr, sc.Instance.Hid,
		uint32(sc.Instance.Count), uint32(sc.Instance.Interval))

	// get all offset
	offsets := sc.RingCache.GetMaxOffset()
	// it's acceptable that timeEnd < timeBeg + ringCount(60)
	count := int(math.Min(float64(sc.Instance.Count),
		float64(compressContext.TimestampEnd-compressContext.TimestampBeg+1)))
	if count != sc.Instance.Count {
		glog.Warningf("step[%s] instance-name[%s] with service[%s]: count[%v] != [%v]",
			sc.Id, sc.Instance.Addr, sc.Instance.DBType, count, sc.Instance.Count)
	}

	itemEmptyCount := 0
	arr := make([]int64, sc.Instance.Count*2)
	lenArr := len(arr)
	for i := 0; i < offsets; i++ {
		shortKey := util.RepInt2String(i)
		longKey, _ := sc.Ds.GetKey(shortKey) // get long key
		compressValue, ok := compressContext.DataMp[i]
		if !ok {
			/*
			 * this key isn't exist in the DataMp which means this key isn't stored in the
			 * past 1 minute. Or not exist in current instance because of dict-server value
			 * is not continuously and some key may not exist.
			 */
			glog.V(3).Infof("step[%s] instance-name[%s] with service[%s]: get offset[%d] short-key[%s] long-key[%s] empty",
				sc.Id, sc.Instance.Addr, sc.Instance.DBType, i, shortKey, longKey)
			continue
		}

		// fetch recent 1 minute data
		_, arr = sc.RingCache.Query(i, compressContext.TimestampEnd, arr) // first return value timestamp is useless here
		if arr == nil || len(arr) == 0 {
			glog.V(3).Infof("step[%s] instance-name[%s] with service[%s]: get offset[%d] short-key[%s] long-key[%s] ring cache query empty",
				sc.Id, sc.Instance.Addr, sc.Instance.DBType, i, shortKey, longKey)
			continue
		}
		// real used array
		usedArr := arr[lenArr-count:]

		var compressRes []byte
		var err error
		switch compressValue.SameFlag {
		case 0:
			fallthrough
		case 1:
			// all data is same, use sameDigitCompress
			compressRes, err = compress.Compress(compress.SameDigitCompress, count, compressValue.SameVal)
			itemEmptyCount++
		case 2:
			// not all the same
			compressRes, err = compress.Compress(compress.DiffCompress, compressValue.GcdValue, usedArr)
		default:
			err = fmt.Errorf("error flag[%d]", compressValue.SameFlag)
		}

		if err != nil {
			glog.Errorf("step[%s] instance-name[%s] with service[%s]: compress offset[%d] short-key[%s] long-key[%s] usedArr[%v] compressValue[%v] error[%v]",
				sc.Id, sc.Instance.Addr, sc.Instance.DBType, i, shortKey, longKey, usedArr, compressValue, err)
			continue
		}

		// modify to get short key instead of long key from dict-server
		if shortKey == util.InvalidShortKey {
			glog.Errorf("step[%s] instance-name[%s] with service[%s]: get offset[%d] short-key[%s] long-key[%s] error",
				sc.Id, sc.Instance.Addr, sc.Instance.DBType, i, shortKey, longKey)
		}

		senderContext.Mp[shortKey] = compressRes

		// update metric
		compressLen := uint64(len(compressRes))
		metric.GetMetric(sc.ServiceName).AddBytesSend(compressLen)
		if compressValue.SameFlag == 1 {
			metric.GetMetric(sc.ServiceName).AddSameDigitCompressPercent(uint64(count)*8, compressLen)
		} else if compressValue.SameFlag == 2 {
			metric.GetMetric(sc.ServiceName).AddDiffCompressPercentPercent(uint64(len(usedArr))*8, compressLen)
		}
	}

	if len(senderContext.Mp) == 0 {
		// still return even if all data empty which means pass to stepSend
		glog.Warningf("step[%s] instance-name[%s] with service[%s]: all data empty",
			sc.Id, sc.Instance.Addr, sc.Instance.DBType)
		// return nil, nil
	}

	// update metric
	metric.GetMetric(sc.ServiceName).SetItems(uint64(len(senderContext.Mp)))
	metric.GetMetric(sc.ServiceName).SetItemsEmpty(uint64(itemEmptyCount), uint64(len(senderContext.Mp)))

	senderContext.Timestamp = compressContext.TimestampEnd - uint32(count) + 1
	if senderContext.Timestamp != compressContext.TimestampBeg { // assert
		glog.Errorf("step[%s] instance-name[%s] with service[%s]: send timestamp[%d] != compress timestamp[%d]",
			sc.Id, sc.Instance.Addr, sc.Instance.DBType, senderContext.Timestamp, compressContext.TimestampBeg)
	}

	return senderContext, nil
}

func (sc *StepCompress) After(input interface{}, params ...interface{}) (bool, error) {
	return true, nil
}
