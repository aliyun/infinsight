package model

import (
	"inspector/util"
)

type TimePoint struct {
	Start    uint32 // start time stamp
	Previous uint32 // previous time stamp
	Step     uint32 // the step id in recent 1 minute: 0, 1, 2, ..., 59, 0, ...

	Frequency uint32 // gap, e.g., 1 second, 5 seconds
	RingCount uint32 // every ring number
}

func NewTimePoint(frequency, count int) *TimePoint {
	tp := &TimePoint{
		Frequency: uint32(frequency),
		RingCount: uint32(count),
	}
	return tp
}

// --------------------splitter--------------------
// used in StepCompress and StepSend
type SenderContext struct {
	Mp           map[string][]byte
	Timestamp    uint32 // first point
	Count        uint32
	Step         uint32
	JobName      string
	InstanceName string
	Hid          int32
}

func NewSenderContext(jobName, instanceName string, hid int32, count, step uint32) *SenderContext {
	return &SenderContext{
		JobName:      jobName,
		InstanceName: instanceName,
		Mp:           make(map[string][]byte),
		Hid:          hid,

		Count: count,
		Step:  step,
	}
}

// --------------------splitter--------------------
type CompressValue struct {
	SameVal  int64 // store the same value
	SameFlag byte  // 0: unset, 1: set and same 2: set and not same
	GcdValue int64 // greatest common divisor
}

func NewCompressValue() *CompressValue {
	return &CompressValue{
		SameFlag: 0,
		GcdValue: 0,
	}
}

// used in StepStore and StepCompress
type CompressContext struct {
	TimestampBeg uint32                 // begin timestamp
	TimestampEnd uint32                 // end timestamp
	DataMp       map[int]*CompressValue // store different data, key: monitor item(ring buffer)
}

// input is the map size which is effective
func NewCompressContext(size int) *CompressContext {
	return &CompressContext{
		DataMp: make(map[int]*CompressValue, size),
	}
}

func (cc *CompressContext) Update(idx int, val int64) {
	cv, ok := cc.DataMp[idx]
	if !ok {
		cv = NewCompressValue()
		cc.DataMp[idx] = cv
	}

	switch cv.SameFlag {
	case 0:
		cv.SameFlag = 1
		cv.SameVal = val
		if val != util.NullData { // don't set gcd when value == null data
			cv.GcdValue = val
		}
	case 1:
		if cv.GcdValue == 0 && val != util.NullData { // set gcd if previous gcd == 0
			cv.GcdValue = val
		}
		if cv.SameVal != val {
			cv.SameFlag = 2
			if val != util.NullData {
				cv.GcdValue = util.GCD(cv.GcdValue, val)
			}
		}
	case 2:
		if val != util.NullData {
			cv.GcdValue = util.GCD(cv.GcdValue, val)
		}
	}

	if cv.GcdValue < 0 {
		cv.GcdValue = -cv.GcdValue
	}
}
