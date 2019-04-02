package metric

import (
	"reflect"
	"sync/atomic"
	"time"
	"unsafe"

	"inspector/util"

	"github.com/golang/glog"
	"sync"
)

const (
	updateInterval        = 10  // seconds
	beginningWaitInterval = 120 // seconds
)

var (
	// MetricVar *Metric
	MetricMap = new(sync.Map)
)

func GetMetric(tp string) *Metric {
	metric, _ := MetricMap.Load(tp)
	return metric.(*Metric)
}

type Percent struct {
	Dividend uint64
	Divisor  uint64
}

func (p *Percent) Set(dividend, divisor uint64) {
	atomic.AddUint64(&p.Dividend, dividend)
	atomic.AddUint64(&p.Divisor, divisor)
}

// input: return string?
func (p *Percent) Get(returnString bool) interface{} {
	if divisor := atomic.LoadUint64(&p.Divisor); divisor == 0 {
		if returnString {
			return "null"
		} else {
			return uint64(util.INT64_MAX)
		}
	} else {
		dividend := atomic.LoadUint64(&p.Dividend)
		if returnString {
			return float64(dividend) / float64(divisor)
			// return fmt.Sprintf("%.02f", float64(dividend)/float64(divisor))
		} else {
			return dividend / divisor
		}
	}
}

type Delta struct {
	Value    uint64 // current value
	Delta    uint64 // delta
	previous uint64 // previous value
}

func (d *Delta) Update() {
	current := atomic.LoadUint64(&d.Value)
	d.Delta, d.previous = current-d.previous, current
}

type Combine struct {
	Total uint64 // total number
	Delta        // delta
}

func (c *Combine) Set(val uint64) {
	atomic.AddUint64(&c.Delta.Value, val)
	atomic.AddUint64(&(c.Total), val)
}

// each step count
type StepCount struct {
	Collect  uint64
	Parse    uint64
	Store    uint64
	Compress uint64
	Send     uint64
}

type Numerical struct {
	Max uint64
	Min uint64
	Avg Percent
}

func NewNumerical() *Numerical {
	return &Numerical{
		Max: 0,
		Min: uint64(util.INT64_MAX),
	}
}

// main struct
type Metric struct {
	Items                    *Numerical // items info: max, min, average
	ItemsEmptyPercent        Percent    // the percentage of empty items
	BytesGet                 Combine    // how many bytes we get for total and every minute
	BytesSend                Combine    // how many bytes we send after compressing for total and every minute
	BytesSendClient          *sync.Map  // bytes send of every grpc client, int -> Combine
	SameDigitCompressPercent Percent    // the compress percentage of same digit algorithm
	DiffCompressPercent      Percent    // the compress percentage of diff algorithm
	TotalCompressPercent     Percent    // the compress percentage of total algorithm
	InstanceNumber           int32      // instance number
	StepRunTimes             *StepCount // count the step run times
	WorkflowDuration         *Numerical
	Uptime                   interface{}
}

func CreateMetric(tp string) {
	if _, ok := MetricMap.Load(tp); ok {
		return
	}

	metric := &Metric{
		Items:            NewNumerical(),
		StepRunTimes:     new(StepCount),
		WorkflowDuration: NewNumerical(),
		BytesSendClient:  new(sync.Map),
		Uptime:           time.Now(),
	}
	MetricMap.Store(tp, metric)
	go metric.run()
}

func (m *Metric) resetEverySecond(items []*Delta) {
	for _, item := range items {
		item.Update()
	}
}

func (m *Metric) run() {
	resetItems := []*Delta{&m.BytesGet.Delta, &m.BytesSend.Delta}
	time.Sleep(beginningWaitInterval * time.Second)
	go func() {
		tick := 0
		for range time.NewTicker(1 * time.Second).C {
			tick++
			m.resetEverySecond(resetItems)
			if tick%updateInterval != 0 {
				continue
			}

			glog.Infof("metric statistics: ItemsMax[%v], ItemsMin[%v], ItemsAvg[%v], ItemsEmpty[%v] "+
				"BytesGet_Total[%v], BytesGet_Delta[%v], BytesSend_Total[%v], BytesSend_Delta[%v], "+
				"SameDigitCompressPercent[%v],  DiffCompressPercent[%v], TotalCompressPercent[%v], "+
				"InstanceNumber[%v], StepRunTimes[%v], WorkflowDuration_Max[%v], WorkflowDuration_Min[%v], "+
				"WorkflowDuration_Avg[%v], Uptime[%v]",
				m.GetItemsMax(), m.GetItemsMin(), m.GetItemsAvg(), m.GetItemsEmpty(),
				util.ConvertTraffic(m.GetBytesGetTotal()), util.ConvertTraffic(m.GetBytesGetDelta()),
				util.ConvertTraffic(m.GetBytesSendTotal()), util.ConvertTraffic(m.GetBytesSendDelta()),
				m.GetSameDigitCompressPercent(), m.GetDiffCompressPercent(), m.GetTotalCompressPercent(),
				m.GetInstanceNumber(), m.GetStepCount(), m.GetWorkflowDurationMax(), m.GetWorkflowDurationMin(),
				m.GetWorkflowDurationAvg(), m.GetUptime())
		}
	}()
}

func (m *Metric) SetItems(val uint64) {
	preMax, preMin := atomic.LoadUint64(&m.Items.Max), atomic.LoadUint64(&m.Items.Min)
	if val > preMax {
		atomic.StoreUint64(&m.Items.Max, val)
	} else if val < preMin {
		atomic.StoreUint64(&m.Items.Min, val)
	}

	m.Items.Avg.Set(val, 1)
}

func (m *Metric) GetItemsMax() interface{} {
	return atomic.LoadUint64(&m.Items.Max)
}

func (m *Metric) GetItemsMin() interface{} {
	return atomic.LoadUint64(&m.Items.Min)
}

func (m *Metric) GetItemsAvg() interface{} {
	return m.Items.Avg.Get(true)
}

func (m *Metric) SetItemsEmpty(empty, whole uint64) {
	m.ItemsEmptyPercent.Set(empty, whole)
}

func (m *Metric) GetItemsEmpty() interface{} {
	return m.ItemsEmptyPercent.Get(true)
}

func (m *Metric) AddBytesGet(val uint64) {
	m.BytesGet.Set(val)
}

func (m *Metric) GetBytesGetDelta() uint64 {
	return atomic.LoadUint64(&m.BytesGet.Delta.Delta)
}

func (m *Metric) GetBytesGetTotal() uint64 {
	return atomic.LoadUint64(&m.BytesGet.Total)
}

func (m *Metric) AddBytesSend(val uint64) {
	m.BytesSend.Set(val)
}

func (m *Metric) GetBytesSendDelta() uint64 {
	return atomic.LoadUint64(&m.BytesSend.Delta.Delta)
}

func (m *Metric) GetBytesSendTotal() uint64 {
	return atomic.LoadUint64(&m.BytesSend.Total)
}

func (m *Metric) AddBytesSendClient(key interface{}, val uint64) {
	client, ok := m.BytesSendClient.Load(key)
	if !ok {
		client = new(Combine)
		m.BytesSendClient.Store(key, client)
	}
	c := client.(*Combine)
	c.Set(val)
}

func (m *Metric) GetBytesSendClient() interface{} {
	mp := make(map[string]interface{}, 4)
	m.BytesSendClient.Range(func(key, val interface{}) bool {
		v := val.(*Combine)
		k := key.(string)
		innerMap := make(map[string]interface{})
		innerMap["Total"] = atomic.LoadUint64(&v.Total)
		innerMap["Delta"] = atomic.LoadUint64(&v.Delta.Delta)
		mp[k] = innerMap
		return true
	})
	return mp
}

func (m *Metric) AddSameDigitCompressPercent(dividend, divisor uint64) {
	m.SameDigitCompressPercent.Set(dividend, divisor)
}

func (m *Metric) GetSameDigitCompressPercent() interface{} {
	return m.SameDigitCompressPercent.Get(true)
}

func (m *Metric) AddDiffCompressPercentPercent(dividend, divisor uint64) {
	m.DiffCompressPercent.Set(dividend, divisor)
}

func (m *Metric) GetDiffCompressPercent() interface{} {
	return m.DiffCompressPercent.Get(true)
}

func (m *Metric) GetTotalCompressPercent() interface{} {
	tot := &Percent{
		Dividend: atomic.LoadUint64(&m.SameDigitCompressPercent.Dividend) +
			atomic.LoadUint64(&m.DiffCompressPercent.Dividend),
		Divisor: atomic.LoadUint64(&m.SameDigitCompressPercent.Divisor) +
			atomic.LoadUint64(&m.DiffCompressPercent.Divisor),
	}
	return tot.Get(true)
}

func (m *Metric) AddInstanceNumber(val int32) {
	atomic.AddInt32(&m.InstanceNumber, val)
}

func (m *Metric) GetInstanceNumber() int32 {
	return atomic.LoadInt32(&m.InstanceNumber)
}

func (m *Metric) AddStepCount(name string) {
	field := reflect.ValueOf(m.StepRunTimes).Elem().FieldByName(name)
	if field.IsValid() == false {
		glog.Warningf("add step count with filed[%s] invalid", name)
		return
	}
	p := unsafe.Pointer(field.Addr().Pointer())
	atomic.AddUint64((*uint64)(p), 1)
}

func (m *Metric) GetStepCount() *StepCount {
	return m.StepRunTimes
}

func (m *Metric) AddWorkflowDuration(val uint64) {
	preMax, preMin := atomic.LoadUint64(&m.WorkflowDuration.Max),
		atomic.LoadUint64(&m.WorkflowDuration.Min)
	if val > preMax {
		atomic.StoreUint64(&m.WorkflowDuration.Max, val)
	} else if val < preMin {
		atomic.StoreUint64(&m.WorkflowDuration.Min, val)
	}

	m.WorkflowDuration.Avg.Set(val, 1)
}

func (m *Metric) GetWorkflowDurationMax() interface{} {
	val := atomic.LoadUint64(&m.WorkflowDuration.Max)
	return time.Duration(val).String()
}

func (m *Metric) GetWorkflowDurationMin() interface{} {
	val := atomic.LoadUint64(&m.WorkflowDuration.Min)
	return time.Duration(val).String()
}

func (m *Metric) GetWorkflowDurationAvg() interface{} {
	val := m.WorkflowDuration.Avg.Get(false).(uint64)
	return time.Duration(val).String()
}

func (m *Metric) SetUptime(val interface{}) {
	m.Uptime = val
}

func (m *Metric) GetUptime() interface{} {
	return m.Uptime
}
