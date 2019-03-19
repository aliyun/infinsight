package mongoSteps

import (
	"flag"
	"fmt"
	"testing"
	"time"

	"inspector/cache"
	"inspector/collector_server/model"
	"inspector/compress"
	"inspector/dict_server"

	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
	"inspector/collector_server/metric"
	"inspector/util"
)

const (
	ringBufferCount = 120
	jobName         = "mongo"
	mongoAddress    = "100.81.245.155:20111" // config server
	mongoUsername   = "admin"
	mongoPassword   = "admin"
)

type Parameter struct {
	RingCache *cache.RingCache       // ring cache used to store data
	Ds        *dictServer.DictServer // dict server map string <-> int
}

func NewParameter(instanceName string) *Parameter {
	ringCache := new(cache.RingCache)
	ringCache.Init(instanceName, ringBufferCount)

	dictConf := &dictServer.Conf{
		Address:    mongoAddress,
		Username:   mongoUsername,
		Password:   mongoPassword,
		ServerType: jobName,
	}
	ds := dictServer.NewDictServer(dictConf, nil)
	if ds == nil {
		glog.Error("create dict server error")
		return nil
	}

	metric.CreateMetric()
	return &Parameter{
		RingCache: ringCache,
		Ds:        ds,
	}
}

func TestStepCompress(t *testing.T) {
	var (
		err           error
		p             *Parameter
		nr            int
		keyNr         int
		output        interface{}
		senderContext *model.SenderContext
		step          uint32
		sc            *StepCompress
	)

	flag.Set("stderrthreshold", "warning")
	// flag.Set("v", "2")

	p = NewParameter("test1")
	assert.NotEqual(t, nil, p, "should be equal")

	sc = &StepCompress{
		Id: "testCompress",
		Instance: &model.Instance{
			Addr:     "test instance name",
			Hid:      12345,
			Interval: 1,
			Count:    60,
		},
		RingCache: p.RingCache,
		Ds:        p.Ds,
	}

	// case, same value
	{
		nr++
		fmt.Printf("TestStepCompress case %d.\n", nr)

		err = p.Ds.DeleteAll()
		assert.Equal(t, nil, err, "should be equal")

		// dict server create 5 keys
		p.Ds.GetValue("test1")
		p.Ds.GetValue("test2")
		p.Ds.GetValue("test3")
		p.Ds.GetValue("test4")
		p.Ds.GetValue("test5")
		time.Sleep((dictServer.HandlerInterval + 1000) * time.Millisecond)

		keyNr = 5 // 5 key/offset
		step = 1

		compressContext := model.NewCompressContext(0)
		compressContext.TimestampBeg = 1000
		compressContext.TimestampEnd = 1030
		// count := (compressContext.TimestampEnd - compressContext.TimestampBeg) / step + 1
		count := uint32(60)

		for i := 0; i < keyNr; i++ {
			for j := compressContext.TimestampBeg; j < compressContext.TimestampEnd; j++ {
				val := int64(i * 100)
				sc.RingCache.PushBack(i, uint32(j), val)
				compressContext.Update(i, val)
			}
		}

		output, err = sc.DoStep(compressContext)
		assert.Equal(t, nil, err, "should be equal")
		senderContext = output.(*model.SenderContext)

		assert.Equal(t, compressContext.TimestampBeg, senderContext.Timestamp, "should be equal")
		assert.Equal(t, count, senderContext.Count, "should be equal")
		assert.Equal(t, step, senderContext.Step, "should be equal")
		assert.Equal(t, sc.Instance.Addr, senderContext.InstanceName, "should be equal")
		assert.Equal(t, sc.Instance.Hid, senderContext.Hid, "should be equal")
		for i := 0; i < keyNr; i++ {
			k := util.RepInt2String(i)
			key, err := p.Ds.GetKey(k)
			assert.Equal(t, nil, err, "should be equal")
			ret := senderContext.Mp[key]
			output, err := compress.Decompress(ret, nil)
			assert.Equal(t, nil, err, "should be equal")
			compareSameValue(int64(i*100), int(count), output)
		}
	}

	// case, same value
	{
		nr++
		fmt.Printf("TestStepCompress case %d.\n", nr)

		err = p.Ds.DeleteAll()
		assert.Equal(t, nil, err, "should be equal")

		keyNr = 2000 // 5 key/offset

		//create new ring cache
		sc.RingCache = new(cache.RingCache)
		sc.RingCache.Init("test2", ringBufferCount)

		// dict server create 5 keys
		for i := 0; i < keyNr; i++ {
			key := fmt.Sprintf("test-%d", i)
			p.Ds.GetValue(key)
		}
		time.Sleep((dictServer.HandlerInterval + 5000) * time.Millisecond)

		step = 1
		compressContext := model.NewCompressContext(0)
		compressContext.TimestampBeg = 1000
		compressContext.TimestampEnd = 1059
		count := (compressContext.TimestampEnd-compressContext.TimestampBeg)/step + 1

		for i := 0; i < keyNr; i++ {
			for j := compressContext.TimestampBeg; j < compressContext.TimestampEnd; j++ {
				val := int64(i * 100)
				sc.RingCache.PushBack(i, uint32(j), val)
				compressContext.Update(i, val)
			}
		}

		output, err = sc.DoStep(compressContext)
		assert.Equal(t, nil, err, "should be equal")
		senderContext = output.(*model.SenderContext)

		assert.Equal(t, compressContext.TimestampBeg, senderContext.Timestamp, "should be equal")
		assert.Equal(t, count, senderContext.Count, "should be equal")
		assert.Equal(t, step, senderContext.Step, "should be equal")
		assert.Equal(t, sc.Instance.Addr, senderContext.InstanceName, "should be equal")
		assert.Equal(t, sc.Instance.Hid, senderContext.Hid, "should be equal")
		for i := 0; i < keyNr; i++ {
			k := util.RepInt2String(i)
			key, err := p.Ds.GetKey(k)
			assert.Equal(t, nil, err, "should be equal")
			ret := senderContext.Mp[key]
			output, err := compress.Decompress(ret, nil)
			assert.Equal(t, nil, err, "should be equal")
			compareSameValue(int64(i*100), int(count), output)
		}
	}

	// case diff
	{
		nr++
		fmt.Printf("TestStepCompress case %d.\n", nr)

		keyNr = 3
		step = 1
		compressContext := model.NewCompressContext(0)
		compressContext.TimestampBeg = 1000
		compressContext.TimestampEnd = 1009
		// count := (compressContext.TimestampEnd - compressContext.TimestampBeg) / step + 1
		count := uint32(60)

		inputs := [][]int64{
			{
				1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			},
			{
				-1, -2, -3, -4, -5, -6, -7, -8, -9, 0,
			},
			{
				-(1 << 32), 1 << 40, -(1 << 50), 123123123123, 0, 0, 0, 0, 0, -(1 << 58) + 1,
			},
		}

		// create new ring cache
		sc.RingCache = new(cache.RingCache)
		sc.RingCache.Init(sc.Instance.Addr, ringBufferCount)
		for i := 0; i < keyNr; i++ {
			for j, val := range inputs[i] {
				sc.RingCache.PushBack(i, compressContext.TimestampBeg+uint32(j), val)
				compressContext.Update(i, val)
			}
		}

		output, err = sc.DoStep(compressContext)
		assert.Equal(t, nil, err, "should be equal")
		senderContext = output.(*model.SenderContext)

		assert.Equal(t, compressContext.TimestampBeg, senderContext.Timestamp, "should be equal")
		assert.Equal(t, count, senderContext.Count, "should be equal")
		assert.Equal(t, step, senderContext.Step, "should be equal")
		assert.Equal(t, sc.Instance.Addr, senderContext.InstanceName, "should be equal")
		assert.Equal(t, sc.Instance.Hid, senderContext.Hid, "should be equal")
		for i := 0; i < keyNr; i++ {
			// copy input
			diffListCopy := make([]int64, len(inputs[i]))
			copy(diffListCopy, inputs[i])

			k := util.RepInt2String(i)
			key, err := p.Ds.GetKey(k)
			assert.Equal(t, nil, err, "should be equal")
			ret := senderContext.Mp[key]
			output, err := compress.Decompress(ret, nil)
			assert.Equal(t, nil, err, "should be equal")
			compareDiffValue(diffListCopy, output)
		}
	}

	// case. only check gcd
	{
		nr++
		fmt.Printf("TestStepCompress case %d.\n", nr)

		compressContext := model.NewCompressContext(0)

		inputs := [][]int64{
			{
				2, 4, 6, 8, 10,
			},
			{
				1000, 2000, 3000, 4000, 5000,
			},
			{
				-10, -20, -30, -40, -50,
			},
			{
				0,
			},
		}

		for i := 0; i < len(inputs); i++ {
			for _, val := range inputs[i] {
				compressContext.Update(i, val)
			}
		}

		assert.Equal(t, int64(2), compressContext.DataMp[0].GcdValue, "should be equal")
		assert.Equal(t, int64(1000), compressContext.DataMp[1].GcdValue, "should be equal")
		assert.Equal(t, int64(10), compressContext.DataMp[2].GcdValue, "should be equal")
		assert.Equal(t, int64(0), compressContext.DataMp[3].SameVal, "should be equal")
	}

	// check length
	{
		nr++
		fmt.Printf("TestStepCompress case %d.\n", nr)

		keyNr = 2
		step = 1
		compressContext := model.NewCompressContext(0)
		compressContext.TimestampBeg = 1000
		compressContext.TimestampEnd = 1059
		count := (compressContext.TimestampEnd-compressContext.TimestampBeg)/step + 1

		inputs := [][]int64{
			{
				1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
			},
			{
				1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61,
			},
		}

		// create new ring cache
		sc.RingCache = new(cache.RingCache)
		sc.RingCache.Init(sc.Instance.Addr, ringBufferCount)
		for i := 0; i < keyNr; i++ {
			for j, val := range inputs[i] {
				sc.RingCache.PushBack(i, compressContext.TimestampBeg+uint32(j), val)
				compressContext.Update(i, val)
			}
		}

		output, err = sc.DoStep(compressContext)
		assert.Equal(t, nil, err, "should be equal")
		senderContext = output.(*model.SenderContext)

		assert.Equal(t, count, senderContext.Count, "should be equal")
	}

	// check length
	{
		nr++
		fmt.Printf("TestStepCompress case %d.\n", nr)

		sc.Instance = &model.Instance{
			Addr:     "test instance name2",
			Hid:      45678,
			Interval: 1,
			Count:    5,
		}
		keyNr = 2
		step = 1
		compressContext := model.NewCompressContext(0)
		compressContext.TimestampBeg = 1000
		compressContext.TimestampEnd = 1005
		// count := (compressContext.TimestampEnd - compressContext.TimestampBeg) / step + 1
		count := uint32(5)

		inputs := [][]int64{
			{
				1, 2, 3, 4, 5,
			},
			{
				1, 2, 3, 4, 5, 6,
			},
		}

		// create new ring cache
		sc.RingCache = new(cache.RingCache)
		sc.RingCache.Init(sc.Instance.Addr, 10)
		for i := 0; i < keyNr; i++ {
			for j, val := range inputs[i] {
				sc.RingCache.PushBack(i, compressContext.TimestampBeg+uint32(j), val)
				compressContext.Update(i, val)
			}
		}

		output, err = sc.DoStep(compressContext)
		assert.Equal(t, nil, err, "should be equal")
		senderContext = output.(*model.SenderContext)

		assert.Equal(t, count, senderContext.Count, "should be equal")
	}
}

func compareSameValue(sameValue int64, count int, valList []int64) bool {
	if count != len(valList) {
		return false
	}

	for _, val := range valList {
		if val != sameValue {
			return false
		}
	}
	return true
}

func compareDiffValue(diffList, valList []int64) bool {
	if len(diffList) != len(valList) {
		return false
	}

	for i := 0; i < len(diffList); i++ {
		if diffList[i] != valList[i] {
			return false
		}
	}
	return true
}
