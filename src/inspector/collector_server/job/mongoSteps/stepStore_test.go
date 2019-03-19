package mongoSteps

import (
	"flag"
	"fmt"
	"inspector/cache"
	"reflect"
	"testing"
	"time"

	"inspector/collector_server/model"
	"inspector/util"

	"github.com/stretchr/testify/assert"
)

func TestCalculatePoint(t *testing.T) {
	var nr int

	deviation = 3
	// continuous value
	{
		nr++
		fmt.Printf("TestCalculatePoint case %d.\n", nr)

		tp := model.NewTimePoint(1, 60)

		for i := 0; i < 59; i++ {
			now := uint32(10 + i)
			current, previous, mark := calculatePoint(now, tp)
			assert.Equal(t, calculatePointOK, mark, "should be equal")
			assert.Equal(t, now, current, "should be equal")
			assert.Equal(t, uint32(0), previous, "should be equal")

			assert.Equal(t, uint32(i)+1, tp.Step, "should be equal")
			assert.Equal(t, uint32(10), tp.Start, "should be equal")
			assert.Equal(t, now, tp.Previous, "should be equal")
		}

		now := uint32(10 + 59)
		current, previous, mark := calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, now, current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")

		assert.Equal(t, uint32(0), tp.Step, "should be equal")
		assert.Equal(t, uint32(10+60), tp.Start, "should be equal")
		assert.Equal(t, now, tp.Previous, "should be equal")

		for i := 0; i < 60000; i++ {
			now := uint32(10 + 60 + i)
			current, previous, mark = calculatePoint(now, tp)
			assert.Equal(t, calculatePointOK, mark, "should be equal")
			assert.Equal(t, now, current, "should be equal")
			assert.Equal(t, uint32(0), previous, "should be equal")

			assert.Equal(t, uint32(i+1)%60, tp.Step, "should be equal")
			assert.Equal(t, uint32(10+60+(i+1)/60*60), tp.Start, "should be equal")
			assert.Equal(t, now, tp.Previous, "should be equal")
		}
	}

	// create hole [-deviation - 2, deviation + 2]
	{
		nr++
		fmt.Printf("TestCalculatePoint case %d.\n", nr)

		tp := model.NewTimePoint(1, 60)
		start := 10

		now := uint32(start)
		current, previous, mark := calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(start), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")

		// -deviation - 2
		now = uint32(start) - deviation - 2
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointCurrentInvalid, mark, "should be equal")
		assert.Equal(t, uint32(0), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")

		// -deviation - 1
		now = uint32(start) - deviation - 1
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointCurrentInvalid, mark, "should be equal")
		assert.Equal(t, uint32(0), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")

		// -deviation
		now = uint32(start) - deviation // current need is 11
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointCurrentInvalid, mark, "should be equal")
		assert.Equal(t, uint32(0), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")

		// -deviation
		now = uint32(start) - deviation + 1 // 8. current need is 11
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointCurrentInvalid, mark, "should be equal")
		assert.Equal(t, uint32(0), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")

		// -deviation + 1
		now = uint32(start) - deviation + 2 // 9. current need is 11
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(start+1), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")

		// -deviation + 1
		now = uint32(start) - deviation + 3 // 10. current need is 12
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(start+2), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")

		// -deviation + 1
		now = uint32(start) + deviation - 2 // 11. current need is 13
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(start+3), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")

		// -deviation
		now = uint32(start) + deviation - 2 // 11. current need is 14
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointCurrentInvalid, mark, "should be equal")
		assert.Equal(t, uint32(0), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")

		// -deviation + 1
		now = uint32(start) + deviation - 1 // 12. current need is 14
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(start+4), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")

		// -deviation + 1
		now = uint32(start) + deviation // 13. current need is 15
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(start+5), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")

		now = uint32(16) // 16. current need is 16
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(16), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")

		now = uint32(19) // 19. current need is 17
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(17), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")

		now = uint32(21) // 21. current need is 18
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointPreviousHole, mark, "should be equal")
		assert.Equal(t, uint32(21), current, "should be equal")
		assert.Equal(t, uint32(18), previous, "should be equal") // 18, 19, 20 need fill null-data, 21 is data

		now = uint32(22) // 22. current need is 22
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(22), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")

		now = uint32(25) // 25. current need is 23
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(23), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")
	}

	// cross ring: 60s
	{
		nr++
		fmt.Printf("TestCalculatePoint case %d.\n", nr)

		tp := model.NewTimePoint(1, 60)

		now := uint32(10)
		current, previous, mark := calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(10), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")
		assert.Equal(t, uint32(10), tp.Start, "should be equal")
		assert.Equal(t, uint32(1), tp.Step, "should be equal")

		now = uint32(50)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointPreviousHole, mark, "should be equal")
		assert.Equal(t, uint32(50), current, "should be equal")
		assert.Equal(t, uint32(11), previous, "should be equal")
		assert.Equal(t, 10, int(tp.Start), "should be equal")
		assert.Equal(t, uint32((50-10+1)%60), tp.Step, "should be equal")

		now = uint32(80)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointPreviousHole, mark, "should be equal")
		assert.Equal(t, uint32(80), current, "should be equal")
		assert.Equal(t, uint32(51), previous, "should be equal")
		assert.Equal(t, 70, int(tp.Start), "should be equal")
		assert.Equal(t, uint32((80-10+1)%60), tp.Step, "should be equal")

		now = uint32(81)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(81), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")
		assert.Equal(t, 70, int(tp.Start), "should be equal")
		assert.Equal(t, uint32((81-10+1)%60), tp.Step, "should be equal")

		now = uint32(130)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointPreviousHole, mark, "should be equal")
		assert.Equal(t, uint32(130), current, "should be equal")
		assert.Equal(t, uint32(82), previous, "should be equal")
		assert.Equal(t, 130, int(tp.Start), "should be equal")
		assert.Equal(t, uint32(1), tp.Step, "should be equal")

		now = uint32(190)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointPreviousHole, mark, "should be equal")
		assert.Equal(t, uint32(190), current, "should be equal")
		assert.Equal(t, uint32(131), previous, "should be equal")
		assert.Equal(t, 190, int(tp.Start), "should be equal")
		assert.Equal(t, uint32(1), tp.Step, "should be equal")

		now = uint32(190 + 60000000)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointPreviousHole, mark, "should be equal")
		assert.Equal(t, uint32(190+60000000), current, "should be equal")
		assert.Equal(t, uint32(191), previous, "should be equal")
		assert.Equal(t, 190+60000000, int(tp.Start), "should be equal")
		assert.Equal(t, uint32(1), tp.Step, "should be equal")

		now = uint32(190 + 60000001)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(190+60000001), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")
		assert.Equal(t, 190+60000000, int(tp.Start), "should be equal")
		assert.Equal(t, uint32(2), tp.Step, "should be equal")

		now = uint32(10 + 600000000)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointPreviousHole, mark, "should be equal")
		assert.Equal(t, uint32(10+600000000), current, "should be equal")
		assert.Equal(t, uint32(190+60000002), previous, "should be equal")
		assert.Equal(t, 10+600000000, int(tp.Start), "should be equal")
		assert.Equal(t, uint32(1), tp.Step, "should be equal")
	}

	// cross ring: 60s
	{
		nr++
		fmt.Printf("TestCalculatePoint case %d.\n", nr)

		tp := model.NewTimePoint(1, 60)

		now := uint32(10)
		current, previous, mark := calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(10), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")
		assert.Equal(t, uint32(10), tp.Start, "should be equal")
		assert.Equal(t, uint32(1), tp.Step, "should be equal")

		now = uint32(69)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointPreviousHole, mark, "should be equal")
		assert.Equal(t, uint32(69), current, "should be equal")
		assert.Equal(t, uint32(11), previous, "should be equal")
		assert.Equal(t, 70, int(tp.Start), "should be equal")
		assert.Equal(t, uint32(0), tp.Step, "should be equal")

		now = uint32(128)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointPreviousHole, mark, "should be equal")
		assert.Equal(t, uint32(128), current, "should be equal")
		assert.Equal(t, uint32(70), previous, "should be equal")
		assert.Equal(t, 70, int(tp.Start), "should be equal")
		assert.Equal(t, uint32(59), tp.Step, "should be equal")

		now = uint32(129)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(129), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")
		assert.Equal(t, 130, int(tp.Start), "should be equal")
		assert.Equal(t, uint32(0), tp.Step, "should be equal")

		now = uint32(130)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(130), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")
		assert.Equal(t, 130, int(tp.Start), "should be equal")
		assert.Equal(t, uint32(1), tp.Step, "should be equal")
	}

	{
		nr++
		fmt.Printf("TestCalculatePoint case %d.\n", nr)

		tp := model.NewTimePoint(1, 60)

		now := uint32(10)
		current, previous, mark := calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(10), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")
		assert.Equal(t, uint32(10), tp.Start, "should be equal")
		assert.Equal(t, uint32(1), tp.Step, "should be equal")

		now = uint32(608)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointPreviousHole, mark, "should be equal")
		assert.Equal(t, uint32(608), current, "should be equal")
		assert.Equal(t, uint32(11), previous, "should be equal")
		assert.Equal(t, 550, int(tp.Start), "should be equal")
		assert.Equal(t, uint32(59), tp.Step, "should be equal")

		now = uint32(609)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(609), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")
		assert.Equal(t, 610, int(tp.Start), "should be equal")
		assert.Equal(t, uint32(0), tp.Step, "should be equal")

		now = uint32(610)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(610), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")
		assert.Equal(t, 610, int(tp.Start), "should be equal")
		assert.Equal(t, uint32(1), tp.Step, "should be equal")
	}

	{
		nr++
		fmt.Printf("TestCalculatePoint case %d.\n", nr)

		tp := model.NewTimePoint(1, 60)

		now := uint32(10)
		current, previous, mark := calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(10), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")
		assert.Equal(t, uint32(10), tp.Start, "should be equal")
		assert.Equal(t, uint32(1), tp.Step, "should be equal")

		now = uint32(67)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointPreviousHole, mark, "should be equal")
		assert.Equal(t, uint32(67), current, "should be equal")
		assert.Equal(t, uint32(11), previous, "should be equal")
		assert.Equal(t, uint32(10), uint32(tp.Start), "should be equal")
		assert.Equal(t, uint32(58), tp.Step, "should be equal")

		now = uint32(71)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointPreviousHole, mark, "should be equal")
		assert.Equal(t, uint32(71), current, "should be equal")
		assert.Equal(t, uint32(68), previous, "should be equal")
		assert.Equal(t, uint32(70), uint32(tp.Start), "should be equal")
		assert.Equal(t, uint32(2), tp.Step, "should be equal")
	}

	{
		nr++
		fmt.Printf("TestCalculatePoint case %d.\n", nr)

		tp := model.NewTimePoint(1, 60)

		now := uint32(10)
		current, previous, mark := calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(10), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")
		assert.Equal(t, uint32(10), tp.Start, "should be equal")
		assert.Equal(t, uint32(1), tp.Step, "should be equal")

		now = uint32(70)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointPreviousHole, mark, "should be equal")
		assert.Equal(t, uint32(70), current, "should be equal")
		assert.Equal(t, uint32(11), previous, "should be equal")
		assert.Equal(t, uint32(70), uint32(tp.Start), "should be equal")
		assert.Equal(t, uint32(1), tp.Step, "should be equal")

		now = uint32(71)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(71), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")
		assert.Equal(t, uint32(70), uint32(tp.Start), "should be equal")
		assert.Equal(t, uint32(2), tp.Step, "should be equal")
	}

	deviation = 2
	{
		nr++
		fmt.Printf("TestCalculatePoint case %d.\n", nr)

		tp := model.NewTimePoint(1, 60)

		now := uint32(10)
		current, previous, mark := calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(10), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")
		assert.Equal(t, uint32(10), tp.Start, "should be equal")
		assert.Equal(t, uint32(1), tp.Step, "should be equal")

		now = uint32(69)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointPreviousHole, mark, "should be equal")
		assert.Equal(t, uint32(69), current, "should be equal")
		assert.Equal(t, uint32(11), previous, "should be equal")
		assert.Equal(t, uint32(70), uint32(tp.Start), "should be equal")
		assert.Equal(t, uint32(0), tp.Step, "should be equal")

		now = uint32(74)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointPreviousHole, mark, "should be equal")
		assert.Equal(t, uint32(74), current, "should be equal")
		assert.Equal(t, uint32(70), previous, "should be equal")
		assert.Equal(t, uint32(70), uint32(tp.Start), "should be equal")
		assert.Equal(t, uint32(5), tp.Step, "should be equal")

		now = uint32(74)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(75), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")
		assert.Equal(t, uint32(70), uint32(tp.Start), "should be equal")
		assert.Equal(t, uint32(6), tp.Step, "should be equal")

		now = uint32(74)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointCurrentInvalid, mark, "should be equal")
		assert.Equal(t, uint32(0), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")
		assert.Equal(t, uint32(70), uint32(tp.Start), "should be equal")
		assert.Equal(t, uint32(6), tp.Step, "should be equal")

		now = uint32(75)
		current, previous, mark = calculatePoint(now, tp)
		assert.Equal(t, calculatePointOK, mark, "should be equal")
		assert.Equal(t, uint32(76), current, "should be equal")
		assert.Equal(t, uint32(0), previous, "should be equal")
		assert.Equal(t, uint32(70), uint32(tp.Start), "should be equal")
		assert.Equal(t, uint32(7), tp.Step, "should be equal")
	}
}

func TestChooseRing(t *testing.T) {
	var nr int

	{
		nr++
		fmt.Printf("TestChooseRing case %d.\n", nr)

		start, end := chooseRing(60, 120, 60, "test")
		assert.Equal(t, 60, int(start), "should be equal")
		assert.Equal(t, 119, int(end), "should be equal")
	}

	{
		nr++
		fmt.Printf("TestChooseRing case %d.\n", nr)

		start, end := chooseRing(60, 100, 60, "test")
		assert.Equal(t, 60, int(start), "should be equal")
		assert.Equal(t, 100, int(end), "should be equal")
	}

	{
		nr++
		fmt.Printf("TestChooseRing case %d.\n", nr)

		start, end := chooseRing(60, 120, 60, "test")
		assert.Equal(t, 60, int(start), "should be equal")
		assert.Equal(t, 119, int(end), "should be equal")
	}

	{
		nr++
		fmt.Printf("TestChooseRing case %d.\n", nr)

		start, end := chooseRing(60, 180, 60, "test")
		assert.Equal(t, 120, int(start), "should be equal")
		assert.Equal(t, 179, int(end), "should be equal")
	}

	{
		nr++
		fmt.Printf("TestChooseRing case %d.\n", nr)

		start, end := chooseRing(60, 180, 60, "test")
		assert.Equal(t, 120, int(start), "should be equal")
		assert.Equal(t, 179, int(end), "should be equal")
	}

	// combine calculatePoint
	{
		nr++
		fmt.Printf("TestChooseRing case %d.\n", nr)

		tp := model.NewTimePoint(1, 60)

		now := uint32(10)
		calculatePoint(now, tp)

		preStart := tp.Start
		calculatePoint(69, tp)
		curStart := tp.Start
		assert.Equal(t, 10, int(preStart), "should be equal")
		assert.Equal(t, 70, int(curStart), "should be equal")

		start, end := chooseRing(preStart, curStart, 60, "test")
		assert.Equal(t, 10, int(start), "should be equal")
		assert.Equal(t, 69, int(end), "should be equal")
	}
}

func TestStepStore(t *testing.T) {
	flag.Set("stderrthreshold", "info")
	flag.Set("v", "2")

	// init
	ss := &StepStore{
		Id: "test-id",
		Instance: &model.Instance{
			Addr: "test-instance-name",
		},
		RingCache:       new(cache.RingCache),
		TP:              model.NewTimePoint(1, 60),
		CompressContext: model.NewCompressContext(0),
	}
	ss.RingCache.Init(ss.Instance.Addr, ringBufferCount)

	var nr int
	var err error

	// case
	{
		var output interface{}
		nr++
		fmt.Printf("TestStepStore case %d.\n", nr)

		cnt := 0
		input := map[int]interface{}{
			0: 12,
			2: -10,
			3: 1 << 58,
			4: -(1 << 58) + 1,
		}
		for range time.NewTicker(1 * time.Second).C {
			cnt++
			output, err = ss.DoStep(input)
			assert.Equal(t, nil, err, "should be equal")
			if output != nil {
				break
			}
		}
		assert.Equal(t, 60, cnt, "should be equal")

		context := output.(*model.CompressContext)
		assert.Equal(t, uint32(60), context.TimestampEnd-context.TimestampBeg+1, "should be equal")
		assert.Equal(t, int64(12), context.DataMp[0].SameVal, "should be equal")
		assert.Equal(t, int64(-10), context.DataMp[2].SameVal, "should be equal")
		assert.Equal(t, int64(1<<58), context.DataMp[3].SameVal, "should be equal")
		assert.Equal(t, int64(-(1<<58)+1), context.DataMp[4].SameVal, "should be equal")
	}

	// case
	{
		var output interface{}
		nr++
		fmt.Printf("TestStepStore case %d.\n", nr)

		cnt := 0
		input := map[int]interface{}{
			0: 12,
			2: -10,
			3: 1 << 58,
			4: -(1 << 58) + 1,
		}
		var interval = 3
		ss.TP = model.NewTimePoint(interval, 60)
		ss.RingCache = new(cache.RingCache)
		ss.RingCache.Init(ss.Instance.Addr, ringBufferCount)

		// adjust interval
		for range time.NewTicker(time.Duration(interval) * time.Second).C {
			cnt++
			output, err = ss.DoStep(input)
			assert.Equal(t, nil, err, "should be equal")
			if output != nil {
				break
			}
		}
		assert.Equal(t, 60, cnt, "should be equal")

		context := output.(*model.CompressContext)
		assert.Equal(t, uint32(60), context.TimestampEnd-context.TimestampBeg+1, "should be equal")
		assert.Equal(t, int64(12), context.DataMp[0].SameVal, "should be equal")
		assert.Equal(t, int64(-10), context.DataMp[2].SameVal, "should be equal")
		assert.Equal(t, int64(1<<58), context.DataMp[3].SameVal, "should be equal")
		assert.Equal(t, int64(-(1<<58)+1), context.DataMp[4].SameVal, "should be equal")
	}
}

// create 1 hole
func TestStepStore2(t *testing.T) {
	flag.Set("stderrthreshold", "info")
	flag.Set("v", "2")

	// init
	ss := &StepStore{
		Id: "test-id",
		Instance: &model.Instance{
			Addr: "test-instance-name",
		},
		RingCache:       new(cache.RingCache),
		TP:              model.NewTimePoint(1, 60),
		CompressContext: model.NewCompressContext(0),
	}
	ss.RingCache.Init(ss.Instance.Addr, ringBufferCount)

	var nr int
	var err error

	// case
	{
		var output interface{}
		nr++
		fmt.Printf("TestStepStore2 case %d.\n", nr)

		cnt := 0
		input := map[int]interface{}{
			0: 12,
		}

		var interval = 1
		ss.TP = model.NewTimePoint(interval, 60)
		arr := make([]int64, 120)

		// adjust interval
		for range time.NewTicker(time.Duration(interval) * time.Second).C {
			cnt++
			if cnt == 10 { // create hole
				continue
			}
			output, err = ss.DoStep(input)
			assert.Equal(t, nil, err, "should be equal")
			if output != nil {
				break
			}
		}
		now := uint32(time.Now().Unix())

		assert.Equal(t, 61, cnt, "should be equal")

		context := output.(*model.CompressContext)
		realCount := (context.TimestampEnd-context.TimestampBeg)/uint32(interval) + 1
		assert.Equal(t, uint32(60), realCount, "should be equal")
		assert.Equal(t, byte(1), context.DataMp[0].SameFlag, "should be equal")
		assert.Equal(t, int64(12), context.DataMp[0].GcdValue, "should be equal")
		assert.Equal(t, uint32(1), now-context.TimestampEnd, "should be equal")

		start, res := ss.RingCache.Query(0, context.TimestampEnd, arr)
		fmt.Println(context.TimestampEnd, context.TimestampBeg, start)
		// assert.Equal(t, context.TimestampBeg, start, "should be equal")
		expected := []int64{12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12}
		assert.Equal(t, true, reflect.DeepEqual(res[len(res)-int(realCount):], expected), "should be equal")
		fmt.Println(res[len(res)-int(realCount):])
	}
}

// create 2 holes
func TestStepStore3(t *testing.T) {
	flag.Set("stderrthreshold", "info")
	flag.Set("v", "2")

	// init
	ss := &StepStore{
		Id: "test-id",
		Instance: &model.Instance{
			Addr: "test-instance-name",
		},
		RingCache:       new(cache.RingCache),
		TP:              model.NewTimePoint(1, 60),
		CompressContext: model.NewCompressContext(0),
	}
	ss.RingCache.Init(ss.Instance.Addr, ringBufferCount)

	var nr int
	var err error

	// case
	{
		var output interface{}
		nr++
		fmt.Printf("TestStepStore3 case %d.\n", nr)

		cnt := 0
		input := map[int]interface{}{
			0: 12,
		}

		var interval = 1
		ss.TP = model.NewTimePoint(interval, 60)
		arr := make([]int64, 120)

		// adjust interval
		for range time.NewTicker(time.Duration(interval) * time.Second).C {
			cnt++
			if cnt == 10 || cnt == 11 { // create hole
				continue
			}
			output, err = ss.DoStep(input)
			assert.Equal(t, nil, err, "should be equal")
			if output != nil {
				break
			}
		}
		now := uint32(time.Now().Unix())

		assert.Equal(t, 62, cnt, "should be equal")

		context := output.(*model.CompressContext)
		realCount := (context.TimestampEnd-context.TimestampBeg)/uint32(interval) + 1
		assert.Equal(t, uint32(60), realCount, "should be equal")
		assert.Equal(t, byte(1), context.DataMp[0].SameFlag, "should be equal")
		assert.Equal(t, int64(12), context.DataMp[0].GcdValue, "should be equal")
		assert.Equal(t, uint32(2), now-context.TimestampEnd, "should be equal")

		start, res := ss.RingCache.Query(0, context.TimestampEnd, arr)
		fmt.Println(context.TimestampEnd, context.TimestampBeg, start)
		// assert.Equal(t, context.TimestampBeg, start, "should be equal")
		expected := []int64{12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12}
		assert.Equal(t, true, reflect.DeepEqual(res[len(res)-int(realCount):], expected), "should be equal")
		fmt.Println(res[len(res)-int(realCount):])
	}
}

// create 3 holes
func TestStepStore4(t *testing.T) {
	flag.Set("stderrthreshold", "info")
	flag.Set("v", "2")

	// init
	ss := &StepStore{
		Id: "test-id",
		Instance: &model.Instance{
			Addr: "test-instance-name",
		},
		RingCache:       new(cache.RingCache),
		TP:              model.NewTimePoint(1, 60),
		CompressContext: model.NewCompressContext(0),
	}
	ss.RingCache.Init(ss.Instance.Addr, ringBufferCount)

	var nr int
	var err error

	// case
	{
		var output interface{}
		nr++
		fmt.Printf("TestStepStore4 case %d.\n", nr)

		cnt := 0
		input := map[int]interface{}{
			0: 12,
		}

		var interval = 1
		ss.TP = model.NewTimePoint(interval, 60)
		arr := make([]int64, 120)

		// adjust interval
		for range time.NewTicker(time.Duration(interval) * time.Second).C {
			cnt++
			if cnt == 10 || cnt == 11 || cnt == 12 { // create hole
				continue
			}
			output, err = ss.DoStep(input)
			assert.Equal(t, nil, err, "should be equal")
			if output != nil {
				break
			}
		}
		now := uint32(time.Now().Unix())

		assert.Equal(t, 60, cnt, "should be equal")

		context := output.(*model.CompressContext)
		realCount := (context.TimestampEnd-context.TimestampBeg)/uint32(interval) + 1
		assert.Equal(t, uint32(60), realCount, "should be equal")
		assert.Equal(t, byte(2), context.DataMp[0].SameFlag, "should be equal")
		assert.Equal(t, int64(12), context.DataMp[0].GcdValue, "should be equal")
		assert.Equal(t, uint32(0), now-context.TimestampEnd, "should be equal")

		start, res := ss.RingCache.Query(0, context.TimestampEnd, arr)
		fmt.Println(context.TimestampEnd, context.TimestampBeg, start)
		// assert.Equal(t, context.TimestampBeg, start, "should be equal")
		expected := []int64{12, 12, 12, 12, 12, 12, 12, 12, 12, util.NullData, util.NullData, util.NullData, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12}
		assert.Equal(t, true, reflect.DeepEqual(res[len(res)-int(realCount):], expected), "should be equal")
		fmt.Println(res[len(res)-int(realCount):])
	}
}

// create > 3 holes
func TestStepStore5(t *testing.T) {
	flag.Set("stderrthreshold", "info")
	flag.Set("v", "2")

	// init
	ss := &StepStore{
		Id: "test-id",
		Instance: &model.Instance{
			Addr: "test-instance-name",
		},
		RingCache:       new(cache.RingCache),
		TP:              model.NewTimePoint(1, 60),
		CompressContext: model.NewCompressContext(0),
	}
	ss.RingCache.Init(ss.Instance.Addr, ringBufferCount)

	var nr int
	var err error

	// case
	{
		var output interface{}
		nr++
		fmt.Printf("TestStepStore5 case %d.\n", nr)

		cnt := 0
		input := map[int]interface{}{
			0: 12,
		}

		var interval = 1
		ss.TP = model.NewTimePoint(interval, 60)
		arr := make([]int64, 120)

		// adjust interval
		for range time.NewTicker(time.Duration(interval) * time.Second).C {
			cnt++
			if cnt == 10 || cnt == 11 || cnt == 12 || cnt == 14 || cnt == 15 { // create hole
				continue
			}
			output, err = ss.DoStep(input)
			assert.Equal(t, nil, err, "should be equal")
			if output != nil {
				break
			}
		}
		now := uint32(time.Now().Unix())

		// 14, 15
		assert.Equal(t, 62, cnt, "should be equal")

		context := output.(*model.CompressContext)
		realCount := (context.TimestampEnd-context.TimestampBeg)/uint32(interval) + 1
		assert.Equal(t, uint32(60), realCount, "should be equal")
		assert.Equal(t, byte(2), context.DataMp[0].SameFlag, "should be equal")
		assert.Equal(t, int64(12), context.DataMp[0].GcdValue, "should be equal")
		assert.Equal(t, uint32(2), now-context.TimestampEnd, "should be equal")

		start, res := ss.RingCache.Query(0, context.TimestampEnd, arr)
		fmt.Println(context.TimestampEnd, context.TimestampBeg, start)
		// assert.Equal(t, context.TimestampBeg, start, "should be equal")
		expected := []int64{12, 12, 12, 12, 12, 12, 12, 12, 12, util.NullData, util.NullData, util.NullData, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12}
		assert.Equal(t, true, reflect.DeepEqual(res[len(res)-int(realCount):], expected), "should be equal")
		fmt.Println(res[len(res)-int(realCount):])
	}
}

// create 2 doubles
func TestStepStore6(t *testing.T) {
	flag.Set("stderrthreshold", "info")
	flag.Set("v", "2")

	// init
	ss := &StepStore{
		Id: "test-id",
		Instance: &model.Instance{
			Addr: "test-instance-name",
		},
		RingCache:       new(cache.RingCache),
		TP:              model.NewTimePoint(1, 60),
		CompressContext: model.NewCompressContext(0),
	}
	ss.RingCache.Init(ss.Instance.Addr, ringBufferCount)

	var nr int
	var err error

	// case
	{
		var output interface{}
		nr++
		fmt.Printf("TestStepStore6 case %d.\n", nr)

		cnt := 0
		input := map[int]interface{}{
			0: 12,
		}

		var interval = 1
		ss.TP = model.NewTimePoint(interval, 60)
		arr := make([]int64, 120)

		// adjust interval
		for range time.NewTicker(time.Duration(interval) * time.Second).C {
			cnt++
			if cnt == 10 || cnt == 11 { // create double
				output, err = ss.DoStep(input)
				assert.Equal(t, nil, err, "should be equal")
			}
			output, err = ss.DoStep(input)
			assert.Equal(t, nil, err, "should be equal")
			if output != nil {
				break
			}
		}
		now := uint32(time.Now().Unix())

		assert.Equal(t, 58, cnt, "should be equal")

		context := output.(*model.CompressContext)
		realCount := (context.TimestampEnd-context.TimestampBeg)/uint32(interval) + 1
		assert.Equal(t, uint32(60), realCount, "should be equal")
		assert.Equal(t, byte(1), context.DataMp[0].SameFlag, "should be equal")
		assert.Equal(t, int64(12), context.DataMp[0].GcdValue, "should be equal")
		assert.Equal(t, uint32(2), context.TimestampEnd-now, "should be equal")

		start, res := ss.RingCache.Query(0, context.TimestampEnd, arr)
		fmt.Println(context.TimestampEnd, context.TimestampBeg, start)
		// assert.Equal(t, context.TimestampBeg, start, "should be equal")
		expected := []int64{12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12}
		assert.Equal(t, true, reflect.DeepEqual(res[len(res)-int(realCount):], expected), "should be equal")
		fmt.Println(res[len(res)-int(realCount):])
	}
}

// create 3 doubles
func TestStepStore7(t *testing.T) {
	flag.Set("stderrthreshold", "info")
	flag.Set("v", "2")

	// init
	ss := &StepStore{
		Id: "test-id",
		Instance: &model.Instance{
			Addr: "test-instance-name",
		},
		RingCache:       new(cache.RingCache),
		TP:              model.NewTimePoint(1, 60),
		CompressContext: model.NewCompressContext(0),
	}
	ss.RingCache.Init(ss.Instance.Addr, ringBufferCount)

	var nr int
	var err error

	// case
	{
		var output interface{}
		nr++
		fmt.Printf("TestStepStore7 case %d.\n", nr)

		cnt := 0
		input := map[int]interface{}{
			0: 12,
		}

		var interval = 1
		ss.TP = model.NewTimePoint(interval, 60)
		arr := make([]int64, 120)

		// adjust interval
		for range time.NewTicker(time.Duration(interval) * time.Second).C {
			cnt++
			if cnt == 10 { // create double
				output, err = ss.DoStep(input)
				assert.Equal(t, nil, err, "should be equal")
				output, err = ss.DoStep(input)
				assert.Equal(t, nil, err, "should be equal")
				output, err = ss.DoStep(input)
				assert.Equal(t, nil, err, "should be equal")
			}
			output, err = ss.DoStep(input)
			assert.Equal(t, nil, err, "should be equal")
			if output != nil {
				break
			}
		}
		now := uint32(time.Now().Unix())

		assert.Equal(t, 58, cnt, "should be equal")

		context := output.(*model.CompressContext)
		realCount := (context.TimestampEnd-context.TimestampBeg)/uint32(interval) + 1
		assert.Equal(t, uint32(60), realCount, "should be equal")
		assert.Equal(t, byte(1), context.DataMp[0].SameFlag, "should be equal")
		assert.Equal(t, int64(12), context.DataMp[0].GcdValue, "should be equal")
		assert.Equal(t, uint32(2), context.TimestampEnd-now, "should be equal")

		start, res := ss.RingCache.Query(0, context.TimestampEnd, arr)
		fmt.Println(context.TimestampEnd, context.TimestampBeg, start)
		// assert.Equal(t, context.TimestampBeg, start, "should be equal")
		expected := []int64{12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12}
		assert.Equal(t, true, reflect.DeepEqual(res[len(res)-int(realCount):], expected), "should be equal")
		fmt.Println(res[len(res)-int(realCount):])
	}
}

func TestStepStore8(t *testing.T) {
	flag.Set("stderrthreshold", "info")
	flag.Set("v", "2")

	// init
	ss := &StepStore{
		Id: "test-id",
		Instance: &model.Instance{
			Addr: "test-instance-name",
		},
		RingCache:       new(cache.RingCache),
		TP:              model.NewTimePoint(1, 60),
		CompressContext: model.NewCompressContext(0),
	}
	ss.RingCache.Init(ss.Instance.Addr, ringBufferCount)

	var nr int
	var err error

	// case
	{
		var output interface{}
		nr++
		fmt.Printf("TestStepStore8 case %d.\n", nr)

		cnt := 0
		input := map[int]interface{}{
			0: 12,
		}

		var interval = 1
		ss.TP = model.NewTimePoint(interval, 60)
		arr := make([]int64, 120)

		// adjust interval
		for range time.NewTicker(time.Duration(interval) * time.Second).C {
			cnt++
			if cnt == 10 { // create hole
				continue
			}
			if cnt == 11 { // call twice
				output, err = ss.DoStep(input)
			}
			output, err = ss.DoStep(input)
			assert.Equal(t, nil, err, "should be equal")
			if output != nil {
				break
			}
		}
		now := uint32(time.Now().Unix())

		assert.Equal(t, 60, cnt, "should be equal")

		context := output.(*model.CompressContext)
		assert.Equal(t, uint32(60), (context.TimestampEnd-context.TimestampBeg)/uint32(interval)+1, "should be equal")
		assert.Equal(t, byte(1), context.DataMp[0].SameFlag, "should be equal")
		assert.Equal(t, int64(12), context.DataMp[0].GcdValue, "should be equal")
		assert.Equal(t, uint32(0), context.TimestampEnd-now, "should be equal")

		start, res := ss.RingCache.Query(0, context.TimestampEnd, arr)
		fmt.Println(context.TimestampEnd, context.TimestampBeg, start)
		// assert.Equal(t, context.TimestampBeg, start, "should be equal")
		expected := []int64{12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12}
		assert.Equal(t, true, reflect.DeepEqual(res[len(res)-cnt:], expected), "should be equal")
		fmt.Println(res[len(res)-cnt:])
	}
}

func TestStepStore9(t *testing.T) {
	flag.Set("stderrthreshold", "info")
	flag.Set("v", "2")

	// init
	ss := &StepStore{
		Id: "test-id",
		Instance: &model.Instance{
			Addr: "test-instance-name",
		},
		RingCache:       new(cache.RingCache),
		TP:              model.NewTimePoint(1, 60),
		CompressContext: model.NewCompressContext(0),
	}
	ss.RingCache.Init(ss.Instance.Addr, ringBufferCount)

	var nr int
	var err error

	// case
	{
		var output interface{}
		nr++
		fmt.Printf("TestStepStore9 case %d.\n", nr)

		cnt := 0
		input := map[int]interface{}{
			0: 12,
		}

		var interval = 1
		ss.TP = model.NewTimePoint(interval, 60)
		arr := make([]int64, 120)

		// adjust interval
		for range time.NewTicker(time.Duration(interval) * time.Second).C {
			cnt++
			if cnt == 10 { // delay 0.9s
				time.Sleep(900 * time.Millisecond)
			}
			output, err = ss.DoStep(input)
			assert.Equal(t, nil, err, "should be equal")
			if output != nil {
				break
			}
		}
		assert.Equal(t, 60, cnt, "should be equal")

		context := output.(*model.CompressContext)
		assert.Equal(t, uint32(60), (context.TimestampEnd-context.TimestampBeg)/uint32(interval)+1, "should be equal")
		assert.Equal(t, byte(1), context.DataMp[0].SameFlag, "should be equal")
		assert.Equal(t, int64(12), context.DataMp[0].GcdValue, "should be equal")

		start, res := ss.RingCache.Query(0, context.TimestampEnd, arr)
		fmt.Println(context.TimestampEnd, context.TimestampBeg, start)
		// assert.Equal(t, context.TimestampBeg, start, "should be equal")
		expected := []int64{12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12}
		assert.Equal(t, true, reflect.DeepEqual(res[len(res)-cnt:], expected), "should be equal")
		fmt.Println(res[len(res)-cnt:])
	}
}
