package model

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"inspector/util"
	"reflect"
	"testing"
)

func TestCompressContextUpdate(t *testing.T) {
	var nr int
	cc := NewCompressContext(0)

	// case1
	{
		nr++
		fmt.Printf("TestCompressContextUpdate case %d.\n", nr)

		idx := nr

		cc.Update(idx, 0)
		cc.Update(idx, 1)
		cc.Update(idx, 2)

		assert.Equal(t, byte(2), cc.DataMp[idx].SameFlag, "should be equal")
		assert.Equal(t, int64(1), cc.DataMp[idx].GcdValue, "should be equal")
		assert.Equal(t, int(3), cc.DataMp[idx].ValCount, "should be equal")
	}

	// case2
	{
		nr++
		fmt.Printf("TestCompressContextUpdate case %d.\n", nr)

		idx := nr

		cc.Update(idx, 0)
		cc.Update(idx, util.NullData)
		cc.Update(idx, 2)

		assert.Equal(t, byte(2), cc.DataMp[idx].SameFlag, "should be equal")
		assert.Equal(t, int64(2), cc.DataMp[idx].GcdValue, "should be equal")
		assert.Equal(t, int(3), cc.DataMp[idx].ValCount, "should be equal")
	}

	// case3
	{
		nr++
		fmt.Printf("TestCompressContextUpdate case %d.\n", nr)

		idx := nr

		cc.Update(idx, util.NullData)
		cc.Update(idx, util.NullData)
		cc.Update(idx, util.NullData)

		assert.Equal(t, byte(1), cc.DataMp[idx].SameFlag, "should be equal")
		assert.Equal(t, util.NullData, cc.DataMp[idx].SameVal, "should be equal")
		assert.Equal(t, int64(0), cc.DataMp[idx].GcdValue, "should be equal")
		assert.Equal(t, int(3), cc.DataMp[idx].ValCount, "should be equal")
	}

	// case4
	{
		nr++
		fmt.Printf("TestCompressContextUpdate case %d.\n", nr)

		idx := nr

		assert.Equal(t, true, reflect.ValueOf(cc.DataMp[idx]).IsNil(), "should be equal")
	}

	// case5
	{
		nr++
		fmt.Printf("TestCompressContextUpdate case %d.\n", nr)

		idx := nr

		cc.Update(idx, 3)
		cc.Update(idx, util.NullData)
		cc.Update(idx, 6)
		cc.Update(idx, 0)
		cc.Update(idx, -3)

		assert.Equal(t, byte(2), cc.DataMp[idx].SameFlag, "should be equal")
		assert.Equal(t, int64(3), cc.DataMp[idx].GcdValue, "should be equal")
		assert.Equal(t, int(5), cc.DataMp[idx].ValCount, "should be equal")
	}

	// case6
	{
		nr++
		fmt.Printf("TestCompressContextUpdate case %d.\n", nr)

		idx := nr

		cc.Update(idx, util.NullData)
		cc.Update(idx, util.NullData)
		cc.Update(idx, 0)
		cc.Update(idx, util.NullData)
		cc.Update(idx, util.NullData)
		cc.Update(idx, 0)
		cc.Update(idx, util.NullData)
		cc.Update(idx, util.NullData)
		cc.Update(idx, 0)

		assert.Equal(t, byte(2), cc.DataMp[idx].SameFlag, "should be equal")
		// assert.Equal(t, util.NullData, cc.DataMp[idx].SameVal, "should be equal")
		assert.Equal(t, int64(0), cc.DataMp[idx].GcdValue, "should be equal")
		assert.Equal(t, int(9), cc.DataMp[idx].ValCount, "should be equal")
	}

	// case7
	{
		nr++
		fmt.Printf("TestCompressContextUpdate case %d.\n", nr)

		idx := nr

		cc.Update(idx, util.NullData)
		cc.Update(idx, util.NullData)
		cc.Update(idx, -5)

		assert.Equal(t, byte(2), cc.DataMp[idx].SameFlag, "should be equal")
		// assert.Equal(t, util.NullData, cc.DataMp[idx].SameVal, "should be equal")
		assert.Equal(t, int64(5), cc.DataMp[idx].GcdValue, "should be equal")
		assert.Equal(t, int(3), cc.DataMp[idx].ValCount, "should be equal")
	}

	// case8
	{
		nr++
		fmt.Printf("TestCompressContextUpdate case %d.\n", nr)

		idx := nr

		cc.Update(idx, 0)

		assert.Equal(t, byte(1), cc.DataMp[idx].SameFlag, "should be equal")
		assert.Equal(t, int64(0), cc.DataMp[idx].SameVal, "should be equal")
		assert.Equal(t, int64(0), cc.DataMp[idx].GcdValue, "should be equal")
		assert.Equal(t, int(1), cc.DataMp[idx].ValCount, "should be equal")
	}

	// case9
	{
		nr++
		fmt.Printf("TestCompressContextUpdate case %d.\n", nr)

		idx := nr

		cc.Update(idx, -5)

		assert.Equal(t, byte(1), cc.DataMp[idx].SameFlag, "should be equal")
		assert.Equal(t, int64(-5), cc.DataMp[idx].SameVal, "should be equal")
		assert.Equal(t, int64(5), cc.DataMp[idx].GcdValue, "should be equal")
		assert.Equal(t, int(1), cc.DataMp[idx].ValCount, "should be equal")
	}
}
