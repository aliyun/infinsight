package model

import(
	"testing"
	"fmt"
	"github.com/stretchr/testify/assert"
	"inspector/util"
	"reflect"
)

func TestCompressContextUpdate(t *testing.T) {
	var nr int
	cc := NewCompressContext(0)

	{
		nr++
		fmt.Printf("TestCompressContextUpdate case %d.\n", nr)

		cc.Update(0, 0)
		cc.Update(0, 1)
		cc.Update(0, 2)

		assert.Equal(t, byte(2), cc.DataMp[0].SameFlag, "should be equal")
		assert.Equal(t, int64(1), cc.DataMp[0].GcdValue, "should be equal")
	}

	{
		nr++
		fmt.Printf("TestCompressContextUpdate case %d.\n", nr)

		idx := nr

		cc.Update(idx, 0)
		cc.Update(idx, util.NullData)
		cc.Update(idx, 2)

		assert.Equal(t, byte(2), cc.DataMp[idx].SameFlag, "should be equal")
		assert.Equal(t, int64(2), cc.DataMp[idx].GcdValue, "should be equal")
	}

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
	}

	{
		nr++
		fmt.Printf("TestCompressContextUpdate case %d.\n", nr)

		idx := nr

		assert.Equal(t, true, reflect.ValueOf(cc.DataMp[idx]).IsNil(), "should be equal")
	}

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
	}

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
	}

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
	}

	{
		nr++
		fmt.Printf("TestCompressContextUpdate case %d.\n", nr)

		idx := nr

		cc.Update(idx, 0)

		assert.Equal(t, byte(1), cc.DataMp[idx].SameFlag, "should be equal")
		assert.Equal(t, int64(0), cc.DataMp[idx].SameVal, "should be equal")
		assert.Equal(t, int64(0), cc.DataMp[idx].GcdValue, "should be equal")
	}

	{
		nr++
		fmt.Printf("TestCompressContextUpdate case %d.\n", nr)

		idx := nr

		cc.Update(idx, -5)

		assert.Equal(t, byte(1), cc.DataMp[idx].SameFlag, "should be equal")
		assert.Equal(t, int64(-5), cc.DataMp[idx].SameVal, "should be equal")
		assert.Equal(t, int64(5), cc.DataMp[idx].GcdValue, "should be equal")
	}
}