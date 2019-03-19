package compress

import (
	"testing"
	"fmt"
	"github.com/stretchr/testify/assert"
	"inspector/util"
)

func TestCompressAndDecompress(t *testing.T) {
	var (
		nr        int
		sameValue int64
		count     int
		byteRet   []byte
		err       error
		gcd       int64
	)

	// case
	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		sameValue = 1
		count = 5
		// compress
		byteRet, err = Compress(SameDigitCompress, count, sameValue)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")
		// fmt.Println(byteRet)

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareSameValue(sameValue, count, output), "should be nil")
	}

	// case
	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		sameValue = 1
		count = 0
		// compress
		byteRet, err = Compress(SameDigitCompress, count, sameValue)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")
		// fmt.Println(byteRet)

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareSameValue(sameValue, count, output), "should be nil")
	}

	// case
	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		sameValue = 0
		count = 1000
		// compress
		byteRet, err = Compress(SameDigitCompress, count, sameValue)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")
		// fmt.Println(byteRet)

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareSameValue(sameValue, count, output), "should be nil")
	}

	// case
	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		sameValue = -1000
		count = 1000
		// compress
		byteRet, err = Compress(SameDigitCompress, count, sameValue)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")
		// fmt.Println(byteRet)

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareSameValue(sameValue, count, output), "should be nil")
	}

	// case
	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		sameValue = -(int64(1) << 61)
		count = 8192
		// compress
		byteRet, err = Compress(SameDigitCompress, count, sameValue)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")
		// fmt.Println(byteRet)

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareSameValue(sameValue, count, output), "should be nil")
	}

	// case
	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		sameValue = 0
		count = 8192
		// compress
		byteRet, err = Compress(SameDigitCompress, count, sameValue)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")
		// fmt.Println(byteRet)

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareSameValue(sameValue, count, output), "should be nil")
	}

	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		gcd = 1
		diffList := []int64{1, 2, 3, 4, 5, 6, 7}
		diffListCopy := make([]int64, len(diffList))
		copy(diffListCopy, diffList)
		byteRet, err = Compress(DiffCompress, gcd, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareDiffValue(diffListCopy, output), "should be nil")
	}

	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		gcd = 2
		diffList := []int64{2, 4, 6, 8, 20, 24, 100}
		diffListCopy := make([]int64, len(diffList))
		copy(diffListCopy, diffList)
		byteRet, err = Compress(DiffCompress, gcd, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareDiffValue(diffListCopy, output), "should be nil")
	}

	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		gcd = 2
		diffList := []int64{2, 4, 6, -8, 20, 24, 100}
		diffListCopy := make([]int64, len(diffList))
		copy(diffListCopy, diffList)
		byteRet, err = Compress(DiffCompress, gcd, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareDiffValue(diffListCopy, output), "should be nil")
	}

	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		gcd = 1
		diffList := []int64{-10000, 0, 100000, -11123, 23423423, 1124123, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 10, -10, -10}
		diffListCopy := make([]int64, len(diffList))
		copy(diffListCopy, diffList)
		byteRet, err = Compress(DiffCompress, gcd, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareDiffValue(diffListCopy, output), "should be nil")
	}

	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		gcd = 1
		diffList := []int64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}
		diffListCopy := make([]int64, len(diffList))
		copy(diffListCopy, diffList)
		byteRet, err = Compress(DiffCompress, gcd, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareDiffValue(diffListCopy, output), "should be nil")
	}

	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		gcd = 1
		diffList := []int64{1 << 56, -(1 << 56), 0, 1 << 57, -(1 << 57), 123412341234, 1234234, -23452345, 12341234346534, 88883, 23, 45, 57, 449569, 998, 938574762, -123845757}
		diffListCopy := make([]int64, len(diffList))
		copy(diffListCopy, diffList)
		byteRet, err = Compress(DiffCompress, gcd, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareDiffValue(diffListCopy, output), "should be nil")
	}

	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		gcd = 1
		diffList := []int64{}
		diffListCopy := make([]int64, len(diffList))
		copy(diffListCopy, diffList)
		byteRet, err = Compress(DiffCompress, gcd, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareDiffValue(diffListCopy, output), "should be nil")
	}

	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		gcd = 1
		diffList := []int64{1 << 58, -(1 << 58) + 1} // max
		diffListCopy := make([]int64, len(diffList))
		copy(diffListCopy, diffList)
		byteRet, err = Compress(DiffCompress, gcd, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareDiffValue(diffListCopy, output), "should be nil")
	}

	// test gcd
	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		gcd = 4
		diffList := []int64{4, 8, 16, 20} // max
		diffListCopy := make([]int64, len(diffList))
		copy(diffListCopy, diffList)
		byteRet, err = Compress(DiffCompress, gcd, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareDiffValue(diffListCopy, output), "should be nil")
		// fmt.Println(diffListCopy, output)
	}

	// test gcd
	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		gcd = 3
		diffList := []int64{3, 27, 9, 3, 3, 81, 300, 900, 903, 33333, 333, 33, 3, -3, 999, -999, 333333333333, 369423693, -369423693} // max
		diffListCopy := make([]int64, len(diffList))
		copy(diffListCopy, diffList)
		byteRet, err = Compress(DiffCompress, gcd, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareDiffValue(diffListCopy, output), "should be nil")
	}

	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		gcd = 1
		var diffList []int64
		for i := 0; i < 60; i ++ {
			diffList = append(diffList, int64(1000000 + 1000 + i))
		}
		diffListCopy := make([]int64, len(diffList))
		copy(diffListCopy, diffList)
		byteRet, err = Compress(DiffCompress, gcd, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output := []int64{1, 2, 5}
		// add same header
		diffListCopy = append([]int64{1, 2, 5}, diffListCopy...)
		output, err := Decompress(byteRet, output)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareDiffValue(diffListCopy, output), "should be nil")
	}

	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		gcd = 60
		var diffList []int64
		for i := 0; i < 60; i ++ {
			diffList = append(diffList, int64(1000000 + 1000 + i) * 60)
		}
		diffListCopy := make([]int64, len(diffList))
		copy(diffListCopy, diffList)
		byteRet, err = Compress(DiffCompress, gcd, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output := []int64{1, 2, 5}
		// add same header
		diffListCopy = append([]int64{1, 2, 5}, diffListCopy...)
		output, err := Decompress(byteRet, output)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareDiffValue(diffListCopy, output), "should be nil")
	}

	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		gcd = 60
		var diffList []int64
		for i := 0; i < 60; i ++ {
			diffList = append(diffList, int64((-1000000 - 1000 - i) * 60))
		}
		diffListCopy := make([]int64, len(diffList))
		copy(diffListCopy, diffList)
		byteRet, err = Compress(DiffCompress, gcd, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output := []int64{1, 2, 5}
		// add same header
		diffListCopy = append([]int64{1, 2, 5}, diffListCopy...)
		output, err := Decompress(byteRet, output)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareDiffValue(diffListCopy, output), "should be nil")
	}

	// test null data
	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		gcd = 2
		diffList := []int64{4, 8, 16, 20, util.NullData, 10}
		diffListCopy := make([]int64, len(diffList))
		copy(diffListCopy, diffList)
		byteRet, err = Compress(DiffCompress, gcd, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareDiffValue(diffListCopy, output), "should be nil")
		// fmt.Println(diffListCopy, output)
	}

	// test null data
	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		gcd = 2
		diffList := []int64{4, 8, 16, 20, util.NullData, util.NullData, util.NullData, util.NullData, -10}
		diffListCopy := make([]int64, len(diffList))
		copy(diffListCopy, diffList)
		byteRet, err = Compress(DiffCompress, gcd, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareDiffValue(diffListCopy, output), "should be nil")
		// fmt.Println(diffListCopy, output)
	}

	// test null data
	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		gcd = 2
		diffList := []int64{util.NullData, util.NullData, 4, util.NullData, 8, 16, 20, util.NullData, util.NullData, -10}
		diffListCopy := make([]int64, len(diffList))
		copy(diffListCopy, diffList)
		byteRet, err = Compress(DiffCompress, gcd, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareDiffValue(diffListCopy, output), "should be nil")
		// fmt.Println(diffListCopy, output)
	}

	// test null data
	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		gcd = 2
		diffList := []int64{util.NullData, util.NullData, 4, util.NullData, 8, 16, 20, util.NullData, util.NullData, -10, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData, util.NullData}
		diffListCopy := make([]int64, len(diffList))
		copy(diffListCopy, diffList)
		byteRet, err = Compress(DiffCompress, gcd, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareDiffValue(diffListCopy, output), "should be nil")
		// fmt.Println(diffListCopy, output)
	}

	// test null data
	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		gcd = 1
		diffList := []int64{util.NullData, util.NullData, util.NullData, util.NullData, util.NullData}
		diffListCopy := make([]int64, len(diffList))
		copy(diffListCopy, diffList)
		byteRet, err = Compress(DiffCompress, gcd, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareDiffValue(diffListCopy, output), "should be nil")
		// fmt.Println(diffListCopy, output)
	}

	// test all zero
	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		gcd = 1
		diffList := []int64{util.NullData, util.NullData, 0, 0, util.NullData, util.NullData, 0, 0, util.NullData, 0, 0, util.NullData, util.NullData, 0, 0, util.NullData, util.NullData, 0, 0, util.NullData, 0, 0, util.NullData, util.NullData, 0, 0, util.NullData, util.NullData, 0, 0, util.NullData, 0, 0, util.NullData, util.NullData, 0, 0, util.NullData, util.NullData, 0, 0, util.NullData, 0, 0}
		diffListCopy := make([]int64, len(diffList))
		copy(diffListCopy, diffList)
		byteRet, err = Compress(DiffCompress, gcd, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareDiffValue(diffListCopy, output), "should be nil")
		// fmt.Println(diffListCopy, output)
	}

	// test all zero and gcd = 0
	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		gcd = 0
		diffList := []int64{util.NullData, util.NullData, 0, 0, util.NullData, util.NullData, 0, 0, util.NullData, 0, 0, util.NullData, util.NullData, 0, 0, util.NullData, util.NullData, 0, 0, util.NullData, 0, 0, util.NullData, util.NullData, 0, 0, util.NullData, util.NullData, 0, 0, util.NullData, 0, 0, util.NullData, util.NullData, 0, 0, util.NullData, util.NullData, 0, 0, util.NullData, 0, 0}
		diffListCopy := make([]int64, len(diffList))
		copy(diffListCopy, diffList)
		byteRet, err = Compress(DiffCompress, gcd, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, true, compareDiffValue(diffListCopy, output), "should be nil")
		// fmt.Println(diffListCopy, output)
	}

	// no compress
	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		diffList := []int64{3, 27, 9, 3, 3, 81, 300, 900, 903, 33333, 333, 33, 3, -3, 999, -999, 333333333333, 369423693, -369423693, 1 << 58, -(1 << 58) + 1} // max
		byteRet, err = Compress(NoCompress, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, output, diffList, "should be nil")
	}

	// no compress
	{
		nr++
		fmt.Printf("TestCompressAndDecompress case %d.\n", nr)
		diffList := []int64{} // max
		byteRet, err = Compress(NoCompress, diffList)
		assert.Equal(t, nil, err, "should be nil")
		assert.NotEqual(t, 0, len(byteRet), "should be nil")

		// decompress
		output, err := Decompress(byteRet, nil)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, output, diffList, "should be nil")
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