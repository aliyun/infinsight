/*
// =====================================================================================
//
//       Filename:  ZigZag.go
//
//    Description:
//
//        Version:  1.0
//        Created:  07/25/2018 05:11:35 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package compress

import (
	"fmt"
	"inspector/util"
	"math/rand"
	"runtime"
	"testing"
)

func TestOriginDistance(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var x uint64 = 0
	var y int64 = 0
	var m uint64 = 1
	var n int64 = 0

	x = OriginDistanceEncode(0)
	check(x == m, "test")
	m++
	for i := 1; i < 100; i++ {
		x = OriginDistanceEncode(int64(-i))
		check(x == m, "test")
		m++
		x = OriginDistanceEncode(int64(i))
		check(x == m, "test")
		m++
	}

	y = OriginDistanceDecode(0)
	check(y == util.NullData, "test")
	y = OriginDistanceDecode(1)
	check(y == 0, "test")
	n++
	for i := 2; i < 200; i++ {
		y = OriginDistanceDecode(uint64(i))
		check(y == -n, "test")
		i++
		y = OriginDistanceDecode(uint64(i))
		check(y == n, "test")
		n++
	}

	check(true, "test")
}

func TestVarInt(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	// case 1
	{
		var output []byte = make([]byte, 10)

		output = output[0:0]
		output = VarUintEncoding(1, output, 0)
		check(len(output) == 1, "test")
		check(output[0] == 0x01, "test")

		output = output[0:0]
		output = VarUintEncoding(127, output, 0)
		check(len(output) == 1, "test")
		check(output[0] == 0x7f, "test")

		output = output[0:0]
		output = VarUintEncoding(128, output, 0)
		check(len(output) == 2, "test")
		check(output[0] == 0x80, "test")
		check(output[1] == 0x01, "test")

		output = output[0:0]
		output = VarUintEncoding(255, output, 0)
		check(len(output) == 2, "test")
		check(output[0] == 0xff, "test")
		check(output[1] == 0x01, "test")

		output = output[0:0]
		output = VarUintEncoding(256, output, 0)
		check(len(output) == 2, "test")
		check(output[0] == 0x80, "test")
		check(output[1] == 0x02, "test")
	}

	// case 2
	{
		var output = VarUintEncoding(0xffffffffffffffff, nil, 0)
		check(len(output) == 10, "test")
		check(output[0] == 0xff, "test")
		check(output[1] == 0xff, "test")
		check(output[2] == 0xff, "test")
		check(output[3] == 0xff, "test")
		check(output[4] == 0xff, "test")
		check(output[5] == 0xff, "test")
		check(output[6] == 0xff, "test")
		check(output[7] == 0xff, "test")
		check(output[8] == 0xff, "test")
		check(output[9] == 0x01, "test")
	}

	// case 3
	{
		var output []byte = make([]byte, 0)

		output = VarUintEncoding(0x7f, output, 1)
		check(len(output) == 2, "test")
		output = VarUintEncoding(0x7f, output, 2)
		check(len(output) == 4, "test")
		output = VarUintEncoding(0x7f, output, 3)
		check(len(output) == 6, "test")
		output = VarUintEncoding(0x7f, output, 4)
		check(len(output) == 8, "test")
		output = VarUintEncoding(0x7f, output, 5)
		check(len(output) == 10, "test")
		output = VarUintEncoding(0x7f, output, 6)
		check(len(output) == 12, "test")

		check(output[0] == 0x7f, "test")
		check(output[1] == 0x01, "test")
		check(output[2] == 0x3f, "test")
		check(output[3] == 0x03, "test")
		check(output[4] == 0x1f, "test")
		check(output[5] == 0x07, "test")
		check(output[6] == 0x0f, "test")
		check(output[7] == 0x0f, "test")
		check(output[8] == 0x07, "test")
		check(output[9] == 0x1f, "test")
		check(output[10] == 0x03, "test")
		check(output[11] == 0x3f, "test")

		output = VarUintEncoding(0x7f, output, 7)
		check(output == nil, "test")
	}

	// case 4
	{
		var count int
		var output []byte
		var result uint64

		for i := uint64(1); i < 10000000000000000000; i *= 10 {
			output = VarUintEncoding(uint64(i), nil, 0)
			result, count = VarUintDecoding(output, 0)
			check(result == uint64(i), "test")
			check(count == len(output), "test")
		}

		for i := uint64(1); i < 10000000000000000000; i *= 10 {
			output = VarUintEncoding(uint64(i), nil, 1)
			result, count = VarUintDecoding(output, 1)
			check(result == uint64(i), "test")
			check(count == len(output), "test")
		}

		for i := uint64(1); i < 10000000000000000000; i *= 10 {
			output = VarUintEncoding(uint64(i), nil, 2)
			result, count = VarUintDecoding(output, 2)
			check(result == uint64(i), "test")
			check(count == len(output), "test")
		}

		for i := uint64(1); i < 10000000000000000000; i *= 10 {
			output = VarUintEncoding(uint64(i), nil, 3)
			result, count = VarUintDecoding(output, 3)
			check(result == uint64(i), "test")
			check(count == len(output), "test")
		}

		for i := uint64(1); i < 10000000000000000000; i *= 10 {
			output = VarUintEncoding(uint64(i), nil, 4)
			result, count = VarUintDecoding(output, 4)
			check(result == uint64(i), "test")
			check(count == len(output), "test")
		}

		for i := uint64(1); i < 10000000000000000000; i *= 10 {
			output = VarUintEncoding(uint64(i), nil, 5)
			result, count = VarUintDecoding(output, 5)
			check(result == uint64(i), "test")
			check(count == len(output), "test")
		}

		for i := uint64(1); i < 10000000000000000000; i *= 10 {
			output = VarUintEncoding(uint64(i), nil, 6)
			result, count = VarUintDecoding(output, 6)
			check(result == uint64(i), "test")
			check(count == len(output), "test")
		}

		for i := uint64(1); i < 10000000000000000000; i *= 10 {
			output = VarUintEncoding(uint64(i), nil, 7)
			check(len(output) == 0, "test")
			result, count = VarUintDecoding(output, 7)
			check(result == 0, "test")
			check(count == 0, "test")
		}
	}

	// case 5
	{
		var count int = 0
		var output []byte = make([]byte, 10)
		var result int64
		for i := int64(-util.InvalidPoint); i < 0; i /= 10 {
			output = output[0:0]
			output = VarIntEncoding(i, output, 0)
			result, count = VarIntDecoding(output, 0)
			check(result == i, "test")
			check(count == len(output), "test")
		}

		for i := int64(1); i > 0 && i < util.InvalidPoint; i *= 10 {
			output = output[0:0]
			output = VarIntEncoding(i, output, 0)
			result, count = VarIntDecoding(output, 0)
			check(result == i, "test")
			check(count == len(output), "test")
		}
	}

	check(true, "test")
}

func TestRunLength(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var output []byte = make([]byte, 3)
	var result int64
	var count int

	output = output[0:0]
	output = RunLengthEncoding(49, 100, output)
	check(len(output) == 3, "test")
	count, result = RunLengthDecoding(output)
	check(result == 49, "test")
	check(count == 100, "test")

	output = RunLengthEncoding(49, 100, output)
	check(len(output) == 6, "test")
	count, result = RunLengthDecoding(output[3:])
	check(result == 49, "test")
	check(count == 100, "test")

	check(true, "test")
}

func TestSimple8B(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	// case 1
	{
		var input []uint64 = make([]uint64, 1024)
		var output []uint64 = make([]uint64, 1024)
		output = output[0:0]

		for i := 0; i < 1000; i++ {
			input[i] = util.UINT64_MAX
		}
		output = Simple8BEncodingForUint64(input[0:100], output)
		check(len(output) == 1, "test")
		check(output[0] == 100|0x1000000000000000, "test")
	}

	// case 2
	{
		var input []uint64 = make([]uint64, 1024)
		var output []uint64 = make([]uint64, 1024)
		output = output[0:0]

		for i := 0; i < 20; i++ {
			input[i] = util.UINT64_MAX
		}
		for i := 20; i < 100; i++ {
			input[i] = 0x1
		}
		output = Simple8BEncodingForUint64(input[0:100], output)
		check(len(output) == 3, "test")
		check(output[0] == 0x1000000000000014, "test")
		check(output[1] == 0x2fffffffffffffff, "test")
		check(output[2] == 0x4249249249249249, "test")
	}

	// case 3
	{
		var input []uint64 = make([]uint64, 1024)
		var output []uint64 = make([]uint64, 1024)

		var result []uint64 = []uint64{
			0x0,
			0x2fffffffffffffff, 0x3aaaaaaaaaaaaaaa, 0x4924924924924924, 0x5888888888888888,
			0x6842108421084210, 0x7820820820820820, 0x8081020408102040, 0x9080808080808080,
			0xa401004010040100, 0xa802008020080200, 0xb400400400400400, 0xb800800800800800,
			0xc200040008001000, 0xc400080010002000, 0xc800100020004000, 0xd080000800008000,
			0xd100001000010000, 0xd200002000020000, 0xd400004000040000, 0xd800008000080000,
			0xe004000000100000, 0xe008000000200000, 0xe010000000400000, 0xe020000000800000,
			0xe040000001000000, 0xe080000002000000, 0xe100000004000000, 0xe200000008000000,
			0xe400000010000000, 0xe800000020000000, 0xf000000040000000, 0xf000000080000000,
			0xf000000100000000, 0xf000000200000000, 0xf000000400000000, 0xf000000800000000,
			0xf000001000000000, 0xf000002000000000, 0xf000004000000000, 0xf000008000000000,
			0xf000010000000000, 0xf000020000000000, 0xf000040000000000, 0xf000080000000000,
			0xf000100000000000, 0xf000200000000000, 0xf000400000000000, 0xf000800000000000,
			0xf001000000000000, 0xf002000000000000, 0xf004000000000000, 0xf008000000000000,
			0xf010000000000000, 0xf020000000000000, 0xf040000000000000, 0xf080000000000000,
			0xf100000000000000, 0xf200000000000000, 0xf400000000000000, 0xf800000000000000,
		}
		for i := 1; i <= 60; i++ {
			var tmp = 0x1 << uint(i-1)
			for j := 0; j < 60/i; j++ {
				input[j] = uint64(tmp)
			}
			output = output[0:0]
			output = Simple8BEncodingForUint64(input[0:60/i], output)
			check(len(output) == 1, "test")
			tmp <<= 1
			check(output[0] == result[i], "test")
		}
	}

	// case 4
	{
		var input []uint64 = make([]uint64, 1024)
		var output []uint64

		var result []uint64
		for i := 0; i < 1000; i++ {
			input[i] = util.UINT64_MAX
		}
		output = Simple8BEncodingForUint64(input[0:100], nil)
		check(len(output) == 1, "test")
		result = Simple8BDecodingForUint64(output, nil)
		check(len(result) == 100, "test")
	}

	// case 5
	{
		var n = 1024
		var input []int64 = make([]int64, n)
		var output []uint64 = make([]uint64, n)
		var result []int64 = make([]int64, n)
		output = output[0:0]
		result = result[0:0]

		// first save n/2
		for i := 0; i < n/2; i++ {
			input[i] = int64(rand.Intn(0xffff))
		}
		output = Simple8BEncodingForInt64(input[:n/2], output)
		result = Simple8BDecodingForInt64(output, result)
		check(len(result) == n/2, "test")
		for i := 0; i < n/2; i++ {
			check(input[i] == result[i], "test")
		}

		// second save n/2
		for i := n / 2; i < n; i++ {
			input[i] = int64(rand.Intn(0xffff))
		}
		var length = len(output)
		output = Simple8BEncodingForInt64(input[n/2:], output)
		result = Simple8BDecodingForInt64(output[length:], result)

		check(len(result) == n, "test")
		for i := 0; i < n; i++ {
			check(input[i] == result[i], "test")
		}
	}

	// case 6
	{
		var n = 1024
		var input []uint64 = make([]uint64, n)
		var output []uint64 = make([]uint64, n)
		var result []uint64 = make([]uint64, n)
		output = output[0:0]
		result = result[0:0]

		// first save n/2
		for i := 0; i < n/2; i++ {
			input[i] = uint64(rand.Intn(0xffff))
		}
		output = Simple8BEncodingForUint64(input[:n/2], output)
		result = Simple8BDecodingForUint64(output, result)
		check(len(result) == n/2, "test")
		for i := 0; i < n/2; i++ {
			check(input[i] == result[i], "test")
		}

		// second save n/2
		for i := n / 2; i < n; i++ {
			input[i] = uint64(rand.Intn(0xffff))
		}
		var length = len(output)
		output = Simple8BEncodingForUint64(input[n/2:], output)
		result = Simple8BDecodingForUint64(output[length:], result)

		check(len(result) == n, "test")
		for i := 0; i < n; i++ {
			check(input[i] == result[i], "test")
		}
		return
	}

	// case 7
	{
		var input []uint64 = []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1}
		var output []uint64
		var result []uint64
		output = Simple8BEncodingForUint64(input, output)
		check(len(output) == 2, "test")
		result = Simple8BDecodingForUint64(output, result)
		check(len(result) == 9, "test")
		for i := 0; i < 9; i++ {
			check(input[i] == result[i], "test")
		}
	}

	// case 8
	{
		var input []int64 = []int64{
			util.NullData, util.NullData,
			1, 1, 1, 1, 1, 1, 1, 1, 1,
			util.NullData, util.NullData, util.NullData,
			1, 1, 1, 1, 1, 1, 1, 1, 1,
			util.NullData,
		}
		for i := 0; i < 1024; i++ {
			input = append(input, int64(rand.Intn(0xffff)))
		}
		var output []byte = make([]byte, 1024)
		var result []int64 = make([]int64, len(input))
		output = output[0:0]
		output = Simple8BEncodingToBytes(input, output)
		result = result[0:0]
		result = Simple8BDecodingFromBytes(output, result)
		for i, it := range result {
			check(it == input[i], "test")
		}
	}

	// case 9
	{
		var n = 1024
		var input []int64 = make([]int64, n)
		var output []byte = make([]byte, n*8)
		var result []int64 = make([]int64, n)
		output = output[0:0]
		result = result[0:0]

		// first save n/2
		for i := 0; i < n/2; i++ {
			input[i] = int64(rand.Intn(0xffff))
		}
		output = Simple8BEncodingToBytes(input[:n/2], output)
		result = Simple8BDecodingFromBytes(output, result)
		check(len(result) == n/2, "test")
		for i := 0; i < n/2; i++ {
			check(input[i] == result[i], "test")
		}

		// second save n/2
		for i := n / 2; i < n; i++ {
			input[i] = int64(rand.Intn(0xffff))
		}
		var length = len(output)
		output = Simple8BEncodingToBytes(input[n/2:], output)
		result = Simple8BDecodingFromBytes(output[length:], result)

		check(len(result) == n, "test")
		for i := 0; i < n; i++ {
			check(input[i] == result[i], "test")
		}
		return
	}

	check(true, "test")
}
