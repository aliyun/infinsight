/*
// =====================================================================================
//
//       Filename:  Simple8B.go
//
//    Description:
//       Simple8b is 64bit word-sized encoder that packs multiple integers into a single word using
//       a 4 bit selector values and up to 60 bits for the remaining values.  Integers are encoded using
//       the following table:
//
//       ┌──────────────┬─────────────────────────────────────────────────────────┐
//       │   Selector   │   0   1   2   3   4   5   6   7  8  9 10 11 12 13 14 15 │
//       ├──────────────┼─────────────────────────────────────────────────────────┤
//       │     Bits     │   0   1   1   2   3   4   5   6  7  8 10 12 15 20 30 60 │
//       ├──────────────┼─────────────────────────────────────────────────────────┤
//       │      N       │   *  60  60  30  20  15  12  10  8  7  6  5  4  3  2  1 │
//       └──────────────┴─────────────────────────────────────────────────────────┘
//
//       selector 0 and selector 1 is special
//       0: count of next uint64
//       1: use bit 1 as count of null
//
//        Version:  1.0
//        Created:  07/25/2018 05:11:35 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package compress

import (
	"bytes"
	"encoding/binary"
	"inspector/util"
	"math/bits"
)

// map: bit size to selector
var selectorTable [61]int = [61]int{
	2,
	2, 3, 4, 5, 6, 7, 8, 9, 10, 10,
	11, 11, 12, 12, 12, 13, 13, 13, 13, 13,
	14, 14, 14, 14, 14, 14, 14, 14, 14, 14,
	15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
	15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
	15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
}

// map: selector to data count
var countTable [16]int = [16]int{
	-1, -1, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1,
}

// map: selector to bit size
var sizeTable [16]int = [16]int{
	-1, 60, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60,
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Simple8BEncodingForUint64
//  Description:
// =====================================================================================
*/
func Simple8BEncodingForUint64(input []uint64, output []uint64) []uint64 {
	// check output
	if output == nil {
		output = make([]uint64, 0)
	}

	var maxBits int = 0
	var newMaxBits int = 0
	var nullCount uint64 = 0
	var dataCount int = 0

	var inputIndex int = 0

	// compress with simple 8B
	var doCompress = func(bits, count int) {
		// head of count diff
		if countTable[selectorTable[bits]]-count > 0 {
			output = append(output, uint64(count))
		}

		// compress data
		var selector = selectorTable[bits]
		var selectorMask = uint64(selector << 60)
		var shift uint = uint(sizeTable[selector])
		var tmpData uint64 = 0
		for i := 0; i < count; i++ {
			tmpData <<= shift
			tmpData |= uint64(input[inputIndex])
			inputIndex++
		}
		tmpData |= selectorMask
		output = append(output, tmpData)

	}

	// compress loop
	for _, it := range input {
		// null data
		if it > 0x0fffffffffffffff {
			if dataCount != 0 {
				var useBits int = 60 / dataCount
				// if 60%dataCount != 0 {
				// 	useBits++
				// }
				doCompress(useBits, dataCount)
				// reset counter
				dataCount = 0
				maxBits = 0
			}

			nullCount++
			inputIndex++
			continue
		}
		if nullCount != 0 {
			// compress null data
			output = append(output, nullCount|0x1000000000000000)
			// reset counter
			nullCount = 0
		}

		// normal data previous
		if countTable[selectorTable[maxBits]] == dataCount {
			doCompress(maxBits, dataCount)
			// reset counter
			dataCount = 0
			maxBits = 0
		}
		var n = bits.LeadingZeros64(it)
		n = 64 - n
		newMaxBits = max(maxBits, n)
		dataCount++
		// check whether out of range if add current data
		if countTable[selectorTable[newMaxBits]] < dataCount {
			// safe previous data
			doCompress(maxBits, dataCount-1)
			// reset counter
			dataCount = 1
			maxBits = n
		} else {
			maxBits = newMaxBits
		}
	}

	// process final data
	if nullCount != 0 {
		output = append(output, nullCount|0x1000000000000000)
		return output
	}
	if dataCount != 0 {
		var useBits int = 60 / dataCount
		// if 60%dataCount != 0 {
		// 	useBits++
		// }
		doCompress(useBits, dataCount)
	}
	return output
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Simple8BDecodingForUint64
//  Description:
// =====================================================================================
*/
func Simple8BDecodingForUint64(input []uint64, output []uint64) []uint64 {
	// check output
	if output == nil {
		output = make([]uint64, 0)
	}

	var index int = len(output)
	var count int = 0
	for _, it := range input {
		var selector int = int(it >> 60)
		var shift uint = uint(sizeTable[selector])
		var mask uint64 = ^(0xffffffffffffffff << shift)
		if selector == 0x0 {
			count = int(it)
		} else if selector == 0x1 {
			count = 0x7fffffff & int(it)
			for i := 0; i < count; i++ {
				output = append(output, util.UINT64_MAX)
			}
			index += count
			count = 0
		} else {
			if count == 0 {
				count = countTable[selector]
			}
			for i := 0; i < count; i++ {
				output = append(output, util.UINT64_MAX)
			}
			for i := count - 1; i >= 0; i-- {
				output[index+i] = it & mask
				it >>= shift
			}
			index += count
			count = 0
		}
	}
	return output
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Simple8BEncodingForInt64
//  Description:
// =====================================================================================
*/
func Simple8BEncodingForInt64(input []int64, output []uint64) []uint64 {
	var uinput []uint64 = make([]uint64, len(input))

	// change int64 to uint64
	for i, it := range input {
		uinput[i] = OriginDistanceEncode(it)
	}

	return Simple8BEncodingForUint64(uinput, output)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Simple8BDecodingForInt64
//  Description:
// =====================================================================================
*/
func Simple8BDecodingForInt64(input []uint64, output []int64) []int64 {
	var uoutput []uint64
	if output == nil {
		uoutput = make([]uint64, len(input))
	} else {
		uoutput = make([]uint64, len(output))
	}
	uoutput = uoutput[0:0]

	uoutput = Simple8BDecodingForUint64(input, uoutput)

	// change int64 to uint64
	for _, it := range uoutput {
		if it == util.UINT64_MAX {
			output = append(output, util.INT64_MAX)
		} else {
			output = append(output, OriginDistanceDecode(it))
		}
	}

	return output
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Simple8BEncodingToBytes
//  Description:
// =====================================================================================
*/
func Simple8BEncodingToBytes(input []int64, output []byte) []byte {
	var uoutput = make([]uint64, len(input))
	uoutput = uoutput[0:0]
	uoutput = Simple8BEncodingForInt64(input, uoutput)

	var buf *bytes.Buffer
	if output == nil {
		buf = bytes.NewBuffer(make([]byte, 0))
	} else {
		buf = bytes.NewBuffer(output)
	}
	var countBuff = make([]byte, 10)
	for _, it := range uoutput {
		if it&0xf000000000000000 == 0x0 {
			// varint to save non-in-table count
			buf.WriteByte(0x0)
			countBuff = countBuff[0:0]
			buf.Write(VarUintEncoding(it, countBuff, 0))
		} else if it&0xf000000000000000 == 0x1000000000000000 {
			// varint to save null data count
			buf.WriteByte(0x1)
			countBuff = countBuff[0:0]
			buf.Write(VarUintEncoding(0x0fffffffffffffff|it, countBuff, 0))
		} else {
			// 8 bytes to save data
			binary.Write(buf, binary.BigEndian, it)
		}
	}
	return buf.Bytes()
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Simple8BDecodingFromBytes
//  Description:
// =====================================================================================
*/
func Simple8BDecodingFromBytes(input []byte, output []int64) []int64 {

	if output == nil {
		output = make([]int64, 0)
	}
	var uoutput = make([]uint64, 0)

	var i = 0
	var data uint64
	var count int
	for i < len(input) {
		if input[i] == 0x0 {
			// read count of next data
			i++
			data, count = VarUintDecoding(input[i:], 0)
			uoutput = append(uoutput, data)
			i += count
		} else if input[i] == 0x10 {
			// read count of null data
			i++
			data, count = VarUintDecoding(input[i:], 0)
			uoutput = append(uoutput, data|0x1000000000000000)
			i += count
		} else {
			// read uint64
			data = binary.BigEndian.Uint64(input[i:])
			i += 8
			uoutput = append(uoutput, data)
		}
	}
	return Simple8BDecodingForInt64(uoutput, output)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  max
//  Description:
// =====================================================================================
*/
func max(x, y int) int {
	if x >= y {
		return x
	} else {
		return y
	}
}
