/*
 * this file is high level compress function that calls other compress function inner.
 */
package compress

import (
	"fmt"
	"bytes"
	"encoding/binary"
	"inspector/util"
)

const (
	/*
	 * prefix encoding like huffman coding, todo, if more compress type added,
	 * it's better to change to huffman tree.
	 */
	SameDigitCompress byte = 0x00 // 0000 0000 -> 00
	DiffCompress      byte = 0x40 // 0100 0000 -> 01
	NoCompress        byte = 0x80 // 1000 0000 -> 10
	UnknownCompress   byte = 0x11 // 1100 0000 -> 11

	maxCompressNumber = 8196 // restrict the max compress number

	byteMask = 255
)

var (
	compressBitMap = map[byte]int{
		SameDigitCompress: 2,
		DiffCompress:      2,
		NoCompress:        2,
	}
)

/*
 * @input: compress type, followed by different parameters
 *     same digit: same value(int64) + count(int64)
 *     diff compress: gcd(int64) + value list([]int64)
 *     no compress: value list([]int64)
 * @return:
 *     same digit: compress type + same value(var-int) + count(var-int)
 *     diff compress: compress type + gcd(var-int) + array(diff + OD + simple8b)
 *     no compress: compress type(2 of 8 bits used) + binary flow
 * please pay attention: the input array may be modified to save memory
 */
func Compress(compressType byte, params ...interface{}) ([]byte, error) {
	flagBits, ok := compressBitMap[compressType]
	if !ok {
		return nil, fmt.Errorf("compress type not supported")
	}

	switch compressType {
	case SameDigitCompress:
		if len(params) < 2 {
			return nil, fmt.Errorf("compress input parameter illegal")
		}

		count := params[0].(int)
		sameValue := params[1].(int64)
		if count < 0 || count >= maxCompressNumber {
			return nil, fmt.Errorf("compress count[%d] bigger than the threshold[%d]",
				count, maxCompressNumber)
		}

		output := RunLengthWithLeadingEncoding(sameValue, int(count), flagBits, nil)
		output[0] |= SameDigitCompress

		return output, nil
	case DiffCompress:
		if len(params) < 2 {
			return nil, fmt.Errorf("compress input parameter illegal")
		}

		gcd := params[0].(int64)
		valList := params[1].([]int64)
		if gcd == 0 { // set default to 1 if all data is mix of 0 and null data
			gcd = 1
		}

		// calculate diff array, ignore the NullData
		var prev int64
		hasNegativeOrNull := false
		for i := range valList {
			if valList[i] == util.NullData {
				hasNegativeOrNull = true
				continue
			}
			valList[i], prev = valList[i] - prev, valList[i]
			valList[i] /= gcd // gcd after diff
			if valList[i] < 0 {
				hasNegativeOrNull = true
			}
		}

		simple8bEncoding := make([]uint64, 0, len(valList))

		// simple8bit compress
		// gcd will be < 0 when has negative or null data
		if hasNegativeOrNull {
			gcd = -gcd
			simple8bEncoding = Simple8BEncodingForInt64(valList, simple8bEncoding)
		} else {
			// copy valList to []uint64 array
			uintValList := make([]uint64, len(valList))
			for i, val := range valList {
				uintValList[i] = uint64(val)
			}
			simple8bEncoding = Simple8BEncodingForUint64(uintValList, simple8bEncoding)
		}

		// encoding gcd(varint)
		byteFlow := VarIntEncoding(gcd, nil, flagBits)
		// set flag
		byteFlow[0] |= DiffCompress

		// encoding array(nothing but convert to byte flow directly)
		byteFlow = array2byteFlow(simple8bEncoding, byteFlow)

		return byteFlow, nil
	case NoCompress:
		valList := params[0].([]int64)

		output := new(bytes.Buffer)
		output.Grow(1 + 8 * len(valList))
		output.WriteByte(NoCompress)
		for _, val := range valList {
			binary.Write(output, binary.BigEndian, val)
		}

		return output.Bytes(), nil
	default:
		return nil, fmt.Errorf("compress type not supported")
	}
}

// output array will be append
func Decompress(input []byte, output []int64) ([]int64, error) {
	if len(input) == 0 {
		return []int64{}, nil
	}
	compressType := parseCompressType(input[0])
	flagBits, ok := compressBitMap[compressType]
	if !ok {
		return nil, fmt.Errorf("decompress type not supported")
	}

	if output == nil {
		output = make([]int64, 0)
	}

	switch compressType {
	case SameDigitCompress:
		count, sameValue := RunLengthWithLeadingDecoding(input, flagBits)
		if count < 0 || count > maxCompressNumber {
			return nil, fmt.Errorf("decompress count[%d] bigger than the threshold[%d]",
				count, maxCompressNumber)
		}

		for i := 0; i < count; i++ {
			output = append(output, sameValue)
		}
	case DiffCompress:
		gcd, n := VarIntDecoding(input, flagBits)
		if gcd == 0 {
			return nil, fmt.Errorf("decompress gcd is 0")
		}

		// convert byte flow to uint64 array
		simple8bEncoding := byteFlow2array(input[n:])

		previousLen := len(output)
		// decode simple8bit
		if gcd < 0 {
			output = Simple8BDecodingForInt64(simple8bEncoding, output)
			var prev int64
			gcd = -gcd
			for i := previousLen; i < len(output); i++ {
				if output[i] == util.NullData {
					continue
				}
				output[i] = prev + output[i]
				prev = output[i]
				output[i] *= gcd
			}
		} else {
			uintOutput := make([]uint64, 0, len(input))
			uintOutput = Simple8BDecodingForUint64(simple8bEncoding, uintOutput)
			var prev int64
			for _, valUint := range uintOutput {
				tmp := prev + int64(valUint)
				output = append(output, tmp * gcd)
				prev = tmp
			}
		}
	case NoCompress:
		readBuf := bytes.NewReader(input[1: ])
		for readBuf.Len() != 0 {
			var x int64
			binary.Read(readBuf, binary.BigEndian, &x)
			output = append(output, x)
		}
	default:
		return nil, fmt.Errorf("decompress type not supported")
	}
	return output, nil
}

// little-endian
func array2byteFlow(input []uint64, output []byte) []byte {
	if output == nil {
		output = make([]byte, 0)
	}
	for _, ele := range input {
		for j := 0; j < 8; j++ {
			output = append(output, byte(ele&byteMask))
			ele >>= 8
		}
	}
	return output
}

func byteFlow2array(input []byte) []uint64 {
	var byteNr uint = 0
	output := make([]uint64, 0, len(input)/4)
	var out uint64
	for _, ele := range input {
		out |= uint64(ele) << byteNr
		byteNr = (byteNr + 8) % 64
		if byteNr == 0 {
			output = append(output, out)
			out = 0
		}
	}
	return output
}

// todo, need change to huffman tree parse when type number increased
func parseCompressType(input byte) byte {
	return input & (0xc0) // currently, we only use 2 bits
}
