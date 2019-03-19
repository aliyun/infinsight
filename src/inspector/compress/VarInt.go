/*
// =====================================================================================
//
//       Filename:  VarIntEncoding.go
//
//    Description:  可预留首字节高n比特的varint压缩算法
//                  所有varint编码对于预分配输出（即output参数）采取追加写方式
//
//                  说明：预留比特的赋值需要在varint压缩之后进行，varint不会假设output[0]中有数据
//
//        Version:  1.0
//        Created:  07/25/2018 05:11:35 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package compress

/*
// ===  FUNCTION  ======================================================================
//         Name:  VarUintEncoding
//  Description:  output len should be great than or equal 10
//                "reserve" mean skip n bit of offset 0 for reservation, [reserve < 7]
// =====================================================================================
*/
func VarUintEncoding(input uint64, output []byte, reserve int) []byte {
	if reserve > 6 || reserve < 0 {
		return nil
	}

	if output == nil {
		output = make([]byte, 0)
	}

	// reserve n + 1, n is reservation for outside, 1 is for myself
	var firstBits uint = uint(8 - 1 - reserve)
	var firstMask byte = byte(1 << firstBits)
	var currentByte byte

	// do first byte
	currentByte = byte(input) & (firstMask - 1)
	input >>= firstBits
	if input > 0 {
		currentByte |= firstMask
	}
	output = append(output, currentByte)

	// do other bytes
	for input > 0 {
		currentByte = byte(input) & 0x7f
		input >>= 7
		if input > 0 {
			currentByte |= 0x80
		}
		output = append(output, currentByte)
	}

	return output
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  VarUintDecoding
//  Description:  "reserve" mean skip n bit of first for reservation
// =====================================================================================
*/
func VarUintDecoding(input []byte, reserve int) (uint64, int) {
	if reserve > 6 || reserve < 0 {
		return 0, 0
	}

	// reserve n + 1, n is reservation for outside, 1 is for myself
	var firstBits uint = uint(8 - 1 - reserve)
	var firstMask byte = byte(1 << firstBits)
	var n int = 0
	var shift uint = 0
	var result uint64 = 0

	result = uint64(input[n] & (firstMask - 1))
	if input[n]&firstMask == 0 {
		return result, 1
	} else {
		n++
		shift += firstBits
	}

	for {
		var tmp = uint64(input[n] & 0x7f)
		tmp <<= shift
		result |= tmp
		if input[n]&0x80 == 0 {
			return result, n + 1
		} else {
			n++
			shift += 7
		}
	}

	return 0, 0
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  VarIntEncoding
//  Description:  output should be 10 len
//                "reserve" mean skip n bit of first for reservation, [reserve < 6]
// =====================================================================================
*/
func VarIntEncoding(input int64, output []byte, reserve int) []byte {
	return VarUintEncoding(OriginDistanceEncode(input), output, reserve)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  VarIntDecoding
//  Description:  "reserve" mean skip n bit of first for reservation
// =====================================================================================
*/
func VarIntDecoding(input []byte, reserve int) (int64, int) {
	uresult, count := VarUintDecoding(input, reserve)
	result := OriginDistanceDecode(uresult)
	return result, count
}
