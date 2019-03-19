/*
// =====================================================================================
//
//       Filename:  RunLengthEncoding.go
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

/*
// ===  FUNCTION  ======================================================================
//         Name:  RunLengthEncoding
//  Description:  first bit for sign, 10 is run-length-encoding，11 is others
//                use varint for saving
//                data struct: flag + len + value
// =====================================================================================
*/
func RunLengthEncoding(input int64, length int, output []byte) []byte {
	if output == nil {
		output = make([]byte, 0)
	}

	var tmp = output
	tmp = VarUintEncoding(uint64(length), tmp, 2)
	tmp = VarUintEncoding(OriginDistanceEncode(input), tmp, 0)

	return tmp
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  RunLengthDecoding
//  Description:  first bit for sign, 10 is run-length-encoding，11 is others
// =====================================================================================
*/
func RunLengthDecoding(input []byte) (int, int64) {
	var len int
	var result uint64
	var n int
	result, n = VarUintDecoding(input[0:], 2)
	len = int(result)
	result, n = VarUintDecoding(input[n:], 0)
	return len, OriginDistanceDecode(result)
}



func RunLengthWithLeadingEncoding(input int64, length, leading int, output []byte) []byte {
	if output == nil {
		output = make([]byte, 0)
	}

	var tmp = output
	tmp = VarUintEncoding(uint64(length), tmp, leading)
	tmp = VarUintEncoding(OriginDistanceEncode(input), tmp, 0)

	return tmp
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  RunLengthDecoding
//  Description:  first bit for sign, 10 is run-length-encoding，11 is others
// =====================================================================================
*/
func RunLengthWithLeadingDecoding(input []byte, leading int) (int, int64) {
	var len int
	var result uint64
	var n int
	result, n = VarUintDecoding(input[0:], leading)
	len = int(result)
	result, n = VarUintDecoding(input[n:], 0)
	return len, OriginDistanceDecode(result)
}
