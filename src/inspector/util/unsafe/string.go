/*
// =====================================================================================
//
//       Filename:  string.cpp
//
//    Description:  字符串处理
//
//        Version:  1.0
//        Created:  09/07/2018 04:39:49 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package unsafe

/*
// ===  FUNCTION  ======================================================================
//         Name:  InPlaceReverseString
//  Description:  字符串反转
// =====================================================================================
*/
func InPlaceReverseString(input *string) string {
	var bytes = String2Bytes(*input)
	var i = 0
	var j = len(*input) - 1
	for i < j {
		bytes[i], bytes[j] = bytes[j], bytes[i]
		i++
		j--
	}
	return *input
}
