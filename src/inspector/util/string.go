/*
// =====================================================================================
//
//       Filename:  string.go
//
//    Description:
//
//        Version:  1.0
//        Created:  09/28/2018 06:07:05 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package util

import "bytes"

/*
// ===  FUNCTION  ======================================================================
//         Name:  StringLTrim
//  Description:  去除字符串左侧空白符
// =====================================================================================
*/
func StringLTrim(str string) string {
	for i, it := range str {
		if it != ' ' && it != '\t' && it != '\n' && it != '\r' {
			return str[i:]
		}
	}
	return str
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  StringRTrim
//  Description:  去除字符串右侧空白符
// =====================================================================================
*/
func StringRTrim(str string) string {
	for i := len(str) - 1; i >= 0; i-- {
		if str[i] != ' ' && str[i] != '\t' && str[i] != '\n' && str[i] != '\r' {
			return str[:i+1]
		}
	}
	return str
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  StringTrim
//  Description:  去除字符串两侧空白符
// =====================================================================================
*/
func StringTrim(str string) string {
	var result = str
	result = StringLTrim(result)
	result = StringRTrim(result)
	return result
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  StringReverse
//  Description:  字符串反转
// =====================================================================================
*/
func StringReverse(input string) string {
	var bytes = make([]rune, len(input))
	var last = len(input) - 1
	for i, it := range input {
		bytes[last-i] = it
	}
	return string(bytes)
}

// used in step-parse
/*
 * pay attention to the boundary and overflow. truncate the last n connectors(includes word).
 * e.g.,
 *     byteBuffer = "abc|hello|world" nr = 1 => byteBuffer = "abc|hello"
 *     byteBuffer = "abc|hello|world" nr = 0 => byteBuffer = "abc|hello|world"
 */
func BackTracking(byteBuffer *bytes.Buffer, nr int, connector byte) {
	if nr <= 0 {
		return
	}
	data := byteBuffer.Bytes()
	for i := len(data) - 1; i >= 0; i-- {
		if data[i] == connector {
			nr--
			if nr <= 0 {
				byteBuffer.Truncate(i)
				return
			}
		}
	}
	byteBuffer.Truncate(0)
}
