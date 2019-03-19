/*
// =====================================================================================
//
//       Filename:  math.go
//
//    Description:  基本数学工具
//
//        Version:  1.0
//        Created:  07/04/2018 09:42:56 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package util

import (
	"fmt"
)

const (
	INT_MAX  int  = int(^uint(0) >> 1)
	INT_MIN  int  = ^INT_MAX
	UINT_MAX uint = ^uint(0)
	UINT_MIN uint = ^UINT_MAX

	INT32_MAX  int32  = int32(^uint32(0) >> 1)
	INT32_MIN  int32  = ^INT32_MAX
	UINT32_MAX uint32 = ^uint32(0)
	UINT32_MIN uint32 = ^UINT32_MAX

	INT64_MAX  int64  = int64(^uint64(0) >> 1)
	INT64_MIN  int64  = ^INT64_MAX
	UINT64_MAX uint64 = ^uint64(0)
	UINT64_MIN uint64 = ^UINT64_MAX

	NullData     int64 = INT64_MAX
	InvalidPoint int64 = (1 << 61) - 1

	InvalidShortKey = ""
)

/*
 * 1 digit: 0-65
 * 2 digits: 0-4355
 * 3 digits: 0-287495
 */
var (
	representation string = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_+="
	repMap         map[byte]int
)

func init() {
	repMap = make(map[byte]int, len(representation))
	for i := range representation {
		repMap[representation[i]] = i
	}
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  GCD
//  Description:  最大公约数
// =====================================================================================
*/
func GCD(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}

	if a < 0 {
		return -a
	}
	return a
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  LCM
//  Description:  最小公倍数
// =====================================================================================
*/
func LCM(a, b int64) int64 {
	res := GCD(a, b)
	return a * b / res
}

// int to string which is 67-digit representation, return "" means illegal
func RepInt2String(x int) string {
	n := len(representation)
	var ans []byte
	for x >= 0 {
		ans = append(ans, representation[x%n])
		x /= n
		if x == 0 {
			break
		}
	}

	// reverse ans
	le := len(ans)
	for i := 0; i < le/2; i++ {
		ans[i], ans[le-i-1] = ans[le-i-1], ans[i]
	}

	return string(ans)
}

// string to int
func RepString2Int(input string) (int, error) {
	if len(input) == 0 {
		return -1, fmt.Errorf("input invalid")
	}
	sum := 0
	n := len(representation)
	for i := range input {
		if x, ok := repMap[input[i]]; !ok {
			return -1, fmt.Errorf("input invalid")
		} else {
			sum = sum*n + x
		}
	}
	return sum, nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  10进制数反转
//  Description:
// =====================================================================================
*/
func IntReverse(n int) int {
	var r = 0
	for n > 0 {
		r *= 10
		r += n % 10
		n /= 10
	}
	return r
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  10进制数反转 for int32
//  Description:  此处假设体系结构中len(int)>=len(int32)
// =====================================================================================
*/
func Int32Reverse(n int32) int32 {
	return int32(IntReverse(int(n)))
}