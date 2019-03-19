/*
// =====================================================================================
//
//       Filename:  string_test.go
//
//    Description:
//
//        Version:  1.0
//        Created:  07/16/2018 07:43:47 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package unsafe

import (
	"fmt"
	"runtime"
	"testing"
)

func TestString(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var s = fmt.Sprintf("hello world")
	fmt.Println("test begin: ")
	var rs = InPlaceReverseString(&s)
	fmt.Println("test end: ")
	check(s == "dlrow olleh", "test")
	check(s == rs, "test")
	check(&s != &rs, "test")
}
