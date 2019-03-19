/*
// =====================================================================================
//
//       Filename:  CollectorManager_test.go
//
//    Description:
//
//        Version:  1.0
//        Created:  06/22/2018 07:14:57 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package collectorManager

import "fmt"
import "runtime"
import "testing"

func TestTCB(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	check(true, "test")
}
