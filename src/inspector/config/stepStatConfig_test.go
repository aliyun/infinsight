/*
// =====================================================================================
//
//       Filename:  stepStatConfig.go
//
//    Description:
//
//        Version:  1.0
//        Created:  06/12/2018 06:42:56 PM
//       Compiler:  g++
//
// =====================================================================================
*/

package config

import (
	"fmt"
	"os/exec"
	"runtime"
	"testing"
)

func TestStepStatConfig(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var err error
	var count int = 0

	var step *stepStatConfig = &stepStatConfig{
		filename: "test.cfg",
		callback: func(event WatcheEvent) error {
			if event == NODECHANGED {
				count++
			}
			return nil
		},
	}

	_, err = step.DoStep(nil)
	check(err == nil, "test")
	check(count == 1, "test")

	_, err = step.DoStep(nil)
	check(err == nil, "test")
	check(count == 1, "test")

	var cmd = exec.Command("/bin/bash", "-c", "touch test.cfg")
	cmd.Run()

	_, err = step.DoStep(nil)
	check(err == nil, "test")
	check(count == 2, "test")

	_, err = step.DoStep(nil)
	check(err == nil, "test")
	check(count == 2, "test")

	check(true, "test")
}
