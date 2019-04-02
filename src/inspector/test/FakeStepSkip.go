/*
// =====================================================================================
//
//       Filename:  FakeStepSkip.cpp
//
//    Description:  用于测试的假step，step中途跳过
//
//        Version:  1.0
//        Created:  06/12/2018 06:42:56 PM
//       Compiler:  g++
//
// =====================================================================================
*/

package test

type FakeStepSkip struct {
	BeforeCount int
	DoStepCount int
	AfterCount  int
	Data        int
}

func (step *FakeStepSkip) Name() string {
	return "FakeStepSkip"
}

func (step *FakeStepSkip) Error() error {
	return nil
}

func (step *FakeStepSkip) Before(input interface{}, params ...interface{}) (bool, error) {
	step.Data = 0
	if input != nil {
		if v, ok := input.(int); ok {
			step.Data += v
		}
	}
	step.BeforeCount++
	return false, nil
}

func (step *FakeStepSkip) DoStep(input interface{}, params ...interface{}) (interface{}, error) {
	var output int
	if input != nil {
		if v, ok := input.(int); ok {
			step.Data += v
			v++
			output = v
		}
	} else {
		output = 1
	}
	step.DoStepCount++
	return output, nil
}

func (step *FakeStepSkip) After(input interface{}, params ...interface{}) (bool, error) {
	if input != nil {
		if v, ok := input.(int); ok {
			step.Data += v
		}
	}
	step.AfterCount++
	return true, nil
}
