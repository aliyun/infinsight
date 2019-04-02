/*
// =====================================================================================
//
//       Filename:  FakeStep.cpp
//
//    Description:  用于测试的假step
//
//        Version:  1.0
//        Created:  06/12/2018 06:42:56 PM
//       Compiler:  g++
//
// =====================================================================================
*/

package test

type FakeStep struct {
	BeforeCount int
	DoStepCount int
	AfterCount  int
	Data        int
}

func (step *FakeStep) Name() string {
	return "FakeStep"
}

func (step *FakeStep) Error() error {
	return nil
}

func (step *FakeStep) Before(input interface{}, params ...interface{}) (bool, error) {
	step.Data = 0
	if input != nil {
		if v, ok := input.(int); ok {
			step.Data += v
		}
	}
	step.BeforeCount++
	return true, nil
}

func (step *FakeStep) DoStep(input interface{}, params ...interface{}) (interface{}, error) {
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

func (step *FakeStep) After(input interface{}, params ...interface{}) (bool, error) {
	if input != nil {
		if v, ok := input.(int); ok {
			step.Data += v
		}
	}
	step.AfterCount++
	return true, nil
}
