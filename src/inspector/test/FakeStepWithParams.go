/*
// =====================================================================================
//
//       Filename:  FakeStepWithParams.cpp
//
//    Description:  用于测试的假step，并接收*int类型参数
//
//        Version:  1.0
//        Created:  06/12/2018 06:42:56 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Elwin.Gao (elwin), elwin.gao4444@gmail.com
//        Company:
//
// =====================================================================================
*/

package test

type FakeStepWithParams struct {
	BeforeCount int
	DoStepCount int
	AfterCount  int
	Data        int
}

func (step *FakeStepWithParams) Name() string {
	return "FakeStepWithParams"
}

func (step *FakeStepWithParams) Error() error {
	return nil
}

func (step *FakeStepWithParams) Before(input interface{}, params ...interface{}) (bool, error) {
	step.Data = 0
	if input != nil {
		if v, ok := input.(int); ok {
			step.Data += v
		}
	}
	for _, it := range params {
		if v, ok := it.(*int); ok {
			(*v)++
		}
	}
	step.BeforeCount++
	return true, nil
}

func (step *FakeStepWithParams) DoStep(input interface{}, params ...interface{}) (interface{}, error) {
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
	for _, it := range params {
		if v, ok := it.(*int); ok {
			(*v)++
		}
	}
	step.DoStepCount++
	return output, nil
}

func (step *FakeStepWithParams) After(input interface{}, params ...interface{}) (bool, error) {
	if input != nil {
		if v, ok := input.(int); ok {
			step.Data += v
		}
	}
	for _, it := range params {
		if v, ok := it.(*int); ok {
			(*v)++
		}
	}
	step.AfterCount++
	return true, nil
}
