/*
// =====================================================================================
// 
//       Filename:  FakeStepError.cpp
// 
//    Description:  用于测试step失败的测试step
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

import "errors"

type FakeStepError struct {
	BeforeCount int
	DoStepCount int
	AfterCount int
}

func (step *FakeStepError) Name() string {
	return "FakeStepError"
}

func (step *FakeStepError) Error() error {
	return nil
}

func (step *FakeStepError) Before(input interface{}, params ...interface{}) (bool, error) {
	step.BeforeCount++
	return true, nil
}

func (step *FakeStepError) DoStep(input interface{}, params ...interface{}) (interface{}, error) {
	step.DoStepCount++
	return nil, errors.New("make error")
}

func (step *FakeStepError) After(input interface{}, params ...interface{}) (bool, error) {
	step.AfterCount++
	return true, nil
}

