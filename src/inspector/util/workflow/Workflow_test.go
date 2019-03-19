/*
// =====================================================================================
//
//       Filename:  workflow_test.go
//
//    Description:
//
//        Version:  1.0
//        Created:  06/11/2018 04:57:28 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package workflow

import "fmt"
import "time"
import "runtime"
import "testing"

import . "inspector/test"

func TestWorkflow(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var step *FakeStep = new(FakeStep)
	var wf *Workflow = new(Workflow)
	wf.Init("test", "default")
	wf.AddStep(step)
	wf.AddStep(step)
	wf.AddStep(step)
	check(wf.stat == WORKINIT, "test init")
	check(wf.currentStepIndex == -1, "test index")
	check(wf.CurrentStepStat() == STEPUNKNOWN, "test current step stat")
	check(wf.LastStepStat() == STEPUNKNOWN, "test last step stat")
	wf.Ready()
	check(wf.stat == WORKREADY, "test ready")
	wf.Start()
	check(wf.stat == WORKRUNNING, "test start")
	for i := 0; i < 3; i++ {
		check(wf.HasNext() == true, "test has next")
		wf.StepNext()
		check(wf.DoStep() == nil, "test has do step")
		check(step.BeforeCount == i+1, "test fake step")
		check(step.DoStepCount == i+1, "test fake step")
		check(step.AfterCount == i+1, "test fake step")
		check(step.Data == i*3, "test fake step")
		check(wf.currentStepIndex == i, "test index")
		if i == 2 {
			check(wf.WorkflowStat() == WORKFINISH, "test current work stat")
		} else {
			check(wf.WorkflowStat() == WORKRUNNING, "test current work stat")
		}
		check(wf.CurrentStepStat() == STEPFINISH, "test current step stat")
		check(wf.LastStepStat() == STEPFINISH, "test last step stat")
	}
	check(wf.HasNext() == false, "test has next")

	check(true, "test")
}

func TestWorkflowError(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var step *FakeStep = new(FakeStep)
	var steperror *FakeStepError = new(FakeStepError)
	var wf *Workflow = new(Workflow)
	wf.Init("test", "RunWithRetryOnce")
	wf.AddStep(step)
	wf.AddStep(steperror)
	wf.AddStep(step)
	check(wf.stat == WORKINIT, "test init")
	check(wf.currentStepIndex == -1, "test index")
	check(wf.CurrentStepStat() == STEPUNKNOWN, "test current step stat")
	check(wf.LastStepStat() == STEPUNKNOWN, "test last step stat")
	wf.Ready()
	check(wf.stat == WORKREADY, "test ready")
	wf.Start()
	check(wf.stat == WORKRUNNING, "test start")

	// step 1
	check(wf.HasNext() == true, "test has next")
	wf.StepNext()
	check(wf.DoStep() == nil, "test has do step")
	check(step.BeforeCount == 1, "test fake step")
	check(step.DoStepCount == 1, "test fake step")
	check(step.AfterCount == 1, "test fake step")
	check(wf.currentStepIndex == 0, "test index")
	check(wf.WorkflowStat() == WORKRUNNING, "test current work stat")
	check(wf.CurrentStepStat() == STEPFINISH, "test current step stat")
	check(wf.LastStepStat() == STEPFINISH, "test last step stat")

	// step 2
	check(wf.HasNext() == true, "test has next")
	wf.StepNext()
	check(wf.DoStep() != nil, "test has do step")
	check(steperror.BeforeCount == 2, "test fake step")
	check(steperror.DoStepCount == 2, "test fake step")
	check(steperror.AfterCount == 0, "test fake step")
	check(wf.currentStepIndex == 1, "test index")
	check(wf.WorkflowStat() == WORKERRFINISH, "test current work stat")
	check(wf.CurrentStepStat() == STEPERRFINISH, "test current step stat")
	check(wf.LastStepStat() == STEPERRFINISH, "test last step stat")

	check(wf.HasNext() == false, "test has next")

	check(true, "test")
}

func TestWorkflowSkip(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var step *FakeStep = new(FakeStep)
	var stepskip *FakeStepSkip = new(FakeStepSkip)
	var wf *Workflow = new(Workflow)
	wf.Init("test", "default")
	wf.AddStep(step)
	wf.AddStep(stepskip)
	wf.AddStep(step)
	check(wf.stat == WORKINIT, "test init")
	check(wf.currentStepIndex == -1, "test index")
	check(wf.CurrentStepStat() == STEPUNKNOWN, "test current step stat")
	check(wf.LastStepStat() == STEPUNKNOWN, "test last step stat")
	wf.Ready()
	check(wf.stat == WORKREADY, "test ready")
	wf.Start()

	// step 1
	check(wf.HasNext() == true, "test has next")
	wf.StepNext()
	check(wf.DoStep() == nil, "test has do step")
	check(step.BeforeCount == 1, "test fake step")
	check(step.DoStepCount == 1, "test fake step")
	check(step.AfterCount == 1, "test fake step")
	check(step.Data == 0, "test fake step")
	check(wf.currentStepIndex == 0, "test index")
	check(wf.WorkflowStat() == WORKRUNNING, "test current work stat")
	check(wf.CurrentStepStat() == STEPFINISH, "test current step stat")
	check(wf.LastStepStat() == STEPFINISH, "test last step stat")

	// step 2
	check(wf.HasNext() == true, "test has next")
	wf.StepNext()
	check(wf.DoStep() == nil, "test has do step")
	check(stepskip.BeforeCount == 1, "test fake step")
	check(stepskip.DoStepCount == 0, "test fake step")
	check(stepskip.AfterCount == 0, "test fake step")
	check(stepskip.Data == 1, "test fake step")
	check(wf.currentStepIndex == 1, "test index")
	check(wf.WorkflowStat() == WORKRUNNING, "test current work stat")
	check(wf.CurrentStepStat() == STEPSKIP, "test current step stat")
	check(wf.LastStepStat() == STEPSKIP, "test last step stat")

	// step 3
	check(wf.HasNext() == true, "test has next")
	wf.StepNext()
	check(wf.DoStep() == nil, "test has do step")
	check(step.BeforeCount == 2, "test fake step")
	check(step.DoStepCount == 2, "test fake step")
	check(step.AfterCount == 2, "test fake step")
	check(step.Data == 3, "test fake step")
	check(wf.currentStepIndex == 2, "test index")
	check(wf.WorkflowStat() == WORKFINISH, "test current work stat")
	check(wf.CurrentStepStat() == STEPFINISH, "test current step stat")
	check(wf.LastStepStat() == STEPFINISH, "test last step stat")

	check(true, "test")
}

func TestWorkflowFinish(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var step *FakeStep = new(FakeStep)
	var stepfinish *FakeStepFinish = new(FakeStepFinish)
	var wf *Workflow = new(Workflow)
	wf.Init("test", "default")
	wf.AddStep(step)
	wf.AddStep(stepfinish)
	wf.AddStep(step)
	check(wf.stat == WORKINIT, "test init")
	check(wf.currentStepIndex == -1, "test index")
	check(wf.CurrentStepStat() == STEPUNKNOWN, "test current step stat")
	check(wf.LastStepStat() == STEPUNKNOWN, "test last step stat")
	wf.Ready()
	check(wf.stat == WORKREADY, "test ready")
	wf.Start()

	// step 1
	check(wf.HasNext() == true, "test has next")
	wf.StepNext()
	check(wf.DoStep() == nil, "test has do step")
	check(step.BeforeCount == 1, "test fake step")
	check(step.DoStepCount == 1, "test fake step")
	check(step.AfterCount == 1, "test fake step")
	check(step.Data == 0, "test fake step")
	check(wf.currentStepIndex == 0, "test index")
	check(wf.WorkflowStat() == WORKRUNNING, "test current work stat")
	check(wf.CurrentStepStat() == STEPFINISH, "test current step stat")
	check(wf.LastStepStat() == STEPFINISH, "test last step stat")

	// step 2
	check(wf.HasNext() == true, "test has next")
	wf.StepNext()
	check(wf.DoStep() == nil, "test has do step")
	check(stepfinish.BeforeCount == 1, "test fake step")
	check(stepfinish.DoStepCount == 1, "test fake step")
	check(stepfinish.AfterCount == 1, "test fake step")
	check(stepfinish.Data == 3, "test fake step")
	check(wf.currentStepIndex == 1, "test index")
	check(wf.WorkflowStat() == WORKFINISH, "test current work stat")
	check(wf.CurrentStepStat() == STEPFINISH, "test current step stat")
	check(wf.LastStepStat() == STEPFINISH, "test last step stat")

	// step 3
	check(wf.HasNext() == false, "test has next")

	check(true, "test")
}

func TestWorkflowTimeDuration(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var step *FakeStep = new(FakeStep)
	var steperror *FakeStepError = new(FakeStepError)
	var wf *Workflow
	var wfDuration time.Duration
	var stepDurationList []time.Duration

	// normal case
	wf = new(Workflow)
	wf.Init("test", "default")
	wf.AddStep(step)
	wf.AddStep(step)
	wf.AddStep(step)
	wf.Ready()
	wf.Start()
	check(wf.DoWorkflow() == nil, "test do workflow")

	wfDuration, stepDurationList = wf.TimeDuration()
	check(len(stepDurationList) == 3, "test")
	fmt.Println(wfDuration)
	fmt.Println(stepDurationList)

	// error case
	wf = new(Workflow)
	wf.Init("test", "default")
	wf.AddStep(step)
	wf.AddStep(steperror)
	wf.AddStep(step)
	wf.Ready()
	wf.Start()
	check(wf.DoWorkflow() != nil, "test do workflow")

	wfDuration, stepDurationList = wf.TimeDuration()
	check(len(stepDurationList) == 2, "test")
	fmt.Println(wfDuration)
	fmt.Println(stepDurationList)

	check(true, "test")
}

func TestWorkflowWithParams(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var step *FakeStepWithParams = new(FakeStepWithParams)
	var wf *Workflow = new(Workflow)
	var i int = 0
	var p *int = &i
	wf.Init("test", "default")
	wf.AddStep(step)
	wf.AddStep(step)
	wf.AddStep(step)
	wf.AddStep(step)
	wf.AddStep(step)
	wf.Ready()
	wf.Start()
	wf.DoWorkflow(p)

	check(i == 15, "test")

	check(true, "test")
}

func BenchmarkWorkflow(b *testing.B) {
	b.ResetTimer()

	var step *FakeStep = new(FakeStep)
	var wf *Workflow = new(Workflow)
	wf.Init("test", "default")
	wf.AddStep(step)
	wf.AddStep(step)
	wf.AddStep(step)
	wf.AddStep(step)
	wf.AddStep(step)
	wf.Ready()
	wf.Start()
	for i := 0; i < b.N; i++ {
		wf.DoWorkflow()
		wf.Reset()
	}
}
