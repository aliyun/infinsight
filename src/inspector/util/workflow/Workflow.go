/*
// =====================================================================================
//
//       Filename:  Workflow.go
//
//    Description:  简单任务流模块
//
//        Version:  1.0
//        Created:  06/11/2018 03:26:27 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package workflow

import (
	"errors"
	"time"

	"github.com/golang/glog"
)

// =====================================================================================
//         Type:  WorkStatus
//  Description:  状态信息（work与step共用状态，work看作是抽象step）
// =====================================================================================
type WorkStatus int

const WORKINIT WorkStatus = 0      // 工作流初始化阶段
const WORKREADY WorkStatus = 1     // 已经上了调度，准备执行
const WORKRUNNING WorkStatus = 2   // 工作流已经启动
const WORKFINISH WorkStatus = 3    // 工作流执行完成
const WORKERRFINISH WorkStatus = 4 // 工作流执行失败
const WORKUNKNOWN WorkStatus = 5   // 未知状态

type StepStatus int

const STEPWAIT StepStatus = 0      // 等待调度
const STEPREADY StepStatus = 1     // 上调度，准备执行Before()
const STEPRUNNING StepStatus = 2   // Before执行完成，开始执行任务
const STEPDONE StepStatus = 3      // 任务执行完成，准备执行After()
const STEPSKIP StepStatus = 4      // 任务跳过，直接进入下一步
const STEPERROR StepStatus = 5     // 在任何阶段执行失败，都会进入STEPERROR状态
const STEPFINISH StepStatus = 6    // 任务经过重试，最终完成
const STEPERRFINISH StepStatus = 7 // 任务经过重试，最终失败
const STEPUNKNOWN StepStatus = 8   // 未知状态

type Workflow struct {
	// workflow名字
	name string
	// 当前任务下标
	currentStepIndex int
	// workflow的step列表
	stepList []StepInterface
	// workflow每项任务的状态列表
	statList []StepStatus
	// workflow每项任务的耗时信息
	stepDuration []time.Duration
	// workflow自身状态信息
	stat WorkStatus
	// workflow整体耗时
	workflowDuration time.Duration
	// workflow超时报警（单位：s）
	workflowTimeout int
	// 带重试策略的step执行函数
	runWithRetryPolicy func(func() error) error
	// 存储相邻step之间的管道数据
	pipeData interface{}
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Init
//  Description:
// =====================================================================================
*/
func (wf *Workflow) Init(name string, retryPolicy string) error {
	wf.name = name
	wf.currentStepIndex = -1
	wf.stepList = make([]StepInterface, 0)
	wf.statList = make([]StepStatus, 0)
	wf.stepDuration = make([]time.Duration, 0)
	wf.stat = WORKINIT
	wf.workflowTimeout = 1

	switch retryPolicy {
	case "RunWithRetryOnce":
		wf.runWithRetryPolicy = wf.runWithRetryOnce
	default:
		wf.runWithRetryPolicy = wf.runWithNoRetry
	}
	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Reset
//  Description:
// =====================================================================================
*/
func (wf *Workflow) Reset() error {
	wf.currentStepIndex = -1
	wf.stat = WORKRUNNING
	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Name
//  Description:
// =====================================================================================
*/
func (wf *Workflow) Name() string {
	return wf.name
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  AddStep
//  Description:  追加step
// =====================================================================================
*/
func (wf *Workflow) AddStep(step StepInterface) error {
	wf.stepList = append(wf.stepList, step)
	wf.statList = append(wf.statList, STEPWAIT)
	wf.stepDuration = append(wf.stepDuration, 0)
	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Ready
//  Description:
// =====================================================================================
*/
func (wf *Workflow) Ready() error {
	if len(wf.stepList) == 0 {
		glog.Errorf("name[%s] no step in workflow", wf.name)
		return errors.New("no step in workflow")
	}
	wf.stat = WORKREADY
	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Start
//  Description:
// =====================================================================================
*/
func (wf *Workflow) Start() error {
	if len(wf.stepList) == 0 {
		glog.Errorf("name[%s] no step in workflow", wf.name)
		return errors.New("no step in workflow")
	}
	wf.stat = WORKRUNNING
	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  DoWorkflow
//  Description:  一次性执行全部workflow，只适用于所有step参数相同（或空参）的场景
// =====================================================================================
*/
func (wf *Workflow) DoWorkflow(params ...interface{}) error {
	var workflowTimeBegin = time.Now()
	var stepDurationList []time.Duration = make([]time.Duration, len(wf.stepList))
	for wf.HasNext() {
		wf.StepNext()
		var stepTimeBegin = time.Now()
		if err := wf.DoStep(params...); err != nil {
			return err
		}
		stepDurationList[wf.CurrentStep()] = time.Since(stepTimeBegin)
	}
	wf.workflowDuration = time.Since(workflowTimeBegin)
	if wf.workflowDuration > time.Duration(wf.workflowTimeout)*time.Second {
		for i, it := range stepDurationList {
			glog.Infof("workflow[%s] step[%d](%s) finish, time used[%v]",
				wf.name, i, wf.stepList[i].Name(), it)
		}
		glog.Infof("workflow[%s] allStepCount[%d] finish, time used[%v]",
			wf.name, len(wf.stepList), wf.workflowDuration)
	}
	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  DoStep
//  Description:
// =====================================================================================
*/
func (wf *Workflow) DoStep(params ...interface{}) error {
	// 参数校验
	if wf.stat == WORKFINISH || wf.stat == WORKERRFINISH {
		glog.Error("workflow has been finished")
		return errors.New("workflow has been finished")
	}
	if wf.currentStepIndex >= len(wf.stepList) {
		glog.Errorf("currentStepIndex[%d] is outof range[%d]", wf.currentStepIndex, len(wf.stepList))
		return errors.New("currentStepIndex is outof range")
	}

	// 创建step单次执行逻辑
	stepClosure := func() error {
		timeBegin := time.Now()

		wf.statList[wf.currentStepIndex] = STEPREADY

		var err error
		var goon bool
		var pipeData interface{}

		// do before
		if goon, err = wf.stepList[wf.currentStepIndex].Before(wf.pipeData, params...); err == nil {
			if goon {
				wf.statList[wf.currentStepIndex] = STEPRUNNING
			} else {
				wf.statList[wf.currentStepIndex] = STEPSKIP
				return nil
			}
		} else {
			wf.statList[wf.currentStepIndex] = STEPERROR
			return err
		}

		// do step
		if pipeData, err = wf.stepList[wf.currentStepIndex].DoStep(wf.pipeData, params...); err == nil {
			wf.statList[wf.currentStepIndex] = STEPDONE
		} else {
			wf.statList[wf.currentStepIndex] = STEPERROR
			return err
		}

		// do after
		if goon, err = wf.stepList[wf.currentStepIndex].After(wf.pipeData, params...); err == nil {
			if goon {
				wf.statList[wf.currentStepIndex] = STEPFINISH
			} else {
				wf.statList[wf.currentStepIndex] = STEPFINISH
				wf.stat = WORKFINISH
			}
		} else {
			wf.statList[wf.currentStepIndex] = STEPERROR
			return err
		}

		// 当step成功之后，管道信息替换成新step的输出
		wf.pipeData = pipeData

		wf.stepDuration[wf.currentStepIndex] = time.Since(timeBegin)
		return nil
	}

	// 将单次执行逻辑套在重试策略之上，带着重试策略执行
	if err := wf.runWithRetryPolicy(stepClosure); err != nil {
		wf.statList[wf.currentStepIndex] = STEPERRFINISH
		wf.stat = WORKERRFINISH
		return err
	}

	if wf.currentStepIndex == len(wf.stepList)-1 {
		wf.stat = WORKFINISH
	}

	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  HasNext
//  Description:
// =====================================================================================
*/
func (wf *Workflow) HasNext() bool {
	if wf.stat == WORKFINISH || wf.stat == WORKERRFINISH {
		return false
	}
	return wf.currentStepIndex < len(wf.stepList)-1
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  StepNext
//  Description:
// =====================================================================================
*/
func (wf *Workflow) StepNext() {
	wf.currentStepIndex++
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  WorkflowStat
//  Description:
// =====================================================================================
*/
func (wf *Workflow) WorkflowStat() WorkStatus {
	return wf.stat
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  CurrentStep
//  Description:
// =====================================================================================
*/
func (wf *Workflow) CurrentStep() int {
	return wf.currentStepIndex
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  CurrentStepStat
//  Description:
// =====================================================================================
*/
func (wf *Workflow) CurrentStepStat() StepStatus {
	if wf.currentStepIndex < 0 {
		return STEPUNKNOWN
	}
	return wf.statList[wf.currentStepIndex]
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  TimeDuration
//  Description:  workflow整体的耗时，及每个step的耗时
// =====================================================================================
*/
func (wf *Workflow) TimeDuration() (time.Duration, []time.Duration) {
	return wf.workflowDuration, wf.stepDuration[:wf.currentStepIndex+1]
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  LastStepStat
//  Description:
// =====================================================================================
*/
func (wf *Workflow) LastStepStat() StepStatus {
	if wf.currentStepIndex < 0 {
		return STEPUNKNOWN
	}
	if wf.currentStepIndex >= len(wf.stepList) {
		return wf.statList[len(wf.stepList)-1]
	} else {
		return wf.statList[wf.currentStepIndex]
	}
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  GetWorkflowDuration
//  Description:  获取上一个workflow的执行时间
// =====================================================================================
*/
//
func (wf *Workflow) GetWorkflowDuration() time.Duration {
	return wf.workflowDuration
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  SetTimeoutWarning
//  Description:  设置workflow超时报警
// =====================================================================================
*/
//
func (wf *Workflow) SetTimeoutWarning(n int) {
	wf.workflowTimeout = n
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  runWithNoRetry
//  Description:
// =====================================================================================
*/
func (wf *Workflow) runWithNoRetry(fun func() error) error {
	if err := fun(); err != nil {
		return err
	}
	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  runWithRetryOnce
//  Description:
// =====================================================================================
*/
func (wf *Workflow) runWithRetryOnce(fun func() error) error {
	if err := fun(); err != nil {
		if err = fun(); err != nil {
			return err
		}
	}
	return nil
}
