/*
// =====================================================================================
//
//       Filename:  TaskControlBlock.go
//
//    Description:  任务控制块，管理任务的全局信息，通过workflow交付各级流水线依次处理
//
//        Version:  1.0
//        Created:  06/11/2018 11:43:11 AM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package scheduler

import (
	"fmt"
	"sync"

	"inspector/util"
	. "inspector/util/workflow"
	"time"
)

type TCBStatus int

const READY TCBStatus = 0
const NOTREADY TCBStatus = 1
const RUNNING TCBStatus = 2
const UNKNOWN TCBStatus = 3

// =====================================================================================
//       Struct:  TCB
//  Description:  Task全局信息管理
// =====================================================================================
type TCB struct {
	// task name
	name string

	// meta信息只有在task发生变更时才会改变，任务周期性执行时不会变化
	// 这里的meta信息需要与config中读取到的数据保持一致
	meta map[string]string

	// data信息就是task持有的1-2分钟的暂存数据
	// 根据task类型的不同，data的数据格式也不同
	data interface{}

	// 注册的任务流
	workflow *Workflow

	// 删除标记
	isDeleted bool
	// 退出函数栈
	exitFuncStack *util.Stack

	// 防止两次重复调度导致的workflow执行错乱
	processLock sync.Mutex
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Init
//  Description:  当前TCB的名字，用于描述对哪个实例进行监控
// =====================================================================================
*/
// func (tcb *TCB) Init(name string, connector connector.Connector) error {
func (tcb *TCB) Init(name string) error {
	tcb.name = name
	tcb.workflow = new(Workflow)
	tcb.workflow.Init(fmt.Sprintf("%s's workflow", name), "default")
	tcb.isDeleted = false
	tcb.exitFuncStack = util.NewStack()
	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Name
//  Description:  当前TCB的名字，用于描述对哪个实例进行监控
// =====================================================================================
*/
func (tcb *TCB) Name() string {
	return tcb.name
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Meta
//  Description:  当前TCB的Meta信息
// =====================================================================================
*/
func (tcb *TCB) Meta() map[string]string {
	return tcb.meta
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Data
//  Description:  当前TCB的Data数据
// =====================================================================================
*/
func (tcb *TCB) Data() interface{} {
	return tcb.data
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  AddWorkflowStep
//  Description:  当前TCB的名字，用于描述对哪个实例进行监控
// =====================================================================================
*/
func (tcb *TCB) AddWorkflowStep(step StepInterface) error {
	return tcb.workflow.AddStep(step)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  GetWorkflowDuration
//  Description:  获取当前workflow的执行时间
// =====================================================================================
*/
//
func (tcb *TCB) GetWorkflowDuration() time.Duration {
	return tcb.workflow.GetWorkflowDuration()
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  SetWorkflowTimeoutWarning
//  Description:  设置Workflow超时报警
// =====================================================================================
*/
//
func (tcb *TCB) SetWorkflowTimeoutWarning(n int) {
	tcb.workflow.SetTimeoutWarning(n)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  setReady
//  Description:  检查当前tcb是否已经删除
// =====================================================================================
*/
func (tcb *TCB) setReady() error {
	return tcb.workflow.Ready()
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  markDel
//  Description:  标记删除当前TCB
// =====================================================================================
*/
func (tcb *TCB) markDel() {
	tcb.isDeleted = true
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  IsDeleted
//  Description:  检查当前tcb是否已经删除
// =====================================================================================
*/
func (tcb *TCB) IsDeleted() bool {
	return tcb.isDeleted
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  status
//  Description:  将workflow状态转化为TCB状态用于调度器调度
// =====================================================================================
*/
func (tcb *TCB) Status() TCBStatus {
	switch tcb.workflow.WorkflowStat() {
	case WORKINIT:
		return NOTREADY
	case WORKREADY:
		return READY
	case WORKRUNNING:
		switch tcb.workflow.CurrentStepStat() {
		case STEPWAIT:
			return READY
		case STEPREADY:
			return RUNNING
		case STEPRUNNING:
			return RUNNING
		case STEPDONE:
			return RUNNING
		case STEPSKIP:
			return RUNNING
		case STEPERROR:
			return RUNNING
		case STEPFINISH:
			return RUNNING
		case STEPERRFINISH:
			return RUNNING
		case STEPUNKNOWN:
			return UNKNOWN
		default:
			return UNKNOWN
		}
	case WORKFINISH:
		return READY
	case WORKERRFINISH:
		return READY
	case WORKUNKNOWN:
		return UNKNOWN
	default:
		return UNKNOWN
	}
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  process
//  Description:  当前待执行的task process，scheduler调度接口
// =====================================================================================
*/
func (tcb *TCB) Process() func() {
	return func() {
		tcb.processLock.Lock()
		defer tcb.processLock.Unlock()

		tcb.workflow.Reset()
		tcb.workflow.DoWorkflow(tcb)
	}
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  AtExit
//  Description:  注册退出时的析构函数
// =====================================================================================
*/
func (tcb *TCB) AtExit(atExit func()) {
	tcb.exitFuncStack.Push(atExit)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  DoExit
//  Description:  执行退出函数栈
// =====================================================================================
*/
func (tcb *TCB) DoExit() {
	tcb.processLock.Lock()
	defer tcb.processLock.Unlock()

	var fun interface{}
	for {
		if fun = tcb.exitFuncStack.Pop(); fun == nil {
			break
		}
		var f = fun.(func())
		f()
	}
}
