/*
// =====================================================================================
//
//       Filename:  Scheduler.go
//
//    Description:  专门针对TCB结构进行定期重复调度，与TCB结构强耦合
//
//        Version:  1.0
//        Created:  06/11/2018 03:28:21 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package scheduler

import (
	"bytes"
	"errors"
	"sync/atomic"
	"time"

	"github.com/golang/glog"

	. "inspector/util"

	. "inspector/util/pool"
	"runtime"
)

// 调度器调度间隔，单位：毫秒
const schedulingInterval int = 10
const schedulingTickerChanLen int = 1000

const (
	SKD_UNDEFINED = iota
	SKD_INIT
	SKD_RUNNING
)

type Scheduler struct {
	// 调度服务的名字
	name string
	// 任务重复间隔，单位：毫秒
	interval int
	// 等待CollectorManager发送命令的管道
	cmdChannel chan Flag
	// 给CollectorManager返回结束信号的管道
	retChannel chan Flag
	// 等待CollectorManager发送新TCB的管道
	tcbChannel chan *TCB
	// 记录ticker次数的管道，用于解决time ticker管道长度为0，导致时间错后的问题
	tickChannel chan int64

	// 协程池
	pool *GoroutinePool

	// 调度列表
	tcbList []*TCB
	// 空闲队列，将freeList当作栈使用，记录调度队列的空闲下标
	freeList []int
	// 当前为空可复用的tcbList下标
	// 考虑到强行破坏指针有可能造成gc的异常，所以虽然单元测试可以通过，但还是不使用这种方法
	freeIndex int

	// 状态字，防止重复启动
	stat int32
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Init
//  Description:  name: 调度器的名字
//                interval: 调度周期(ms)，调度周期必须大于100ms
// =====================================================================================
*/
func (skd *Scheduler) Init(name string, interval int) error {
	if len(name) == 0 {
		return errors.New("scheduler name is not allow a empey string")
	}

	if interval < 100 {
		return errors.New("scheduler interval must be greater than 100(ms)")
	}

	skd.name = name
	skd.interval = interval
	skd.cmdChannel = make(chan Flag)
	skd.retChannel = make(chan Flag)
	skd.tcbChannel = make(chan *TCB)
	skd.tickChannel = make(chan int64, schedulingTickerChanLen)

	// 初始化协程池
	skd.pool = new(GoroutinePool)
	skd.pool.Init()

	// 初始化调度列表
	skd.tcbList = make([]*TCB, 0)
	skd.freeList = make([]int, 0)
	glog.Infof("Scheduler[%s] init", name)

	// 暂停使用freeIndex方式，代码保留
	// skd.freeIndex = -1

	atomic.StoreInt32(&skd.stat, SKD_INIT)
	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  doScheduleByTimeClosure
//  Description:  给定ticker时间(ms)进行调度
// =====================================================================================
*/
func (skd *Scheduler) doScheduleByTimeClosure() func(int) {
	var index int = 0
	var length int = 0
	var msPoint int = 0
	var oneCount int = 0  // 每ms调度的数量
	var remainder int = 0 // 每秒调度无法整除的剩余数量

	return func(tickDuration int) {
		// 将这些任务顺序交由GoroutinePool执行
		for tickDuration > 0 {
			// 当msPoint点数归0时，初始化下一次调度周期的基础数值
			if msPoint == 0 {
				index = 0
				length = len(skd.tcbList) // 每次调度不收addTCB影响，只调度确定长度
				oneCount = length / skd.interval
				remainder = length % skd.interval
			}

			// 根据是否完成一个周期的调用进行区分
			var count = 0
			if msPoint+tickDuration < skd.interval { // 没有超过一次调度周期
				count = tickDuration * oneCount
				msPoint += tickDuration
				tickDuration = 0
			} else { // 一个调度周期已经结束
				var last = skd.interval - msPoint
				count = last*oneCount + remainder
				msPoint = 0
				tickDuration -= last
			}

			for j := 0; j < count; j++ {
				var tcb = skd.tcbList[index]

				if tcb == nil {
					index++
					continue
				}
				if tcb.IsDeleted() {
					if tcb.Status() == RUNNING {
						index++
						continue // skip
					} else {
						tcb.DoExit()
						tcb = nil
						skd.tcbList[index] = nil
						skd.freeList = append(skd.freeList, index)
						index++
						continue
					}
				}
				if stat := tcb.Status(); stat == READY {
					skd.pool.AsyncRun(tcb.Process())
				} else {
					//glog.Warningf("tcb[%s] can't be scheduled, workflow status[%d], step[%d], step status[%d]",
					//	tcb.Name(), tcb.workflow.WorkflowStat(), tcb.workflow.CurrentStep(), tcb.workflow.CurrentStepStat())
				}
				index++
			}
		}
	}
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  doScheduleClosure
//  Description:  按照10ms为ticker粒度进行调度
// =====================================================================================
*/
func (skd *Scheduler) doScheduleClosure() func() {
	var indexOfScheduleBucket int = 0
	var countOfScheduleBucket int = 0
	var indexOfTCBList int = 0
	var oneTickTaskCount int = 0
	return func() {
		// 计算当前调度周期需要调度多少个任务
		if indexOfScheduleBucket == 0 { // 每次轮转完一圈再重新计算队列长度，即新任务有可能需要等到下一个interval
			countOfScheduleBucket = skd.interval / schedulingInterval
			indexOfTCBList = 0
			oneTickTaskCount = len(skd.tcbList) / countOfScheduleBucket
			if len(skd.tcbList)%countOfScheduleBucket != 0 {
				oneTickTaskCount++
			}
		}

		// 将这些任务顺序交由GoroutinePool执行
		var left int = indexOfTCBList
		var right int = indexOfTCBList + oneTickTaskCount
		if right > len(skd.tcbList) {
			right = len(skd.tcbList)
		}
		for i, tcb := range skd.tcbList[left:right] {
			if tcb == nil {
				continue
			}
			if tcb.IsDeleted() {
				if tcb.Status() == RUNNING {
					continue // skip
				} else {
					tcb = nil
					skd.tcbList[left+i] = nil

					// 暂停使用freeIndex方式，代码保留
					// *(*int64)(unsafe.Pointer(&skd.tcbList[left + i])) = int64(skd.freeIndex)
					// skd.freeIndex = left + i
					skd.freeList = append(skd.freeList, left+i)
					continue
				}
			}
			if stat := tcb.Status(); stat == READY {
				skd.pool.AsyncRun(tcb.Process())
			} else {
				//glog.Warningf("tcb[%s] can't be scheduled, workflow status[%d], step[%d], step status[%d]",
				//	tcb.Name(), tcb.workflow.WorkflowStat(), tcb.workflow.CurrentStep(), tcb.workflow.CurrentStepStat())
			}
		}
		indexOfScheduleBucket++
		indexOfScheduleBucket %= countOfScheduleBucket
		indexOfTCBList = right
	}
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  ticker
//  Description:
// =====================================================================================
*/
func (skd *Scheduler) ticker() {
	runtime.LockOSThread()

	var tick = time.NewTicker(time.Duration(schedulingInterval) * time.Millisecond)

	var start = time.Now()
	var preNS int64 = 0
	// main loop
	for i := 0; ; i++ {
		select {
		case <-tick.C:
			var duration = time.Since(start)
			var ns = duration.Nanoseconds()
			skd.tickChannel <- ns - preNS
			preNS = ns
		}
	}
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Run
//  Description:
// =====================================================================================
*/
func (skd *Scheduler) Run() {
	if ok := atomic.CompareAndSwapInt32(&skd.stat, SKD_INIT, SKD_RUNNING); !ok {
		glog.Errorf("Scheduler[%s] is running", skd.name)
		return
	}
	glog.Infof("Scheduler[%s] starting to run", skd.name)

	runtime.LockOSThread()

	// 启动协程池
	go skd.pool.Run()

	// 启动ticker
	go skd.ticker()

	// 当前协程退出时，向父协程发送信号
	defer func() {
		skd.retChannel <- FLAGCHLD
	}()

	// var doSchedule = skd.doScheduleClosure()
	var doSchedule = skd.doScheduleByTimeClosure()

	// main loop
	var remain int64 = 0
	for {
		select {
		case ns := <-skd.tickChannel:
			ns = ns + remain
			var ms = int(ns / (1000 * 1000))
			remain = ns % (1000 * 1000)
			doSchedule(ms)

		case tcb := <-skd.tcbChannel:
			// 暂停使用freeIndex方式，代码保留
			// if skd.freeIndex == -1 {
			// 	// 如果没有可复用的位置，则追加
			// 	skd.tcbList = append(skd.tcbList, tcb)
			// } else {
			// 	// 如果有可复用的位置，取出“链表头”进行复用
			// 	var index int = skd.freeIndex
			// 	skd.freeIndex = int(*(*int64)(unsafe.Pointer(&skd.tcbList[index])))
			// 	skd.tcbList[index] = tcb
			// }
			if len(skd.freeList) == 0 {
				// 如果没有可复用的位置，则追加
				skd.tcbList = append(skd.tcbList, tcb)
			} else {
				// 如果有可复用的位置，取出空闲下标并弹栈
				index := skd.freeList[len(skd.freeList)-1]
				skd.tcbList[index] = tcb
				skd.freeList = skd.freeList[:len(skd.freeList)-1]
			}

		case flag := <-skd.cmdChannel:
			switch flag {
			case FLAGTERM:
				// 将FLAGTERM转发给GoroutinePool
				glog.Infof("recv FLAGTERM at Scheduler[%s]", skd.name)
				skd.pool.SendCommand(FLAGTERM)
			default:
				glog.Errorf("unknown flag[%d], terminal Scheduler", flag)
				return
			}

		case flag := <-skd.pool.RetChan():
			switch flag {
			case FLAGCHLD:
				skd.pool.Close()
				glog.Infof("Scheduler[%s] recv GoroutinePool quit", skd.name)
				return
			default:
				glog.Errorf("unknown flag[%d], GoroutinePool ret", flag)
				return
			}
		}
	}
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  AddTCB
//  Description:  新增已有进程控制块，将其下调度，不对task进行去重
// =====================================================================================
*/
func (skd *Scheduler) AddTCB(tcbList []*TCB) {
	// record log
	var tcbNameListBuf bytes.Buffer
	for _, tcb := range tcbList {
		skd.tcbChannel <- tcb
		tcb.setReady()
		tcbNameListBuf.WriteString(tcb.Name())
		tcbNameListBuf.WriteString(";")
	}
	glog.Infof("Scheduler[%s] add TCB[%s]", skd.name, tcbNameListBuf.String())
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  DelTCB
//  Description:  删除已有进程控制块，将其下调度
// =====================================================================================
*/
func (skd *Scheduler) DelTCB(tcbList []*TCB) {
	var tcbNameListBuf bytes.Buffer
	for _, tcb := range tcbList {
		// 删除TCB过程只是置删除标记，真正的资源回收在调度中进行
		// markDel原子打标记，不必加锁
		tcb.markDel()

		tcbNameListBuf.WriteString(tcb.Name())
		tcbNameListBuf.WriteString(";")
	}
	glog.Infof("Scheduler[%s] del TCB[%s]", skd.name, tcbNameListBuf.String())
}

/*
// ===  function  ======================================================================
//         name:  SendCommand
//  description:
// =====================================================================================
*/
func (skd *Scheduler) SendCommand(flag Flag) {
	skd.cmdChannel <- FLAGTERM
}

/*
// ===  function  ======================================================================
//         name:  WaitRet
//  description:
// =====================================================================================
*/
func (skd *Scheduler) WaitRet() Flag {
	return <-skd.retChannel
}

/*
// ===  function  ======================================================================
//         name:  RetChan
//  description:
// =====================================================================================
*/
func (skd *Scheduler) RetChan() chan Flag {
	return skd.retChannel
}
