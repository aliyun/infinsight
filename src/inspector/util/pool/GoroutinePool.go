/*
// =====================================================================================
//
//       Filename:  GoroutinePool.go
//
//    Description:
//
//        Version:  1.0
//        Created:  06/11/2018 03:51:28 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package pool

import (
	"github.com/golang/glog"

	. "inspector/util"
)

const poolInitSize = 2048
const poolResRecoveryThreshold = 0.2

type GoroutinePool struct {
	// 当前pool中的routine总数
	totalSize int
	// 当前pool中active的routine总数
	activeSize int
	// 结束标记
	isClosed bool

	// 用于接收上层调用传递的控制信号
	cmdChannel chan Flag
	// 用于对上层返回的结束信号
	retChannel chan Flag
	// 用于接收上层调用传递的执行闭包
	funChannel chan func()

	// 用于向子routine发送控制信息
	innerCmdChannel chan Flag
	// 用于接收子routine的结束信号
	innerRetChannel chan Flag
	// 用于向子routine发送执行闭包
	innerFunChannel chan func()
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Init
//  Description:
// =====================================================================================
*/
func (gopool *GoroutinePool) Init() error {
	gopool.totalSize = 0
	gopool.activeSize = 0
	gopool.isClosed = false
	gopool.cmdChannel = make(chan Flag)
	gopool.retChannel = make(chan Flag, poolInitSize * 4)
	gopool.funChannel = make(chan func(), poolInitSize * 4)
	gopool.innerCmdChannel = make(chan Flag)
	gopool.innerRetChannel = make(chan Flag, poolInitSize * 4)
	gopool.innerFunChannel = make(chan func(), poolInitSize * 4)

	// 初始将pool的size设置为poolInitSize
	// 后续pool会根据压力自动增减pool size
	for i := 0; i < poolInitSize; i++ {
		gopool.createRoutine()
	}

	glog.Info("GoroutinePool Init")

	return nil
}

/*
// ===  function  ======================================================================
//         name:  Run
//  description:
// =====================================================================================
*/
func (gopool *GoroutinePool) Run() {
	glog.Info("GoroutinePool Run")

	// 当前协程退出时，向父协程发送信号
	defer func() {
		gopool.retChannel <- FLAGCHLD
	}()

	// main loop
	for {
		select {
		// 接收外部函数，并路由给没有任务的worker routine进行处理
		case fun, ok := <-gopool.funChannel:
			if ok == false {
				glog.Infof("GoroutinePool.funChannel has been closed")
				return
			}
			// 当活动协程全部为活动协程时，扩大pool容量
			if gopool.activeSize >= gopool.totalSize {
				// 经过实测，连续创建routine的时间约为2.5ms/千个
				// 为了不让fun执行不致于等太久，每次增量创建100个
				for i := 0; i < 100; i++ {
					gopool.createRoutine()
				}
				glog.Infof("GoroutinePool is almost full[%d / %d], create 100 more", gopool.activeSize, gopool.totalSize)
			}
			// 将闭包转发给worker routine
			gopool.innerFunChannel <- fun
			gopool.activeSize++

		// 接收外部信号，目前只处理FLAGTERM结束信号
		case flag, ok := <-gopool.cmdChannel:
			if ok == false {
				glog.Infof("GoroutinePool.cmdChannel has been closed")
				return
			}
			// 收到结束信号，销毁所有routine，并结束
			switch flag {
			case FLAGTERM:
				gopool.isClosed = true
				close(gopool.innerFunChannel)
				close(gopool.innerCmdChannel)
				glog.Info("recv FLAGTERM at GoroutinePool")
			default:
				glog.Errorf("unknown flag[%d], terminal GoroutinePool", flag)
				return
			}

		// 接收子routine执行完毕的返回信号
		case flag := <-gopool.innerRetChannel:
			switch flag {
			case FLAGFINISH:
				gopool.activeSize--
				// 如果活动协程低于资源回收阈值，且协程总数没有低于初始值，则进行资源回收
				if gopool.isClosed == false && gopool.totalSize > poolInitSize &&
					gopool.activeSize < int(float32(gopool.totalSize)*poolResRecoveryThreshold) {
					gopool.innerCmdChannel <- FLAGTERM
				}
			case FLAGCHLD:
				gopool.totalSize--
				if gopool.totalSize == 0 {
					close(gopool.innerRetChannel)
					glog.Info("GoroutinePool quit")
					return
				}
			default:
				glog.Errorf("unknown flag[%d], routineLoop ret", flag)
				return
			}
		}
	}
}

/*
// ===  function  ======================================================================
//         name:  WaitRet
//  description:
// =====================================================================================
*/
func (gopool *GoroutinePool) WaitRet() Flag {
	return <-gopool.retChannel
}

/*
// ===  function  ======================================================================
//         name:  RetChan
//  description:
// =====================================================================================
*/
func (gopool *GoroutinePool) RetChan() chan Flag {
	return gopool.retChannel
}

/*
// ===  function  ======================================================================
//         name:  AsyncRun
//  description:
// =====================================================================================
*/
func (gopool *GoroutinePool) AsyncRun(fun func()) {
	gopool.funChannel <- fun
}

/*
// ===  function  ======================================================================
//         name:  SendCommand
//  description:
// =====================================================================================
*/
func (gopool *GoroutinePool) SendCommand(flag Flag) {
	gopool.cmdChannel <- flag
}

/*
// ===  function  ======================================================================
//         name:  Stop
//  description:
// =====================================================================================
*/
func (gopool *GoroutinePool) Stop() {
	gopool.cmdChannel <- FLAGTERM
	if gopool.WaitRet() == FLAGCHLD {
		gopool.Close()
	}
}

/*
// ===  function  ======================================================================
//         name:  Close
//  description:
// =====================================================================================
*/
func (gopool *GoroutinePool) Close() {
	close(gopool.cmdChannel)
	close(gopool.funChannel)
	close(gopool.retChannel)
}

/*
// ===  function  ======================================================================
//         name:  routineLoop
//  description:
// =====================================================================================
*/
func (gopool *GoroutinePool) routineLoop() {
	defer func() {
		gopool.innerRetChannel <- FLAGCHLD
	}()
	for {
		select {
		case flag, ok := <-gopool.innerCmdChannel:
			if ok == false {
				return
			}
			switch flag {
			case FLAGTERM:
				return

			default:
				glog.Errorf("unknown flag[%d], terminal routineLoop", flag)
				return
			}

		case fun, ok := <-gopool.innerFunChannel:
			if ok == false {
				return
			}
			fun()
			gopool.innerRetChannel <- FLAGFINISH
		}
	}
}

/*
// ===  function  ======================================================================
//         name:  createRoutine
//  description:
// =====================================================================================
*/
func (gopool *GoroutinePool) createRoutine() {
	go gopool.routineLoop()
	gopool.totalSize++
}
