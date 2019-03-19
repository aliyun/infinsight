/*
// =====================================================================================
//
//       Filename:  Scheduler_test.go
//
//    Description:
//
//        Version:  1.0
//        Created:  06/12/2018 05:08:10 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package scheduler

import "fmt"
import "time"
import "unsafe"
import "runtime"
import "testing"

import . "inspector/util"
import . "inspector/test"

func TestDoSchedule(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var skd *Scheduler = new(Scheduler)
	skd.Init("test Scheduler", 100)
	go skd.pool.Run()

	skd.tcbList = make([]*TCB, 100)
	var stepList []*FakeStep = make([]*FakeStep, 100)
	for i := 0; i < 100; i++ {
		skd.tcbList[i] = new(TCB)
		skd.tcbList[i].Init(fmt.Sprintf("%d", i))
		stepList[i] = new(FakeStep)
		skd.tcbList[i].AddWorkflowStep(stepList[i])
		skd.tcbList[i].setReady()
	}
	do := skd.doScheduleClosure()
	for i := 0; i < 10; i++ {
		check(stepList[10*i].DoStepCount == 0, "test step")
		do()
		time.Sleep(10 * time.Millisecond)
		check(stepList[10*i].DoStepCount == 1, "test step")
		check(stepList[10*i+9].DoStepCount == 1, "test step")
	}

	check(true, "test")
}

func TestDoScheduleByTimeClosure(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var skd *Scheduler = new(Scheduler)
	skd.Init("test Scheduler", 100)
	go skd.pool.Run()
	do := skd.doScheduleByTimeClosure()

	// case 1
	skd.tcbList = make([]*TCB, 100)
	var stepList []*FakeStep = make([]*FakeStep, 100)
	for i := 0; i < 100; i++ {
		skd.tcbList[i] = new(TCB)
		skd.tcbList[i].Init(fmt.Sprintf("%d", i))
		stepList[i] = new(FakeStep)
		skd.tcbList[i].AddWorkflowStep(stepList[i])
		skd.tcbList[i].setReady()
	}
	for i := 0; i < 100; i++ {
		check(stepList[i].DoStepCount == 0, "test step")
		do(1)
		time.Sleep(5 * time.Millisecond)
		check(stepList[i].DoStepCount == 1, "test step")
	}

	// case 2
	skd.tcbList = make([]*TCB, 101)
	stepList = make([]*FakeStep, 101)
	for i := 0; i < 101; i++ {
		skd.tcbList[i] = new(TCB)
		skd.tcbList[i].Init(fmt.Sprintf("%d", i))
		stepList[i] = new(FakeStep)
		skd.tcbList[i].AddWorkflowStep(stepList[i])
		skd.tcbList[i].setReady()
	}
	for i := 0; i < 100; i++ {
		check(stepList[i].DoStepCount == 0, "test step")
		do(1)
		time.Sleep(5 * time.Millisecond)
		check(stepList[i].DoStepCount == 1, "test step")
	}
	check(stepList[100].DoStepCount == 1, "test step")

	// case 3
	skd.tcbList = make([]*TCB, 101)
	stepList = make([]*FakeStep, 101)
	for i := 0; i < 101; i++ {
		skd.tcbList[i] = new(TCB)
		skd.tcbList[i].Init(fmt.Sprintf("%d", i))
		stepList[i] = new(FakeStep)
		skd.tcbList[i].AddWorkflowStep(stepList[i])
		skd.tcbList[i].setReady()
	}

	for i := 0; i < 50; i++ {
		check(stepList[i].DoStepCount == 0, "test step")
	}
	do(50)
	time.Sleep(5 * time.Millisecond)
	for i := 0; i < 50; i++ {
		check(stepList[i].DoStepCount == 1, "test step")
	}

	for i := 0; i < 50; i++ {
		check(stepList[i+50].DoStepCount == 0, "test step")
	}
	check(stepList[100].DoStepCount == 0, "test step")
	for i := 0; i < 20; i++ {
		check(stepList[i].DoStepCount == 1, "test step")
	}
	do(70)
	time.Sleep(5 * time.Millisecond)
	for i := 0; i < 50; i++ {
		check(stepList[i+50].DoStepCount == 1, "test step")
	}
	check(stepList[100].DoStepCount == 1, "test step")
	for i := 0; i < 20; i++ {
		check(stepList[i].DoStepCount == 2, "test step")
	}

	// case 4
	skd.tcbList = make([]*TCB, 101)
	stepList = make([]*FakeStep, 101)
	for i := 0; i < 101; i++ {
		skd.tcbList[i] = new(TCB)
		skd.tcbList[i].Init(fmt.Sprintf("%d", i))
		stepList[i] = new(FakeStep)
		skd.tcbList[i].AddWorkflowStep(stepList[i])
		skd.tcbList[i].setReady()
	}
	for i := 0; i < 100; i++ {
		check(stepList[i].DoStepCount == 0, "test step")
	}
	do(200)
	time.Sleep(5 * time.Millisecond)
	for i := 0; i < 100; i++ {
		check(stepList[i].DoStepCount == 2, "test step")
	}
	check(stepList[100].DoStepCount == 2, "test step")

	check(true, "test")
}

func TestDoSchedule101TCB(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var skd *Scheduler = new(Scheduler)
	skd.Init("test Scheduler", 100)
	go skd.pool.Run()

	skd.tcbList = make([]*TCB, 101)
	var stepList []*FakeStep = make([]*FakeStep, 101)
	for i := 0; i < 101; i++ {
		skd.tcbList[i] = new(TCB)
		skd.tcbList[i].Init(fmt.Sprintf("%d", i))
		stepList[i] = new(FakeStep)
		skd.tcbList[i].AddWorkflowStep(stepList[i])
		skd.tcbList[i].setReady()
	}
	do := skd.doScheduleClosure()
	do()
	time.Sleep(10 * time.Millisecond)
	check(stepList[0].DoStepCount == 1, "test step")
	check(stepList[9].DoStepCount == 1, "test step")
	check(stepList[10].DoStepCount == 1, "test step")
	for i := 0; i < 8; i++ {
		do()
	}
	time.Sleep(1 * time.Millisecond)
	check(stepList[98].DoStepCount == 1, "test step")
	check(stepList[99].DoStepCount == 0, "test step")
	do()
	time.Sleep(1 * time.Millisecond)
	check(stepList[99].DoStepCount == 1, "test step")
	check(stepList[100].DoStepCount == 1, "test step")

	check(true, "test")
}

func TestAddAndDel(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var skd *Scheduler = new(Scheduler)
	skd.Init("test Scheduler", 100)
	go skd.Run()

	var tcbList []*TCB = make([]*TCB, 100)
	for i := 0; i < 100; i++ {
		tcbList[i] = new(TCB)
		tcbList[i].Init(fmt.Sprintf("%d", i))
		tcbList[i].AddWorkflowStep(new(FakeStep))
	}
	skd.AddTCB(tcbList)
	time.Sleep(5 * time.Millisecond)
	check(len(skd.freeList) == 0, "freeList")
	check(len(skd.tcbList) == 100, "test skd tcb len")

	// 先调度个2周期，跨过13号，这样后面继续扫描会优先扫到50号
	time.Sleep(20 * time.Millisecond)

	skd.DelTCB([]*TCB{tcbList[50], tcbList[13], tcbList[76]})
	check(tcbList[50].IsDeleted() == true, "test tcb")
	check(tcbList[13].IsDeleted() == true, "test tcb")
	check(tcbList[76].IsDeleted() == true, "test tcb")
	check(len(skd.freeList) == 0, "freeList")

	var tcb *TCB
	// 由于调度是顺序的，所以无论删除顺序如何，都按照调度顺序进行
	time.Sleep(40 * time.Millisecond)
	check(skd.freeList[0] == 50, "freeList")
	check(len(skd.freeList) == 1, "freeList")
	// 至此正好调度了一轮，13号没有赶上调度，所以没有被回收
	fmt.Println("test begin")
	time.Sleep(40 * time.Millisecond)
	check(skd.freeList[1] == 76, "freeList")
	check(len(skd.freeList) == 2, "freeList")
	fmt.Println("test end")
	// 增加一项，测试空闲资源重复利用
	tcb = new(TCB)
	tcb.Init(fmt.Sprintf("new1"))
	tcb.AddWorkflowStep(new(FakeStep))
	skd.AddTCB([]*TCB{tcb})
	time.Sleep(1 * time.Millisecond)
	check(len(skd.freeList) == 1, "freeList")

	// 再调度个2周期，回收13号
	time.Sleep(20 * time.Millisecond)
	check(skd.freeList[1] == 13, "freeList")
	check(len(skd.freeList) == 2, "freeList")

	// 增加一项，测试空闲资源重复利用
	tcb = new(TCB)
	tcb.Init(fmt.Sprintf("new2"))
	tcb.AddWorkflowStep(new(FakeStep))
	skd.AddTCB([]*TCB{tcb})
	time.Sleep(1 * time.Millisecond)
	check(len(skd.freeList) == 1, "freeList")

	// 增加一项，测试空闲资源重复利用
	tcb = new(TCB)
	tcb.Init(fmt.Sprintf("new3"))
	tcb.AddWorkflowStep(new(FakeStep))
	skd.AddTCB([]*TCB{tcb})
	time.Sleep(1 * time.Millisecond)
	check(len(skd.freeList) == 0, "freeList")

	// 验证插入的正确性
	check(skd.tcbList[76].name == "new1", "test")
	check(skd.tcbList[13].name == "new2", "test")
	check(skd.tcbList[50].name == "new3", "test")

	skd.SendCommand(FLAGTERM)
	skd.WaitRet()
	check(true, "test")
}

func TestAddAndDelUnsafe(t *testing.T) {
	// 针对复用调度队列空闲位置的方法记录删除信息，考虑到有风险，所以暂不使这种方法
	// 但是单元测试可以通过，测试代码暂时保留
	return
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var skd *Scheduler = new(Scheduler)
	skd.Init("test Scheduler", 100)
	go skd.Run()

	var tcbList []*TCB = make([]*TCB, 100)
	for i := 0; i < 100; i++ {
		tcbList[i] = new(TCB)
		tcbList[i].Init(fmt.Sprintf("%d", i))
		tcbList[i].AddWorkflowStep(new(FakeStep))
	}
	skd.AddTCB(tcbList)
	check(skd.freeIndex == -1, "freeIndex")
	time.Sleep(1 * time.Millisecond)
	check(len(skd.tcbList) == 100, "test skd tcb len")

	// 先调度个2周期，跨过13号，这样后面继续扫描会优先扫到50号
	time.Sleep(20 * time.Millisecond)

	skd.DelTCB([]*TCB{tcbList[50], tcbList[13], tcbList[76]})
	check(tcbList[50].IsDeleted() == true, "test tcb")
	check(tcbList[13].IsDeleted() == true, "test tcb")
	check(tcbList[76].IsDeleted() == true, "test tcb")
	check(skd.freeIndex == -1, "freeIndex")

	var tcb *TCB
	// 由于调度是顺序的，所以无论删除顺序如何，都按照调度顺序进行
	time.Sleep(40 * time.Millisecond)
	check(skd.freeIndex == 50, "freeIndex")
	check(int(*(*int64)(unsafe.Pointer(&skd.tcbList[50]))) == -1, "freeIndex")
	// 至此正好调度了一轮，13号没有赶上调度，所以没有被回收
	time.Sleep(40 * time.Millisecond)
	check(skd.freeIndex == 76, "freeIndex")
	check(int(*(*int64)(unsafe.Pointer(&skd.tcbList[76]))) == 50, "freeIndex")
	// 增加一项，测试空闲资源重复利用
	tcb = new(TCB)
	tcb.Init(fmt.Sprintf("new1"))
	tcb.AddWorkflowStep(new(FakeStep))
	skd.AddTCB([]*TCB{tcb})
	time.Sleep(1 * time.Millisecond)
	check(skd.freeIndex == 50, "freeIndex")

	// 再调度个2周期，回收13号
	time.Sleep(20 * time.Millisecond)
	check(skd.freeIndex == 13, "freeIndex")
	check(int(*(*int64)(unsafe.Pointer(&skd.tcbList[13]))) == 50, "freeIndex")

	// 增加一项，测试空闲资源重复利用
	tcb = new(TCB)
	tcb.Init(fmt.Sprintf("new2"))
	tcb.AddWorkflowStep(new(FakeStep))
	skd.AddTCB([]*TCB{tcb})
	time.Sleep(1 * time.Millisecond)
	check(skd.freeIndex == 50, "freeIndex")

	// 增加一项，测试空闲资源重复利用
	tcb = new(TCB)
	tcb.Init(fmt.Sprintf("new3"))
	tcb.AddWorkflowStep(new(FakeStep))
	skd.AddTCB([]*TCB{tcb})
	time.Sleep(1 * time.Millisecond)
	check(skd.freeIndex == -1, "freeIndex")

	// 验证插入的正确性
	check(skd.tcbList[76].name == "new1", "test")
	check(skd.tcbList[13].name == "new2", "test")
	check(skd.tcbList[50].name == "new3", "test")

	skd.SendCommand(FLAGTERM)
	skd.WaitRet()
	check(true, "test")
}

func BenchmarkScheduler(b *testing.B) {
	b.ResetTimer()

	var skd *Scheduler = new(Scheduler)
	skd.Init("test Scheduler", 1000)
	go skd.pool.Run()

	skd.tcbList = make([]*TCB, 10000)
	for i := 0; i < 10000; i++ {
		skd.tcbList[i] = new(TCB)
		skd.tcbList[i].Init(fmt.Sprintf("%d", i))
		for j := 0; j < 10; j++ {
			skd.tcbList[i].AddWorkflowStep(new(FakeStep))
		}
		skd.tcbList[i].setReady()
	}
	do := skd.doScheduleClosure()
	for i := 0; i < b.N; i++ {
		do()
	}
}
