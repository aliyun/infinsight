/*
// =====================================================================================
//
//       Filename:  GoroutinePool_test.go
//
//    Description:
//
//        Version:  1.0
//        Created:  06/11/2018 05:47:22 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package pool

import "fmt"
import "time"
import "runtime"
import "testing"

import . "inspector/util"

func TestInit(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var gopool *GoroutinePool = new(GoroutinePool)
	gopool.Init()
	check(gopool.totalSize == 1000, "test Init pool total size")
	check(gopool.activeSize == 0, "test Init pool active size")
}

func TestAddFun(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var gopool *GoroutinePool = new(GoroutinePool)
	gopool.Init()
	go gopool.Run()
	var tc chan int = make(chan int)
	fun := func() { <-tc }

	for i := 0; i < 1000; i++ {
		gopool.AsyncRun(fun)
	}
	// sleep一下，确保协程全部启动完成
	time.Sleep(10 * time.Millisecond)
	check(gopool.totalSize == 1000, "test pool total size")
	check(gopool.activeSize == 1000, "test pool active size")

	for i := 0; i < 100; i++ {
		gopool.AsyncRun(fun)
	}
	// sleep一下，确保协程全部启动完成
	time.Sleep(1 * time.Millisecond)
	check(gopool.totalSize == 1100, "test pool total size")
	check(gopool.activeSize == 1100, "test pool active size")
}

func TestFunReturn(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var gopool *GoroutinePool = new(GoroutinePool)
	gopool.Init()
	go gopool.Run()
	var tc chan int = make(chan int)
	fun := func() { <-tc }

	for i := 0; i < 2000; i++ {
		gopool.AsyncRun(fun)
	}
	// sleep一下，确保协程全部启动完成
	time.Sleep(10 * time.Millisecond)

	for i := 0; i < 1000; i++ {
		tc <- 0
	}
	// sleep一下，确保协程执行完毕
	time.Sleep(10 * time.Millisecond)
	check(gopool.totalSize == 2000, "test pool total size")
	check(gopool.activeSize == 1000, "test pool active size")

	for i := 0; i < 1000; i++ {
		tc <- 0
	}
	// sleep一下，确保协程执行完毕
	time.Sleep(10 * time.Millisecond)
	check(gopool.totalSize == 1600, "test pool total size")
	check(gopool.activeSize == 0, "test pool active size")
}

func TestFunTerm(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var gopool *GoroutinePool = new(GoroutinePool)
	gopool.Init()
	go gopool.Run()
	var tc chan int = make(chan int)
	fun := func() { <-tc }

	for i := 0; i < 10000; i++ {
		gopool.AsyncRun(fun)
	}
	time.Sleep(100 * time.Millisecond)
	check(gopool.totalSize == 10000, "test pool total size")
	check(gopool.activeSize == 10000, "test pool active size")
	for i := 0; i < 10000; i++ {
		tc <- 0
	}

	time.Sleep(100 * time.Millisecond)
	check(gopool.activeSize == 0, "test pool active size")
	check(gopool.totalSize == 8000, "test pool total size")

	gopool.Stop()
	check(gopool.totalSize == 0, "test pool total size")
	check(gopool.activeSize == 0, "test pool active size")
}

func BenchmarkGoroutinePool(b *testing.B) {
	b.ResetTimer()

	var gopool *GoroutinePool = new(GoroutinePool)
	gopool.Init()
	go gopool.Run()
	var i int = 0
	fun := func() { i++ }

	for i := 0; i < b.N; i++ {
		gopool.AsyncRun(fun)
	}

	gopool.SendCommand(FLAGTERM)
	// benchmark会启动多个routine进行压力测试
	// 但不知道为什么SendCommand总会多执行，导致管道卡住
	// 所以在压力测试时将gopool.WaitRet()注释掉
	// gopool.WaitRet()
}
