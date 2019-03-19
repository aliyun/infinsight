/*
// =====================================================================================
//
//       Filename:  LocalConfig.go
//
//    Description:
//
//        Version:  1.0
//        Created:  06/10/2018 03:15:03 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package config

import (
	"fmt"
	"os/exec"
	"runtime"
	"testing"
	"time"
)

func TestConnect(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var err error
	var l []string

	var cfg ConfigInterface
	factory := ConfigFactory{Name: "local"}

	// test no exist file
	cfg, err = factory.Create("no-exist.cfg", "", "", "", 0)
	check(err != nil, "test no-exist file EstablishConnect")

	// test exist file
	cfg, err = factory.Create("test.cfg", "", "", "", 0)
	check(err == nil, "test EstablishConnect")

	l, err = cfg.GetSectionList()
	check(err == nil, "test GetSectionList")
	check(len(l) == 7, "test section len")
}

func TestGet(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var err error
	var str string
	var n32 int
	var n64 int64
	var f32 float32
	var list []string

	// var cfg ConfigInterface = new(LocalConfig)
	var cfg ConfigInterface
	factory := ConfigFactory{Name: "local"}

	cfg, err = factory.Create("test.cfg", "", "", "", 0)
	check(err == nil, "test EstablishConnect")

	str, err = cfg.GetString("DEFAULT", "normalString")
	check(err == nil, "test err")
	check(str == "abc", "test str")

	list, err = cfg.GetArray("DEFAULT", "normalArray")
	check(err == nil, "test err")
	check(len(list) == 3, "test str")
	check(list[0] == "a", "test str")
	check(list[1] == "2", "test str")
	check(list[2] == "4", "test str")

	str, err = cfg.GetString("job", "MongoDB", "Title")
	check(err == nil, "test err")
	check(str == "AlibabaMongoDB", "test str")

	n32, err = cfg.GetInt("job", "MongoDB", "MetaServerPort")
	check(err == nil, "test err")
	check(n32 == 3306, "test int32")

	n64, err = cfg.GetInt64("type", "MongoDBTemp", "bigInt")
	check(err == nil, "test err")
	check(n64 == 9999999999, "test int64")

	f32, err = cfg.GetFloat32("type", "MongoDBTemp", "pi")
	check(err == nil, "test err")
	check(f32 == 3.14, "test float32")

	list, err = cfg.GetArray("type", "MongoDBTemp", "list")
	check(err == nil, "test err")
	check(len(list) == 5, "test len")
	check(list[0] == "a", "test list")
	check(list[1] == "b", "test list")
	check(list[2] == "{\"a\":1}", "test list")
	check(list[3] == "4", "test list")
	check(list[4] == "5", "test list")

}

func TestSetItem(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var err error
	var str string

	var cfg ConfigInterface
	factory := ConfigFactory{Name: "local"}

	// test exist file
	cfg, err = factory.Create("test.cfg", "", "", "", 0)
	check(err == nil, "test EstablishConnect")

	err = cfg.SetItem("DEFAULT", "test", "test")
	check(err == nil, "test SetItem")
	str, err = cfg.GetString("DEFAULT", "test")
	check(str == "test", "test str")

	err = cfg.SetItem("test", "test", "test")
	check(err == nil, "test SetItem")
	str, err = cfg.GetString("test", "test")
	check(str == "test", "test str")

	err = cfg.SetItem("test", "test", 0, "a")
	check(err == nil, "test SetItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"a\":0}", "test str")

	err = cfg.SetItem("test", "test", 1, "a")
	check(err == nil, "test SetItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"a\":1}", "test str")

	err = cfg.SetItem("test", "test", 1.1, "a", "a1")
	check(err != nil, "test SetItem")

	err = cfg.SetItem("test", "test", []byte("{\"a1\":1.1}"), "a")
	check(err == nil, "test SetItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"a\":{\"a1\":1.1}}", "test str")

	err = cfg.SetItem("test", "test", 2, "b")
	check(err == nil, "test SetItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"a\":{\"a1\":1.1},\"b\":2}", "test str")

	err = cfg.SetItem("test", "test", 3, "c", "c1")
	check(err == nil, "test SetItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"a\":{\"a1\":1.1},\"b\":2,\"c\":{\"c1\":3}}", "test str")

	err = cfg.SetItem("test", "test", 4, "d", "[1]")
	check(err == nil, "test SetItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"a\":{\"a1\":1.1},\"b\":2,\"c\":{\"c1\":3},\"d\":[null,4]}", "test str")

	err = cfg.SetItem("test", "test", 4, "d", "[1]", "d3")
	check(err != nil, "test SetItem")

	err = cfg.SetItem("test", "test", []byte("{\"d3\":4}"), "d", "[1]")
	check(err == nil, "test SetItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"a\":{\"a1\":1.1},\"b\":2,\"c\":{\"c1\":3},\"d\":[null,{\"d3\":4}]}", "test str")

	err = cfg.SetItem("test", "test", 3, "d", "[0]", "[2]")
	check(err == nil, "test SetItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"a\":{\"a1\":1.1},\"b\":2,\"c\":{\"c1\":3},\"d\":[[null,null,3],{\"d3\":4}]}", "test str")

	err = cfg.SetItem("test", "test", 7, "d", "[0]", "[2]")
	check(err == nil, "test SetItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"a\":{\"a1\":1.1},\"b\":2,\"c\":{\"c1\":3},\"d\":[[null,null,7],{\"d3\":4}]}", "test str")

	err = cfg.SetItem("test", "test", 44, "d", "[0]", "[0]", "d4")
	check(err == nil, "test SetItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"a\":{\"a1\":1.1},\"b\":2,\"c\":{\"c1\":3},\"d\":[[{\"d4\":44},null,7],{\"d3\":4}]}", "test str")

	err = cfg.SetItem("test", "test", 1, "a")
	check(err == nil, "test SetItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"a\":1,\"b\":2,\"c\":{\"c1\":3},\"d\":[[{\"d4\":44},null,7],{\"d3\":4}]}", "test str")

	err = cfg.SetItem("test", "test", []byte("{\"a2\":2.2}"), "a")
	check(err == nil, "test SetItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"a\":{\"a2\":2.2},\"b\":2,\"c\":{\"c1\":3},\"d\":[[{\"d4\":44},null,7],{\"d3\":4}]}", "test str")

	err = cfg.SetItem("test", "test", "{}")
	check(err == nil, "test SetItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{}", "test str")

	// for TestDelItemAndSection
	err = cfg.SetItem("test", "test", "{\"a\":{\"a2\":2.2},\"b\":2,\"c\":{\"c1\":3},\"d\":[[{\"d4\":44},null,7],{\"d3\":4}]}")
	check(err == nil, "test SetItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"a\":{\"a2\":2.2},\"b\":2,\"c\":{\"c1\":3},\"d\":[[{\"d4\":44},null,7],{\"d3\":4}]}", "test str")
}

func TestSetArray(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var err error
	var str string

	// var cfg ConfigInterface = new(LocalConfig)
	factory := ConfigFactory{Name: "local"}
	cfg, err := factory.Create("test.cfg", "", "", "", 0)
	check(err == nil, "test EstablishConnect")

	err = cfg.SetArray("test", "array", []string{"a1", "a2", "a3", "a4", "a5"})
	check(err == nil, "test SetArray")
	str, err = cfg.GetString("test", "array")
	check(str == "[a1,a2,a3,a4,a5]", "test str")

	err = cfg.SetArray("test", "array", []string{"a1", "a3", "a5"})
	check(err == nil, "test SetArray")
	str, err = cfg.GetString("test", "array")
	check(str == "[a1,a3,a5]", "test str")

	err = cfg.SetArray("test", "array", []string{})
	check(err == nil, "test SetArray")
	str, err = cfg.GetString("test", "array")
	check(str == "[]", "test str")

	err = cfg.SetArray("test", "array", nil)
	check(err != nil, "test SetArray")
}

func TestDelItemAndSection(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var err error
	var str string

	// var cfg ConfigInterface = new(LocalConfig)
	factory := ConfigFactory{Name: "local"}
	cfg, err := factory.Create("test.cfg", "", "", "", 0)
	check(err == nil, "test EstablishConnect")

	str, err = cfg.GetString("test", "test")
	check(str == "{\"a\":{\"a2\":2.2},\"b\":2,\"c\":{\"c1\":3},\"d\":[[{\"d4\":44},null,7],{\"d3\":4}]}", "test str")

	err = cfg.DeleteItem("test", "test", "a", "a3")
	check(err != nil, "test DelItem")

	err = cfg.DeleteItem("test", "test", "a", "a2")
	check(err == nil, "test DelItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"a\":{},\"b\":2,\"c\":{\"c1\":3},\"d\":[[{\"d4\":44},null,7],{\"d3\":4}]}", "test str")

	err = cfg.DeleteItem("test", "test", "a", "a3")
	check(err != nil, "test DelItem")

	err = cfg.DeleteItem("test", "test", "an")
	check(err != nil, "test DelItem")

	err = cfg.DeleteItem("test", "test", "a")
	check(err == nil, "test DelItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"b\":2,\"c\":{\"c1\":3},\"d\":[[{\"d4\":44},null,7],{\"d3\":4}]}", "test str")

	err = cfg.DeleteItem("test", "test", "d", "[1]", "d3")
	check(err == nil, "test DelItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"b\":2,\"c\":{\"c1\":3},\"d\":[[{\"d4\":44},null,7],{}]}", "test str")

	err = cfg.DeleteItem("test", "test", "d", "[1]")
	check(err == nil, "test DelItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"b\":2,\"c\":{\"c1\":3},\"d\":[[{\"d4\":44},null,7]]}", "test str")

	err = cfg.DeleteItem("test", "d", "[1]")
	check(err != nil, "test DelItem")

	err = cfg.DeleteItem("test", "test", "d", "[0]", "[0]")
	check(err == nil, "test DelItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"b\":2,\"c\":{\"c1\":3},\"d\":[[null,7]]}", "test str")

	err = cfg.DeleteItem("test", "test", "d", "[0]", "[1]")
	check(err == nil, "test DelItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"b\":2,\"c\":{\"c1\":3},\"d\":[[null]]}", "test str")

	err = cfg.DeleteItem("test", "test", "d", "[0]", "[0]")
	check(err == nil, "test DelItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"b\":2,\"c\":{\"c1\":3},\"d\":[[]]}", "test str")

	err = cfg.DeleteItem("test", "test", "d", "[0]")
	check(err == nil, "test DelItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"b\":2,\"c\":{\"c1\":3},\"d\":[]}", "test str")

	err = cfg.DeleteItem("test", "c", "[0]")
	check(err != nil, "test DelItem")

	err = cfg.DeleteItem("test", "test", "c")
	check(err == nil, "test DelItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"b\":2,\"d\":[]}", "test str")

	err = cfg.DeleteItem("test", "test", "d")
	check(err == nil, "test DelItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{\"b\":2}", "test str")

	err = cfg.DeleteItem("test", "test", "b")
	check(err == nil, "test DelItem")
	str, err = cfg.GetString("test", "test")
	check(str == "{}", "test str")

	err = cfg.DeleteItem("DEFAULT", "test")
	check(err == nil, "test DelItem")

	err = cfg.DeleteItem("test", "test")
	check(err == nil, "test DelItem")

	err = cfg.DeleteSection("test")
	check(err == nil, "test DelItem")
}

func TestSync(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var err error

	// var cfg ConfigInterface = new(LocalConfig)
	factory := ConfigFactory{Name: "local"}
	cfg, err := factory.Create("test.cfg", username, password, db, 0)
	check(err == nil, "test EstablishConnect")

	// test lock
	err = cfg.Lock("DEFAULT", "null")
	check(err == nil, "test lock")

	err = cfg.Lock("DEFAULT", "null")
	check(err != nil, "test lock")

	cfg.Unlock("DEFAULT", "null")

	err = cfg.Lock("DEFAULT", "null")
	check(err == nil, "test lock")

	err = cfg.Lock("DEFAULT", "null")
	check(err != nil, "test lock")

	cfg.Unlock("DEFAULT", "null")
}

func TestWatch(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var err error
	var count int = 0
	var cmd = exec.Command("/bin/bash", "-c", "touch test.cfg")

	// var cfg ConfigInterface = new(LocalConfig)
	factory := ConfigFactory{Name: "local"}
	cfg, err := factory.Create("test.cfg", "", "", "", 0)
	check(err == nil, "test EstablishConnect")

	var watcher = Watcher{
		Event: NODECHANGED,
		Handler: func(event WatcheEvent) error {
			if event == NODECHANGED {
				count++
			}
			return nil
		},
	}
	cfg.RegisterGlobalWatcher("test", "test", &watcher)

	time.Sleep(checkDuration * time.Millisecond)
	check(count == 1, "test EstablishConnect")
	time.Sleep(checkDuration * time.Millisecond)
	check(count == 1, "test EstablishConnect")

	cmd.Run()

	time.Sleep(checkDuration * time.Millisecond)
	check(count == 2, "test EstablishConnect")
	time.Sleep(checkDuration * time.Millisecond)
	check(count == 2, "test EstablishConnect")

	cfg.RemoveGlobalWatcher("test", "test")

	time.Sleep(checkDuration * time.Millisecond)
	check(count == 2, "test EstablishConnect")
	time.Sleep(checkDuration * time.Millisecond)
	check(count == 2, "test EstablishConnect")

	cmd.Run()

	time.Sleep(checkDuration * time.Millisecond)
	check(count == 2, "test EstablishConnect")
	time.Sleep(checkDuration * time.Millisecond)
	check(count == 2, "test EstablishConnect")
}
