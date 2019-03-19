package config

import (
	"fmt"
	"testing"

	"flag"
	"github.com/stretchr/testify/assert"
	"os"
	"sync/atomic"
	"time"
)

var (
	// test address
	address  string = "100.81.245.155:20111"
	username string = "admin"
	password string = "admin"
	db       string = "inspectorConfig"
)

func TestGetAndSetAndDelete(t *testing.T) {
	var (
		ret        []string
		retBool    bool
		retString  string
		retInt     int
		retFloat32 float32
		retFloat64 float64
		retMap     map[string]interface{}
		// retInt64   int64
		err error
		nr  uint
	)
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	factory := ConfigFactory{Name: "mongo"}
	handler, err := factory.Create(address, username, password, db, 0)
	assert.Equal(t, err, nil, "should be nil")

	// delete all first
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	err = handler.DeleteAll()
	assert.Equal(t, err, nil, "should be nil")

	// section must be empty first
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	ret, err = handler.GetSectionList()
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, ret, []string{}, "something error")

	// add item string
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	err = handler.SetItem("section1", "key1", "value1")
	assert.Equal(t, err, nil, "should be nil")

	// get item
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	retString, err = handler.GetString("section1", "key1")
	assert.Equal(t, retString, "value1")

	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	err = handler.DeleteItem("section1", "key1", "path1", "path2")
	assert.Equal(t, err, nil, "should be nil")

	// add item string cover previous one
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	err = handler.SetItem("section1", "key1", "value2", "path1", "path2")
	assert.Equal(t, err, nil, "should be nil")

	// get item
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	retString, err = handler.GetString("section1", "key1", "path1", "path2")
	assert.Equal(t, retString, "value2")

	// get item
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	retString, err = handler.GetString("section1", "key2", "path1", "path2")
	assert.NotEqual(t, err, "value2", nil, "must be error")

	// add item int
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	err = handler.SetItem("section1", "key2", int(123456789), "fuckInt1", "fuckInt2", "fuckInt3")
	assert.Equal(t, err, nil, "should be nil")

	// get item
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	retInt, err = handler.GetInt("section1", "key2", "fuckInt1", "fuckInt2", "fuckInt3")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, retInt, 123456789)

	// add item
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	err = handler.SetArray("section1", "key3", []string{"1.1", "1.2", "1.3"})
	assert.Equal(t, err, nil, "should be nil")

	// get item
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	retString, err = handler.GetString("section1", "key3", "[0]")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, retString, "1.1")

	// add item float
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	err = handler.SetItem("section1", "key4", float64(3.141592635979345), "fuckFloat2", "fuckFloat3", "fuckFloat4", "fuckFloat5", "fuckFloat6")
	assert.Equal(t, err, nil, "should be nil")

	// get item
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	retFloat64, err = handler.GetFloat64("section1", "key4", "fuckFloat2", "fuckFloat3", "fuckFloat4", "fuckFloat5", "fuckFloat6")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, retFloat64, float64(3.141592635979345))

	// get item
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	retFloat32, err = handler.GetFloat32("section1", "key4", "fuckFloat2", "fuckFloat3", "fuckFloat4", "fuckFloat5", "fuckFloat6")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, retFloat32, float32(3.141592635979345))

	//// add item int64
	//fmt.Println("TestGetAndSetAndDelete case 16.")
	//err = handler.SetItem("section2", "key1", int64(-1234567891011121314), "hello")
	//assert.Equal(t, err, nil, "should be nil")
	//
	//// get item
	//fmt.Println("TestGetAndSetAndDelete case 17.")
	//retInt64, err = handler.GetInt64("section2", "key1", "hello")
	//assert.Equal(t, err, nil, "should be nil")
	//assert.Equal(t, retInt64, int64(-1234567891011121314))

	// delete section2
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	err = handler.DeleteSection("section2")
	assert.NotEqual(t, err, nil, "should be nil")

	// get item again
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	_, err = handler.GetInt64("section2", "key1", "hello")
	assert.NotEqual(t, err, nil, "must be error")

	// delete key1/path1/path2
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	err = handler.DeleteItem("section1", "key1", "path1", "path2")
	assert.Equal(t, err, nil, "should be nil")

	// get item
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	_, err = handler.GetString("section1", "key1", "path1", "path2")
	assert.NotEqual(t, err, nil, "must be error")

	// delete key1
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	err = handler.DeleteItem("section1", "key1")
	assert.Equal(t, err, nil, "should be nil")

	// get item
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	_, err = handler.GetString("section1", "key1", "path1")
	assert.NotEqual(t, err, nil, "must be error")

	// get item
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	_, err = handler.GetString("section1", "key1")
	assert.NotEqual(t, err, nil, "must be error")

	// add item int64
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	err = handler.SetItem("section3", "key1", true, "level1", "level2")
	assert.Equal(t, err, nil, "should be nil")

	// get item
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	retBool, err = handler.GetBool("section3", "key1", "level1", "level2")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, retBool, true)

	// add item array
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	err = handler.SetArray("section3", "key2", []string{"I'm first", "I'm second", "I'm third"})
	assert.Equal(t, err, nil, "should be nil")

	// get item
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	ret, err = handler.GetArray("section3", "key2")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, ret, []string{"I'm first", "I'm second", "I'm third"})

	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	ret, err = handler.GetKeyList("section1")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, ret, []string{"key2", "key3", "key4"})

	// add item array
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	err = handler.SetItem("section1", "key5", 1.1)
	assert.Equal(t, err, nil, "should be nil")

	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	ret, err = handler.GetKeyList("section1")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, ret, []string{"key2", "key3", "key4", "key5"})

	// test set array by setItem
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	err = handler.SetItem("section10", "key10", []string{"1.1", "hello", "----"}, "l1", "l2", "l3")
	assert.Equal(t, err, nil, "should be nil")

	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	ret, err = handler.GetArray("section10", "key10", "l1", "l2", "l3")
	assert.Equal(t, ret, []string{"1.1", "hello", "----"})

	// add item int
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	err = handler.SetItem("section11", "key2", int(-150000000), "fuckInt1", "fuckInt2", "fuckInt3")
	assert.Equal(t, err, nil, "should be nil")

	// get item
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	retInt, err = handler.GetInt("section11", "key2", "fuckInt1", "fuckInt2", "fuckInt3")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, retInt, -150000000)

	// add item int
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	err = handler.SetItem("section11", "key21", -1.12345678910111213, "fuckInt1", "fuckInt2", "fuckInt3")
	assert.Equal(t, err, nil, "should be nil")

	// get item
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	retFloat64, err = handler.GetFloat64("section11", "key21", "fuckInt1", "fuckInt2", "fuckInt3")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, retFloat64, -1.12345678910111213)

	// get item
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	retFloat32, err = handler.GetFloat32("section11", "key21", "fuckInt1", "fuckInt2", "fuckInt3")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, retFloat32, float32(-1.12345678910111213))

	//// add item int
	//fmt.Println("TestGetAndSetAndDelete case 39.")
	//err = handler.SetItem("section11", "key2", int64(-1500000000000000), "fuckInt1", "fuckInt2", "fuckInt3")
	//assert.Equal(t, err, nil, "should be nil")
	//
	//// get item
	//fmt.Println("TestGetAndSetAndDelete case 40.")
	//retInt64, err = handler.GetInt64("section11", "key2", "fuckInt1", "fuckInt2", "fuckInt3")
	//assert.Equal(t, err, nil, "should be nil")
	//assert.Equal(t, retInt64, int64(-1500000000000000))

	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	err = handler.SetItem("section110", "key21", "xxx", "v1")
	assert.Equal(t, err, nil, "should be nil")
	err = handler.SetItem("section110", "key21", 1.0, "v2_3_4")
	assert.Equal(t, err, nil, "should be nil")
	err = handler.SetItem("section110", "key21", -123456789, "v3")
	assert.Equal(t, err, nil, "should be nil")

	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	retMap, err = handler.GetMap("section110", "key21")
	assert.Equal(t, err, nil, "should be nil")

	assert.Equal(t, "xxx", retMap["v1"].(string), "should be nil")
	assert.Equal(t, 1.0, retMap["v2_3_4"].(float64), "should be nil")
	assert.Equal(t, float64(-123456789), retMap["v3"].(float64), "should be nil")

	// delete all
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	err = handler.DeleteAll()
	assert.Equal(t, err, nil, "should be nil")

	// get item
	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	_, err = handler.GetArray("section3", "key2")
	assert.NotEqual(t, err, nil, "must be error")

	nr++
	fmt.Printf("TestGetAndSetAndDelete case %d.\n", nr)
	watcher := Watcher{
		Event: NODECHANGED,
		Handler: func(event WatcheEvent) error {
			return nil
		},
	}
	err = handler.RegisterGlobalWatcher("section3", "key2", &watcher)
	assert.NotEqual(t, err, nil, "must be error")
}

func TestLockAndUnLock(t *testing.T) {
	var (
		ret       []string
		retString string
		err       error
		nr        uint
	)
	nr++
	fmt.Printf("TestLockAndUnLock case %d.\n", nr)
	factory := ConfigFactory{Name: "mongo"}
	handler, err := factory.Create(address, username, password, db, 0)
	assert.Equal(t, err, nil, "should be nil")

	// delete all first
	nr++
	fmt.Printf("TestLockAndUnLock case %d.\n", nr)
	err = handler.DeleteAll()
	assert.Equal(t, err, nil, "should be nil")

	// section must be empty first
	nr++
	fmt.Printf("TestLockAndUnLock case %d.\n", nr)
	ret, err = handler.GetSectionList()
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, ret, []string{}, "something error")

	// add item string
	nr++
	fmt.Printf("TestLockAndUnLock case %d.\n", nr)
	err = handler.SetItem("section1", "key1", "I'm the value", "path1")
	assert.Equal(t, err, nil, "should be nil")

	// get item
	nr++
	fmt.Printf("TestLockAndUnLock case %d.\n", nr)
	retString, err = handler.GetString("section1", "key1", "path1")
	assert.Equal(t, retString, "I'm the value", "should be nil")

	// lock section1
	nr++
	fmt.Printf("TestLockAndUnLock case %d.\n", nr)
	err = handler.Lock("section1", "")
	assert.Equal(t, err, nil, "should be nil")

	// get key list
	nr++
	fmt.Printf("TestLockAndUnLock case %d.\n", nr)
	ret, err = handler.GetKeyList("section1")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, ret, []string{"key1"})

	// lock section1 which is error as expect
	nr++
	fmt.Printf("TestLockAndUnLock case %d.\n", nr)
	err = handler.Lock("section1", "")
	assert.NotEqual(t, err, nil, "must be error")

	// lock section1/key1 which is error as expect
	nr++
	fmt.Printf("TestLockAndUnLock case %d.\n", nr)
	err = handler.Lock("section1", "key1")
	assert.NotEqual(t, err, nil, "must be error")

	// unlock key, it's useless
	handler.Unlock("section1", "")

	// lock key again
	nr++
	fmt.Printf("TestLockAndUnLock case %d.\n", nr)
	err = handler.Lock("section1", "")
	assert.Equal(t, err, nil, "should be nil")

	time.Sleep((RecordLockTimeout) / 2 * time.Second)

	// lock key again
	nr++
	fmt.Printf("TestLockAndUnLock case %d.\n", nr)
	err = handler.Lock("section1", "key2")
	assert.NotEqual(t, err, nil, "must be error")

	time.Sleep((RecordLockTimeout + 1) * time.Second)

	// lock key again
	nr++
	fmt.Printf("TestLockAndUnLock case %d.\n", nr)
	err = handler.Lock("section1", "")
	assert.Equal(t, err, nil, "should be nil")
}

func TestExportAndImport(t *testing.T) {
	var (
		ret        []string
		retBool    bool
		retString  string
		retFloat64 float64
		retInt     int
		err        error
		nr         uint
	)

	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	factory := ConfigFactory{Name: "mongo"}
	handler, err := factory.Create(address, username, password, db, 0)
	assert.Equal(t, err, nil, "should be nil")

	// delete all first
	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	err = handler.DeleteAll()
	assert.Equal(t, err, nil, "should be nil")

	// add item bool
	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	err = handler.SetItem("section1", "key1", true, "level1", "level2")
	assert.Equal(t, err, nil, "should be nil")

	// add item string
	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	err = handler.SetItem("section1", "key2", "fuck", "level1", "level2")
	assert.Equal(t, err, nil, "should be nil")

	// add item array
	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	err = handler.SetArray("section1", "key3", []string{"first", "second"})
	assert.Equal(t, err, nil, "should be nil")

	// add item float
	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	err = handler.SetItem("section2", "key4", 1.123)
	assert.Equal(t, err, nil, "should be nil")

	// add item int64
	// pay attention, it may loses accuracy if the int64 is too big
	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	err = handler.SetItem("section3", "key5", int(123456789), "level1")
	assert.Equal(t, err, nil, "should be nil")

	// add item int64
	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	err = handler.SetItem("section3", "key6", int(-123456789), "level1")
	assert.Equal(t, err, nil, "should be nil")

	// add item int
	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	err = handler.SetItem("section3", "key7", 1, "level1")
	assert.Equal(t, err, nil, "should be nil")

	// delete int
	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	err = handler.DeleteItem("section3", "key7")
	assert.Equal(t, err, nil, "should be nil")

	// add item int
	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	err = handler.SetItem("section3", "key7", 1, "level1", "l2", "l3", "l4", "l5", "l6", "l6", "l6", "l6", "l6", "l6")
	assert.Equal(t, err, nil, "should be nil")

	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	err = handler.Lock("section1", "")
	assert.Equal(t, err, nil, "should be nil")

	exportFilename := "export.ini"
	err = handler.Export(exportFilename)
	assert.Equal(t, err, nil, "should be nil")

	/*----------------------splitter-------------------*/
	// delete all
	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	err = handler.DeleteAll()
	assert.Equal(t, err, nil, "should be nil")

	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	err = handler.Import(exportFilename)
	assert.Equal(t, err, nil, "should be nil")

	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	retBool, err = handler.GetBool("section1", "key1", "level1", "level2")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, retBool, true, "should be equal")

	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	retString, err = handler.GetString("section1", "key2", "level1", "level2")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, retString, "fuck", "should be equal")

	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	ret, err = handler.GetArray("section1", "key3")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, ret, []string{"first", "second"}, "should be equal")

	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	retFloat64, err = handler.GetFloat64("section2", "key4")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, retFloat64, 1.1230)

	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	retInt, err = handler.GetInt("section3", "key5", "level1")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, retInt, 123456789)

	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	retInt, err = handler.GetInt("section3", "key6", "level1")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, retInt, -123456789)

	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	retFloat64, err = handler.GetFloat64("section3", "key7", "level1", "l2", "l3", "l4", "l5", "l6", "l6", "l6", "l6", "l6", "l6")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, retFloat64, 1.0)

	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	err = os.Remove(exportFilename)
	assert.Equal(t, err, nil, "should be nil")

	/*----------------------splitter-------------------*/
	// add item float
	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	err = handler.SetItem("section3", "key666", float64(3.141592635979345), "fuckFloat2", "fuckFloat3", "fuckFloat4", "fuckFloat5", "fuckFloat6")
	assert.Equal(t, err, nil, "should be nil")

	exportFilename = "export2.ini"
	err = handler.Export(exportFilename)
	assert.Equal(t, err, nil, "should be nil")

	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	err = handler.Import(exportFilename)
	assert.Equal(t, err, nil, "should be nil")

	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	retFloat64, err = handler.GetFloat64("section3", "key666", "fuckFloat2", "fuckFloat3", "fuckFloat4", "fuckFloat5", "fuckFloat6")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, retFloat64, float64(3.141592635979345))

	// delete all
	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	err = handler.DeleteAll()
	assert.Equal(t, err, nil, "should be nil")

	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	err = handler.Import(exportFilename)
	assert.Equal(t, err, nil, "should be nil")

	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	retFloat64, err = handler.GetFloat64("section3", "key666", "fuckFloat2", "fuckFloat3", "fuckFloat4", "fuckFloat5", "fuckFloat6")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, retFloat64, float64(3.141592635979345))

	return
	nr++
	fmt.Printf("TestExportAndImport case %d.\n", nr)
	err = os.Remove(exportFilename)
	assert.Equal(t, err, nil, "should be nil")
}

func TestWathcer(t *testing.T) {
	flag.Set("stderrthreshold", "warning")
	flag.Set("v", "0")

	watcherInterval := 2000
	nr := 1
	fmt.Printf("TestWathcer case %d\n", nr)
	factory := ConfigFactory{Name: "mongo"}
	handler, err := factory.Create(address, username, password, db, watcherInterval)
	assert.Equal(t, err, nil, "should be nil")

	// delete all
	nr++
	fmt.Printf("TestWathcer case %d\n", nr)
	err = handler.DeleteAll()
	assert.Equal(t, err, nil, "should be nil")

	var s1k1Create uint32 = 0
	watcher1 := Watcher{
		Event: NODECREATED,
		Handler: func(event WatcheEvent) error {
			fmt.Printf("section1 key1 NODECHANGED\n")
			atomic.CompareAndSwapUint32(&s1k1Create, 0, 1)
			return nil
		},
	}
	var s1k1Changed uint32 = 0
	var s1k1Deleted uint32 = 0
	watcher2 := Watcher{
		Event: NODECHANGED,
		Handler: func(event WatcheEvent) error {
			fmt.Printf("section1 key1 NODECHANGED\n")
			atomic.CompareAndSwapUint32(&s1k1Changed, 0, 1)
			return nil
		},
	}
	err = handler.RegisterGlobalWatcher("section1", "key1", &watcher1)
	assert.Equal(t, err, nil, "should be nil")
	err = handler.RegisterGlobalWatcher("section1", "key1", &watcher2)
	assert.Equal(t, err, nil, "should be nil")

	// add item bool
	nr++
	fmt.Printf("TestWathcer case %d\n", nr)
	err = handler.SetItem("section1", "key1", true, "level1", "level2")
	assert.Equal(t, err, nil, "should be nil")

	time.Sleep(time.Duration(watcherInterval+1000) * time.Millisecond)
	assert.Equal(t, uint32(1), atomic.LoadUint32(&s1k1Create), "should be equal")

	nr++
	fmt.Printf("TestWathcer case %d\n", nr)
	err = handler.SetItem("section1", "key1", false, "level1", "level2")
	assert.Equal(t, err, nil, "should be nil")

	time.Sleep(time.Duration(watcherInterval+1000) * time.Millisecond)
	assert.Equal(t, uint32(1), atomic.LoadUint32(&s1k1Changed), "should be equal")

	s1k1Changed = 0
	time.Sleep(time.Duration(watcherInterval+1000) * time.Millisecond)
	assert.Equal(t, uint32(0), atomic.LoadUint32(&s1k1Changed), "should be equal")

	// delete item
	nr++
	fmt.Printf("TestWathcer case %d\n", nr)
	s1k1Changed = 0

	s1k1Watcher := Watcher{
		Event: NODEDELETED,
		Handler: func(event WatcheEvent) error {
			fmt.Printf("section1 key1 NODEDELETED\n")
			atomic.CompareAndSwapUint32(&s1k1Deleted, 0, 1)
			return nil
		},
	}
	err = handler.RegisterGlobalWatcher("section1", "key1", &s1k1Watcher)
	assert.Equal(t, err, nil, "should be nil")

	err = handler.DeleteItem("section1", "key1")
	assert.Equal(t, err, nil, "should be nil")

	time.Sleep(time.Duration(watcherInterval+1000) * time.Millisecond)
	assert.Equal(t, uint32(0), atomic.LoadUint32(&s1k1Changed), "should be equal")
	assert.Equal(t, uint32(1), atomic.LoadUint32(&s1k1Deleted), "should be equal")

	/*----------------------splitter-------------------*/
	time.Sleep(time.Duration(watcherInterval+1000) * time.Millisecond)
	assert.Equal(t, uint32(0), atomic.LoadUint32(&s1k1Changed), "should be equal")

	// register NODECHANGED watcher on section1
	var s1Create uint32 = 0
	s1CreateWatcher := Watcher{
		Event: NODECREATED,
		Handler: func(event WatcheEvent) error {
			fmt.Printf("section1 NODECHANGED\n")
			atomic.CompareAndSwapUint32(&s1Create, 0, 1)
			return nil
		},
	}
	err = handler.RegisterGlobalWatcher("section1", "", &s1CreateWatcher)
	assert.Equal(t, err, nil, "should be nil")

	// register NODECHANGED watcher on section1
	var s1Change uint32 = 0
	s1ChangeWatcher := Watcher{
		Event: NODECHANGED,
		Handler: func(event WatcheEvent) error {
			fmt.Printf("section1 NODECHANGED\n")
			atomic.CompareAndSwapUint32(&s1Change, 0, 1)
			return nil
		},
	}
	err = handler.RegisterGlobalWatcher("section1", "", &s1ChangeWatcher)
	assert.Equal(t, err, nil, "should be nil")

	// register NODEALL watcher on section1
	var s1All uint32 = 0
	s1AllWatcher := Watcher{
		Event: NODEALL,
		Handler: func(event WatcheEvent) error {
			fmt.Printf("section1 NODEALL\n")
			atomic.CompareAndSwapUint32(&s1All, 0, 1)
			return nil
		},
	}
	err = handler.RegisterGlobalWatcher("section1", "", &s1AllWatcher)
	assert.Equal(t, err, nil, "should be nil")

	// add item bool
	nr++
	fmt.Printf("TestWathcer case %d\n", nr)
	err = handler.SetItem("section1", "key1", true, "level1", "level2")
	assert.Equal(t, err, nil, "should be nil")

	time.Sleep(time.Duration(watcherInterval+1000) * time.Millisecond)
	assert.Equal(t, uint32(0), atomic.LoadUint32(&s1k1Changed), "should be equal")
	assert.Equal(t, uint32(1), atomic.LoadUint32(&s1k1Create), "should be equal")
	assert.Equal(t, uint32(1), atomic.LoadUint32(&s1Create), "should be equal")
	assert.Equal(t, uint32(0), atomic.LoadUint32(&s1Change), "should be equal")
	assert.Equal(t, uint32(1), atomic.LoadUint32(&s1All), "should be equal")
	s1k1Create = 0
	s1k1Changed = 0
	atomic.CompareAndSwapUint32(&s1Create, 1, 0)
	atomic.CompareAndSwapUint32(&s1All, 1, 0)
	time.Sleep(time.Duration(watcherInterval+1000) * time.Millisecond) // wait for handler close

	s1k1Create = 0
	s1k1Changed = 0
	s1Create = 0
	s1Change = 0
	s1All = 0
	// set item false
	nr++
	fmt.Printf("TestWathcer case %d\n", nr)
	err = handler.SetItem("section1", "key1", false, "level1", "level2")
	assert.Equal(t, err, nil, "should be nil")

	time.Sleep(time.Duration(watcherInterval+1000) * time.Millisecond)
	assert.Equal(t, uint32(1), atomic.LoadUint32(&s1k1Changed), "should be equal")
	assert.Equal(t, uint32(0), atomic.LoadUint32(&s1k1Create), "should be equal")
	assert.Equal(t, uint32(0), atomic.LoadUint32(&s1Create), "should be equal")
	assert.Equal(t, uint32(1), atomic.LoadUint32(&s1Change), "should be equal")
	assert.Equal(t, uint32(1), atomic.LoadUint32(&s1All), "should be equal")

	s1k1Create = 0
	s1k1Changed = 0
	s1Create = 0
	s1Change = 0
	s1All = 0

	// delete all-key
	nr++
	fmt.Printf("TestWathcer case %d\n", nr)
	err = handler.RemoveGlobalWatcher("section1", "")
	assert.Equal(t, err, nil, "should be nil")

	err = handler.SetItem("section1", "key1", true, "level1", "level3")
	assert.Equal(t, err, nil, "should be nil")

	assert.Equal(t, uint32(0), atomic.LoadUint32(&s1k1Changed), "should be equal")
	assert.Equal(t, uint32(0), atomic.LoadUint32(&s1k1Create), "should be equal")
	assert.Equal(t, uint32(0), atomic.LoadUint32(&s1Create), "should be equal")
	assert.Equal(t, uint32(0), atomic.LoadUint32(&s1Change), "should be equal")
	assert.Equal(t, uint32(0), atomic.LoadUint32(&s1All), "should be equal")

	// close handler and register again
	nr++
	fmt.Printf("TestWathcer case %d\n", nr)
	err = handler.RegisterGlobalWatcher("section1", "", &s1AllWatcher)
	assert.Equal(t, err, nil, "should be nil")

	handler.Close()
	s1k1Create = 0
	s1k1Changed = 0
	s1Create = 0
	s1Change = 0
	s1All = 0
	time.Sleep(time.Duration(watcherInterval+1000) * time.Millisecond)

	nr++
	fmt.Printf("TestWathcer case %d\n", nr)
	err = handler.SetItem("section1", "key1", 1.1)
	assert.Equal(t, err, nil, "should be nil")

	time.Sleep(time.Duration(watcherInterval+1000) * time.Millisecond)
	assert.Equal(t, uint32(0), atomic.LoadUint32(&s1k1Changed), "should be equal")
	assert.Equal(t, uint32(0), atomic.LoadUint32(&s1k1Create), "should be equal")
	assert.Equal(t, uint32(0), atomic.LoadUint32(&s1Create), "should be equal")
	assert.Equal(t, uint32(0), atomic.LoadUint32(&s1All), "should be equal")
}

func TestWathcer2(t *testing.T) {
	flag.Set("stderrthreshold", "warning")
	flag.Set("v", "0")

	watcherInterval := 2000
	nr := 1
	fmt.Printf("TestWathcer2 case %d\n", nr)
	factory := ConfigFactory{Name: "mongo"}
	handler, err := factory.Create(address, username, password, db, watcherInterval)
	assert.Equal(t, err, nil, "should be nil")

	{
		// delete all
		nr++
		fmt.Printf("TestWathcer2 case %d\n", nr)
		err = handler.DeleteAll()
		assert.Equal(t, err, nil, "should be nil")
	}

	{
		nr++
		fmt.Printf("TestWathcer2 case %d\n", nr)

		var s1All uint32 = 0
		s1AllWatcher := Watcher{
			Event: NODEALL,
			Handler: func(event WatcheEvent) error {
				fmt.Printf("section3 NODEALL\n")
				atomic.CompareAndSwapUint32(&s1All, 0, 1)
				return nil
			},
		}
		err = handler.RegisterGlobalWatcher("section3", "", &s1AllWatcher)
		assert.Equal(t, err, nil, "should be nil")

		nr++
		fmt.Printf("TestWathcer2 case %d\n", nr)
		err = handler.SetItem("section3", "key1", true, "level1")
		assert.Equal(t, err, nil, "should be nil")

		time.Sleep(time.Duration(watcherInterval+1000) * time.Millisecond)
		assert.Equal(t, uint32(1), atomic.LoadUint32(&s1All), "should be equal")

		atomic.CompareAndSwapUint32(&s1All, 1, 0)
		nr++
		fmt.Printf("TestWathcer2 case %d\n", nr)
		err = handler.SetItem("section3", "key1", "fuck")
		assert.Equal(t, err, nil, "should be nil")

		time.Sleep(time.Duration(watcherInterval+1000) * time.Millisecond)
		assert.Equal(t, uint32(1), atomic.LoadUint32(&s1All), "should be equal")

		atomic.CompareAndSwapUint32(&s1All, 1, 0)
		nr++
		fmt.Printf("TestWathcer2 case %d\n", nr)
		err = handler.DeleteItem("section3", "key1")
		assert.Equal(t, err, nil, "should be nil")

		time.Sleep(time.Duration(watcherInterval+1000) * time.Millisecond)
		assert.Equal(t, uint32(1), atomic.LoadUint32(&s1All), "should be equal")

		atomic.CompareAndSwapUint32(&s1All, 1, 0)
		nr++
		fmt.Printf("TestWathcer2 case %d\n", nr)
		err = handler.SetItem("section3", "key1", "key2")
		assert.Equal(t, err, nil, "should be nil")

		time.Sleep(time.Duration(watcherInterval+1000) * time.Millisecond)
		assert.Equal(t, uint32(1), atomic.LoadUint32(&s1All), "should be equal")
	}
}
