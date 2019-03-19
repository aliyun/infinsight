/*
// =====================================================================================
//
//       Filename:  TimeFlowIndex_test.go
//
//    Description:  基于递增时间流的索引
//
//        Version:  1.0
//        Created:  07/16/2018 08:27:20 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Elwin.Gao (elwin), elwin.gao4444@gmail.com
//        Company:
//
// =====================================================================================
*/

package index

import "fmt"
import "runtime"
import "testing"

func TestTimeFlowIndex(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var key string
	var err error

	// case 1
	{
		var tfIndex = NewTimeFlowIndex("host", "role")
		tfIndex.PushBack("primary", 1000)

		key, err = tfIndex.CheckIndex(999)
		check(err != nil, "test")
		key, err = tfIndex.CheckIndex(1001)
		check(err != nil, "test")

		key, err = tfIndex.CheckIndex(1000)
		check(err == nil, "test")
		check(key == "primary", "test")
	}

	// case 2
	{
		var tfIndex = NewTimeFlowIndex("host", "role")
		tfIndex.PushBack("primary", 1000)
		tfIndex.PushBack("primary", 1002)
		tfIndex.PushBack("primary", 1004)
		tfIndex.PushBack("primary", 1009)

		key, err = tfIndex.CheckIndex(999)
		check(err != nil, "test")
		key, err = tfIndex.CheckIndex(1010)
		check(err != nil, "test")

		key, err = tfIndex.CheckIndex(1005)
		check(err == nil, "test")
		check(key == "primary", "test")

		key, err = tfIndex.CheckIndex(1000)
		check(err == nil, "test")
		check(key == "primary", "test")
	}

	// case 3
	{
		var tfIndex = NewTimeFlowIndex("host", "role")
		tfIndex.PushBack("primary", 1000)
		tfIndex.PushBack("primary", 1009)
		tfIndex.PushBack("secondary", 1020)
		tfIndex.PushBack("secondary", 1029)

		key, err = tfIndex.CheckIndex(999)
		check(err != nil, "test")
		key, err = tfIndex.CheckIndex(1030)
		check(err != nil, "test")

		key, err = tfIndex.CheckIndex(1005)
		check(err == nil, "test")
		check(key == "primary", "test")

		key, err = tfIndex.CheckIndex(1015)
		check(err == nil, "test")
		check(key == "primary", "test")

		key, err = tfIndex.CheckIndex(1025)
		check(err == nil, "test")
		check(key == "secondary", "test")
	}

	// case 4
	{
		var tfIndex = NewTimeFlowIndex("host", "role")
		tfIndex.PushBack("primary", 1000)
		tfIndex.PushBack("primary", 1009)
		tfIndex.PushBack("secondary", 1020)
		tfIndex.PushBack("secondary", 1029)
		err = tfIndex.PushBack("secondary", 1001)
		check(err == nil, "test")
		err = tfIndex.PushBack("secondary", 999)
		check(err != nil, "test")

		key, err = tfIndex.CheckIndex(1005)
		check(err == nil, "test")
		check(key == "secondary", "test")

		key, err = tfIndex.CheckIndex(1015)
		check(err == nil, "test")
		check(key == "secondary", "test")

		key, err = tfIndex.CheckIndex(1025)
		check(err == nil, "test")
		check(key == "secondary", "test")
	}

	// case 5
	{
		var tfIndex = NewTimeFlowIndex("host", "role")
		tfIndex.PushBack("primary", 1000)
		tfIndex.PushBack("secondary", 1029)
		tfIndex.PushBack("hiden", 1059)

		var list []TFIndexKeyTimeRange = nil

		list, err = tfIndex.CheckIndexRange(1, 999)
		check(err != nil, "test")
		check(list == nil, "test")

		list, err = tfIndex.CheckIndexRange(2000, 3000)
		check(err != nil, "test")
		check(list == nil, "test")

		list, err = tfIndex.CheckIndexRange(500, 1030)
		check(err == nil, "test")
		check(len(list) == 2, "test")
		check(list[0].Key == "primary", "test")
		check(list[0].TimeBegin == 1000, "test")
		check(list[0].TimeEnd == 1028, "test")
		check(list[1].Key == "secondary", "test")
		check(list[1].TimeBegin == 1029, "test")
		check(list[1].TimeEnd == 1030, "test")

		list, err = tfIndex.CheckIndexRange(1030, 2000)
		check(err == nil, "test")
		check(len(list) == 2, "test")
		check(list[0].Key == "secondary", "test")
		check(list[0].TimeBegin == 1030, "test")
		check(list[0].TimeEnd == 1058, "test")
		check(list[1].Key == "hiden", "test")
		check(list[1].TimeBegin == 1059, "test")
		check(list[1].TimeEnd == 1059, "test")

		list, err = tfIndex.CheckIndexRange(1015, 1050)
		check(err == nil, "test")
		check(len(list) == 2, "test")
		check(list[0].Key == "primary", "test")
		check(list[0].TimeBegin == 1015, "test")
		check(list[0].TimeEnd == 1028, "test")
		check(list[1].Key == "secondary", "test")
		check(list[1].TimeBegin == 1029, "test")
		check(list[1].TimeEnd == 1050, "test")

		list, err = tfIndex.CheckIndexRange(1030, 1050)
		check(err == nil, "test")
		check(len(list) == 1, "test")
		check(list[0].Key == "secondary", "test")
		check(list[0].TimeBegin == 1030, "test")
		check(list[0].TimeEnd == 1050, "test")

		list, err = tfIndex.CheckIndexRange(1005, 1005)
		check(err == nil, "test")
		check(len(list) == 1, "test")
		check(list[0].Key == "primary", "test")
		check(list[0].TimeBegin == 1005, "test")
		check(list[0].TimeEnd == 1005, "test")

		list, err = tfIndex.CheckIndexRange(0, 10000)
		check(err == nil, "test")
		check(len(list) == 3, "test")
		check(list[0].Key == "primary", "test")
		check(list[0].TimeBegin == 1000, "test")
		check(list[0].TimeEnd == 1028, "test")
		check(list[1].Key == "secondary", "test")
		check(list[1].TimeBegin == 1029, "test")
		check(list[1].TimeEnd == 1058, "test")
		check(list[2].Key == "hiden", "test")
		check(list[2].TimeBegin == 1059, "test")
		check(list[2].TimeEnd == 1059, "test")
	}

	check(true, "test")
}

func TestTimeFlowIndexClean(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var err error
	var list []TFIndexKeyTimeRange = nil

	// case 1
	{
		var tfIndex = NewTimeFlowIndex("host", "role")
		tfIndex.PushBack("primary", 1000)
		tfIndex.PushBack("secondary", 1060)
		tfIndex.PushBack("hiden", 1200)
		tfIndex.PushBack("hiden", 1300)

		tfIndex.CleanPreviousTo(1010)
		list, err = tfIndex.CheckIndexRange(0, 10000)
		check(err == nil, "test")
		check(len(list) == 3, "test")
		check(list[0].Key == "primary", "test")
		check(list[0].TimeBegin == 1010, "test")
		check(list[0].TimeEnd == 1059, "test")
		check(list[1].Key == "secondary", "test")
		check(list[1].TimeBegin == 1060, "test")
		check(list[1].TimeEnd == 1199, "test")
		check(list[2].Key == "hiden", "test")
		check(list[2].TimeBegin == 1200, "test")
		check(list[2].TimeEnd == 1300, "test")
	}

	// case 2
	{
		var tfIndex = NewTimeFlowIndex("host", "role")
		tfIndex.PushBack("primary", 1000)
		tfIndex.PushBack("secondary", 1060)
		tfIndex.PushBack("hiden", 1200)
		tfIndex.PushBack("hiden", 1300)

		tfIndex.CleanPreviousTo(1060)
		list, err = tfIndex.CheckIndexRange(0, 10000)
		check(err == nil, "test")
		check(len(list) == 2, "test")
		check(list[0].Key == "secondary", "test")
		check(list[0].TimeBegin == 1060, "test")
		check(list[0].TimeEnd == 1199, "test")
		check(list[1].Key == "hiden", "test")
		check(list[1].TimeBegin == 1200, "test")
		check(list[1].TimeEnd == 1300, "test")
	}

	// case 3
	{
		var tfIndex = NewTimeFlowIndex("host", "role")
		tfIndex.PushBack("primary", 1000)
		tfIndex.PushBack("secondary", 1060)
		tfIndex.PushBack("hiden", 1200)
		tfIndex.PushBack("hiden", 1300)

		tfIndex.CleanPreviousTo(1100)
		list, err = tfIndex.CheckIndexRange(0, 10000)
		check(err == nil, "test")
		check(len(list) == 2, "test")
		check(list[0].Key == "secondary", "test")
		check(list[0].TimeBegin == 1100, "test")
		check(list[0].TimeEnd == 1199, "test")
		check(list[1].Key == "hiden", "test")
		check(list[1].TimeBegin == 1200, "test")
		check(list[1].TimeEnd == 1300, "test")
	}

	// case 4
	{
		var tfIndex = NewTimeFlowIndex("host", "role")
		tfIndex.PushBack("primary", 1000)
		tfIndex.PushBack("secondary", 1060)
		tfIndex.PushBack("hiden", 1200)
		tfIndex.PushBack("hiden", 1300)

		tfIndex.CleanPreviousTo(1200)
		list, err = tfIndex.CheckIndexRange(0, 10000)
		check(err == nil, "test")
		check(len(list) == 1, "test")
		check(list[0].Key == "hiden", "test")
		check(list[0].TimeBegin == 1200, "test")
		check(list[0].TimeEnd == 1300, "test")
	}

	// case 5
	{
		var tfIndex = NewTimeFlowIndex("host", "role")
		tfIndex.PushBack("primary", 1000)
		tfIndex.PushBack("secondary", 1060)
		tfIndex.PushBack("hiden", 1200)
		tfIndex.PushBack("hiden", 1300)

		tfIndex.CleanPreviousTo(1201)
		list, err = tfIndex.CheckIndexRange(0, 10000)
		check(err == nil, "test")
		check(len(list) == 1, "test")
		check(list[0].Key == "hiden", "test")
		check(list[0].TimeBegin == 1201, "test")
		check(list[0].TimeEnd == 1300, "test")
	}

	check(true, "test")
}
