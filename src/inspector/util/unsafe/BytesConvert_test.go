/*
// =====================================================================================
//
//       Filename:  BytesString.go
//
//    Description:
//
//        Version:  1.0
//        Created:  06/23/2018 02:34:41 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package unsafe

import "fmt"
import "runtime"
import "testing"

func TestBytes2String(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	// byte slice的数据不可能出现在代码段上，所以使用unsafe手段修改是可以的
	var bdata []byte = []byte("hello")
	var sdata string = Bytes2String(bdata)

	// sdata与bdata共用指针空间，没有发生复制
	check(BytesPointer(bdata) == StringPointer(sdata), "test")

	// sdata保持和原byte相同的内容
	check(sdata == "hello", "test")

	// bdata变更，string随之一起变更
	bdata[1] = 'a'
	check(sdata == "hallo", "test")

	// bdata变长，string并不会随之一起变长
	bdata = append(bdata, []byte(" world")...)
	check(sdata == "hallo", "test")

	// 重新赋值后，sdata发生相应变化
	sdata = Bytes2String(bdata)
	check(sdata == "hallo world", "test")

	check(true, "test")
}

func TestString2Bytes(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	// const_sdata的字符串在代码段，而不在堆上，强行使用unsafe手段进行修改必然引起程序崩溃
	var const_sdata string = "hello"
	fmt.Println(StringPointer(const_sdata))

	// fmt.Sprint()生成的字符串在堆上，可以使用unsafe手段进行修改
	var sdata string = fmt.Sprint("hello")
	var bdata []byte = String2Bytes(sdata)

	fmt.Println("sdata: ", StringPointer(sdata))
	// sdata与bdata共用指针空间，没有发生复制
	check(BytesPointer(bdata) == StringPointer(sdata), "test")

	// bdata变更，string随之一起变更
	bdata[1] = 'a'
	check(sdata == "hallo", "test")

	// bdata变长，string并不会随之一起变长
	bdata = append(bdata, []byte(" world")...)
	check(sdata == "hallo", "test")
	// bdata变长之后，发生内存拷贝，bdata数据指针发生变化
	check(BytesPointer(bdata) != StringPointer(sdata), "test")

	// 重新赋值后，sdata发生相应变化
	sdata = Bytes2String(bdata)
	check(sdata == "hallo world", "test")

	check(true, "test")
}
