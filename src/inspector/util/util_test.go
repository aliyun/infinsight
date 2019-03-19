/*
// =====================================================================================
//
//       Filename:  util_test.go
//
//    Description:  为util目录下的小工具进行测试，并不能算是一般意义上的单元测试
//
//        Version:  1.0
//        Created:  07/16/2018 07:43:47 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package util

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"bytes"
)

func TestMD5(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	fmt.Println(Md5([]byte("hello world")))
	fmt.Println(Md5In64([]byte("hello world")))
	for x := 1; x < 10; x++ {
		for i := 0; i < 20; i++ {
			fmt.Printf("%d\t", ConsistentHash([]byte(fmt.Sprintf("%d", i)), x))
		}
		fmt.Println("")
	}
	check(true, "test")
}

func TestIntStringConvert(t *testing.T) {
	testArr := []int{0, 1, 10, 100, 123456, 123456789}
	for _, x := range testArr {
		s := RepInt2String(x)
		ret, err := RepString2Int(s)
		assert.Equal(t, err, nil, "should be nil")
		assert.Equal(t, ret, x, "should be nil")
	}
	s := RepInt2String(65)
	assert.Equal(t, "=", s, "should be nil")
	s = RepInt2String(66)
	assert.Equal(t, "10", s, "should be nil")
	s = RepInt2String(4356)
	assert.Equal(t, "100", s, "should be nil")
	s = RepInt2String(0)
	assert.Equal(t, "0", s, "should be nil")
	s = RepInt2String(-1)
	assert.Equal(t, "", s, "should be nil")
	s = RepInt2String(750)
	assert.Equal(t, string(representation[750 / len(representation)]) + string(representation[750 % len(representation)]),
		s, "should be nil")

	_, err := RepString2Int("")
	assert.NotEqual(t, err, nil, "should be nil")

	ret, err := RepString2Int("9")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, ret, 9, "should be nil")

	ret, err = RepString2Int("a")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, ret, 10, "should be nil")

	ret, err = RepString2Int("aa")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, ret, 670, "should be nil")

	ret, err = RepString2Int("aaa")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, ret, 44230, "should be nil")

	ret, err = RepString2Int("aaaa")
	assert.Equal(t, err, nil, "should be nil")
	assert.Equal(t, ret, 2919190, "should be nil")

	fmt.Println(RepString2Int("2S"))
}

func TestStack(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var stack = NewStack()
	check(stack.Size() == 0, "test")
	check(stack.Pop() == nil, "test")
	stack.Push(1)
	check(stack.Size() == 1, "test")
	stack.Push(2)
	check(stack.Size() == 2, "test")
	stack.Push(3)
	check(stack.Size() == 3, "test")
	check(stack.Top().(int) == 3, "test")
	check(stack.Size() == 3, "test")
	check(stack.Pop().(int) == 3, "test")
	check(stack.Size() == 2, "test")
	check(stack.Pop().(int) == 2, "test")
	check(stack.Size() == 1, "test")
	check(stack.Pop().(int) == 1, "test")
	check(stack.Size() == 0, "test")
	check(stack.Pop() == nil, "test")

	check(true, "test")
}

func TestString(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	check(StringLTrim(" \t\nabc") == "abc", "test")
	check(StringRTrim("abc \t\n") == "abc", "test")
	check(StringTrim(" \t\nabc \t\n") == "abc", "test")
	check(StringReverse("abc") == "cba", "test")
	check(StringReverse("abcd") == "dcba", "test")

	check(true, "test")
}

func TestNet(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	addrs, _ := GetAllNetAddr()
	fmt.Println("localhost: ", addrs)

	check(true, "test")
}

func TestMath(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var r = IntReverse(123)
	check(r == 321, "test")
	var r32 = Int32Reverse(123)
	check(r32 == 321, "test")

	check(true, "test")
}

func TestTmp(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	fmt.Println("test tmp: ", RepInt2String(193))

	check(true, "test")
}

// only for print
func TestDecryptCfb(t *testing.T) {
	input := []byte("d1Csp9JPhkygftMACLoT")
	output, err := DecryptCfb(Base64Decode(input))
	assert.Equal(t, nil, err, "should be nil")
	fmt.Printf("decode: %s\n", string(output))
}

func TestGCD(t *testing.T) {
	assert.Equal(t, int64(5), GCD(10, 5), "should be nil")
	assert.Equal(t, int64(5), GCD(10, -5), "should be nil")
	assert.Equal(t, int64(5), GCD(-10, 5), "should be nil")
	assert.Equal(t, int64(5), GCD(-10, -5), "should be nil")
	assert.Equal(t, int64(10), GCD(10, -10), "should be nil")
	assert.Equal(t, int64(1), GCD(1, 1), "should be nil")
	assert.Equal(t, int64(1), GCD(1, -1), "should be nil")
	assert.Equal(t, int64(1), GCD(111, -223), "should be nil")
	assert.Equal(t, int64(1), GCD(1, 0), "should be nil")
	assert.Equal(t, int64(1), GCD(-1, 0), "should be nil")
	assert.Equal(t, int64(0), GCD(0, 0), "should be nil")
}

func TestBackTracking(t *testing.T) {
	var nr int
	{
		nr++
		fmt.Printf("TestBackTracking case %d.\n", nr)
		byteBuffer := bytes.NewBufferString("abc|hello|world")
		BackTracking(byteBuffer, 1, byte('|'))
		assert.Equal(t, "abc|hello", byteBuffer.String(), "should be equal")
	}

	{
		nr++
		fmt.Printf("TestBackTracking case %d.\n", nr)
		byteBuffer := bytes.NewBufferString("abc|hello|world")
		BackTracking(byteBuffer, 0, byte('|'))
		assert.Equal(t, "abc|hello|world", byteBuffer.String(), "should be equal")
	}

	{
		nr++
		fmt.Printf("TestBackTracking case %d.\n", nr)
		byteBuffer := bytes.NewBufferString("abc|hello|world")
		BackTracking(byteBuffer, 2, byte('|'))
		assert.Equal(t, "abc", byteBuffer.String(), "should be equal")
	}

	{
		nr++
		fmt.Printf("TestBackTracking case %d.\n", nr)
		byteBuffer := bytes.NewBufferString("abc|hello|world")
		BackTracking(byteBuffer, 3, byte('|'))
		assert.Equal(t, "", byteBuffer.String(), "should be equal")
	}

	{
		nr++
		fmt.Printf("TestBackTracking case %d.\n", nr)
		byteBuffer := bytes.NewBufferString("abc|hello|world")
		BackTracking(byteBuffer, 10, byte('|'))
		assert.Equal(t, "", byteBuffer.String(), "should be equal")
	}
}
