/*
// =====================================================================================
//
//       Filename:  JsonObj.go
//
//    Description:
//
//        Version:  1.0
//        Created:  06/23/2018 12:19:31 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package whatson

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"inspector/util/unsafe"
)

var jsonData string = `
{
	"map": {
		"i" : 1024,
		"f" : 3.14,
		"bt" : true,
		"bf" : false,
		"n" : null,
		"o" : {
			"s1" : "s1",
			"s2" : "s2",
			"s3" : "s3",
			"s4" : "s4"
		},
		"l" : [1, 2, 3, 4, 5]
	},
	"list" : [
		1024, 3.14, true, false, null, {
			"s1" : "s1",
			"s2" : "s2",
			"s3" : "s3",
			"s4" : "s4"	
		},
		[1, 2, 3, 4, 5]
	]
}
`

var jsonIterLargeFile string = `
[{
	"person": {
		"id": "d50887ca-a6ce-4e59-b89f-14f0b5d03b03",
		"name": {
			"fullName": "Leonid Bugaev",
			"givenName": "Leonid",
			"familyName": "Bugaev"
		},
		"email": "leonsbox@gmail.com",
		"gender": "male",
		"location": "Saint Petersburg, Saint Petersburg, RU",
		"geo": {
			"city": "Saint Petersburg",
			"state": "Saint Petersburg",
			"country": "Russia",
			"lat": 59.9342802,
			"lng": 30.3350986
		},
		"bio": "Senior engineer at Granify.com",
		"site": "http://flickfaver.com",
		"avatar": "https://d1ts43dypk8bqh.cloudfront.net/v1/avatars/d50887ca-a6ce-4e59-b89f-14f0b5d03b03",
		"employment": {
			"name": "www.latera.ru",
			"title": "Software Engineer",
			"domain": "gmail.com"
		},
		"facebook": {
			"handle": "leonid.bugaev"
		},
		"github": {
			"handle": "buger",
			"id": 14009,
			"avatar": "https://avatars.githubusercontent.com/u/14009?v=3",
			"company": "Granify",
			"blog": "http://leonsbox.com",
			"followers": 95,
			"following": 10
		},
		"twitter": {
			"handle": "flickfaver",
			"id": 77004410,
			"bio": null,
			"followers": 2,
			"following": 1,
			"statuses": 5,
			"favorites": 0,
			"location": "",
			"site": "http://flickfaver.com",
			"avatar": null
		},
		"linkedin": {
			"handle": "in/leonidbugaev"
		},
		"googleplus": {
			"handle": null
		},
		"angellist": {
			"handle": "leonid-bugaev",
			"id": 61541,
			"bio": "Senior engineer at Granify.com",
			"blog": "http://buger.github.com",
			"site": "http://buger.github.com",
			"followers": 41,
			"avatar": "https://d1qb2nb5cznatu.cloudfront.net/users/61541-medium_jpg?1405474390"
		},
		"klout": {
			"handle": null,
			"score": null
		},
		"foursquare": {
			"handle": null
		},
		"aboutme": {
			"handle": "leonid.bugaev",
			"bio": null,
			"avatar": null
		},
		"gravatar": {
			"handle": "buger",
			"urls": [
			],
			"avatar": "http://1.gravatar.com/avatar/f7c8edd577d13b8930d5522f28123510",
			"avatars": [
			{
				"url": "http://1.gravatar.com/avatar/f7c8edd577d13b8930d5522f28123510",
				"type": "thumbnail"
			}
			]
		},
		"fuzzy": false
	},
	"company": "hello"
}]
`

func TestParse(t *testing.T) {
	parser := NewParser("json")
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var err error
	var buf strings.Builder
	var comma bool
	buf.WriteRune('{')
	err = parser.Parse([]byte(jsonData), func(keyPath []string, value []byte, valueType ValueType) error {
		switch valueType {
		case STRING:
			if comma == true {
				buf.WriteRune(',')
			}
			if keyPath[len(keyPath)-1][0:1] != "[" {
				buf.WriteRune('"')
				buf.WriteString(keyPath[len(keyPath)-1])
				buf.WriteRune('"')
				buf.WriteRune(':')
			}
			buf.WriteRune('"')
			buf.Write(value)
			buf.WriteRune('"')
			comma = true
		case INTEGER:
			fallthrough
		case FLOAT:
			fallthrough
		case BOOL:
			fallthrough
		case NULL:
			if comma == true {
				buf.WriteRune(',')
			}
			if keyPath[len(keyPath)-1][0:1] != "[" {
				buf.WriteRune('"')
				buf.WriteString(keyPath[len(keyPath)-1])
				buf.WriteRune('"')
				buf.WriteRune(':')
			}
			buf.Write(value)
			comma = true
		case OBJECT:
			if value == nil { // OBJECT 开始
				if comma == true {
					buf.WriteRune(',')
				}
				if keyPath[len(keyPath)-1][0:1] != "[" {
					buf.WriteRune('"')
					buf.WriteString(keyPath[len(keyPath)-1])
					buf.WriteRune('"')
					buf.WriteRune(':')
				}
				buf.WriteRune('{')
				comma = false
			} else { // OBJECT 结束
				buf.WriteRune('}')
				comma = true
			}
		case ARRAY:
			if value == nil { // ARRAY 开始
				if comma == true {
					buf.WriteRune(',')
				}
				if keyPath[len(keyPath)-1][0:1] != "[" {
					buf.WriteRune('"')
					buf.WriteString(keyPath[len(keyPath)-1])
					buf.WriteRune('"')
					buf.WriteRune(':')
				}
				buf.WriteRune('[')
				comma = false
			} else { // ARRAY 结束
				buf.WriteRune(']')
				comma = true
			}
		}
		return nil
	})
	buf.WriteRune('}')
	var jsonDataTrim string = jsonData
	jsonDataTrim = strings.Replace(jsonDataTrim, " ", "", -1)
	jsonDataTrim = strings.Replace(jsonDataTrim, "\t", "", -1)
	jsonDataTrim = strings.Replace(jsonDataTrim, "\n", "", -1)
	check(jsonDataTrim == buf.String(), "test")
	check(err == nil, "test")
	check(true, "test")
}

func TestJsonParseJsonIterLargeFile(t *testing.T) {
	parser := NewParser("json")
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var err error
	var buf strings.Builder
	var comma bool
	buf.WriteRune('[')
	err = parser.Parse([]byte(jsonIterLargeFile), func(keyPath []string, value []byte, valueType ValueType) error {
		switch valueType {
		case STRING:
			if comma == true {
				buf.WriteRune(',')
			}
			if keyPath[len(keyPath)-1][0:1] != "[" {
				buf.WriteRune('"')
				buf.WriteString(keyPath[len(keyPath)-1])
				buf.WriteRune('"')
				buf.WriteRune(':')
			}
			buf.WriteRune('"')
			buf.Write(value)
			buf.WriteRune('"')
			comma = true
		case INTEGER:
			fallthrough
		case FLOAT:
			fallthrough
		case BOOL:
			fallthrough
		case NULL:
			if comma == true {
				buf.WriteRune(',')
			}
			if keyPath[len(keyPath)-1][0:1] != "[" {
				buf.WriteRune('"')
				buf.WriteString(keyPath[len(keyPath)-1])
				buf.WriteRune('"')
				buf.WriteRune(':')
			}
			buf.Write(value)
			comma = true
		case OBJECT:
			if value == nil { // OBJECT 开始
				if comma == true {
					buf.WriteRune(',')
				}
				if keyPath[len(keyPath)-1][0:1] != "[" {
					buf.WriteRune('"')
					buf.WriteString(keyPath[len(keyPath)-1])
					buf.WriteRune('"')
					buf.WriteRune(':')
				}
				buf.WriteRune('{')
				comma = false
			} else { // OBJECT 结束
				buf.WriteRune('}')
				comma = true
			}
		case ARRAY:
			if value == nil { // ARRAY 开始
				if comma == true {
					buf.WriteRune(',')
				}
				if keyPath[len(keyPath)-1][0:1] != "[" {
					buf.WriteRune('"')
					buf.WriteString(keyPath[len(keyPath)-1])
					buf.WriteRune('"')
					buf.WriteRune(':')
				}
				buf.WriteRune('[')
				comma = false
			} else { // ARRAY 结束
				buf.WriteRune(']')
				comma = true
			}
		}
		return nil
	})
	buf.WriteRune(']')
	var jsonIterLargeFileTrim string = jsonIterLargeFile
	jsonIterLargeFileTrim = strings.Replace(jsonIterLargeFileTrim, " ", "", -1)
	jsonIterLargeFileTrim = strings.Replace(jsonIterLargeFileTrim, "\t", "", -1)
	jsonIterLargeFileTrim = strings.Replace(jsonIterLargeFileTrim, "\n", "", -1)
	checkString := buf.String()
	checkString = strings.Replace(checkString, " ", "", -1)
	checkString = strings.Replace(checkString, "\t", "", -1)
	checkString = strings.Replace(checkString, "\n", "", -1)
	check(jsonIterLargeFileTrim == checkString, "test")
	check(err == nil, "test")
	check(true, "test")
}

func TestJsonParseInvalid(t *testing.T) {
	return
	parser := NewParser("json")
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var err error

	err = parser.Parse([]byte(` \t\n`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`a`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{ `), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{*`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{}{}`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{{}}`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{{},{}}`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a"`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a";`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a":`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a":"1"`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a":1`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a":tru`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a":trues`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a":True`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a":TRUE`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a":fal`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a":falses`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a":False`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a":FALSE`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a":nul`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a":nulls`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a":Null`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a":NULL`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a":c`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a":0,`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a":0,}`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a":0,"b":{`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`{"a":[]`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`[`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`[ `), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`["`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`["a`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`["a"`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`["a":`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`["a",`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`["a",{`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	err = parser.Parse([]byte(`["a",{}`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err != nil, "test")

	check(true, "test")
}

func TestJsonParseValidEmpty(t *testing.T) {
	return
	parser := NewParser("json")
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var err error

	err = parser.Parse([]byte(`{}`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err == nil, "test")

	err = parser.Parse([]byte(`{"a":[], "b":{}}`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err == nil, "test")

	err = parser.Parse([]byte(`[]`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err == nil, "test")

	err = parser.Parse([]byte(`[[], [] ,[]]`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err == nil, "test")

	err = parser.Parse([]byte(`[{}, [], {}]`), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	check(err == nil, "test")

	check(true, "test")
}

func TestGet(t *testing.T) {
	parser := NewParser("json")
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}

	var jsonDataTrim string = jsonData
	jsonDataTrim = strings.Replace(jsonDataTrim, " ", "", -1)
	jsonDataTrim = strings.Replace(jsonDataTrim, "\t", "", -1)
	jsonDataTrim = strings.Replace(jsonDataTrim, "\n", "", -1)

	var value []byte
	var err error

	value, err = parser.Get([]byte(jsonDataTrim))
	check(err == nil, "test")
	check(string(value) == jsonDataTrim, "test")

	value, err = parser.Get([]byte(jsonDataTrim), "map")
	check(err == nil, "test")
	check(string(value) == "{\"i\":1024,\"f\":3.14,\"bt\":true,\"bf\":false,\"n\":null,\"o\":{\"s1\":\"s1\",\"s2\":\"s2\",\"s3\":\"s3\",\"s4\":\"s4\"},\"l\":[1,2,3,4,5]}", "test")

	value, err = parser.Get([]byte(jsonDataTrim), "map", "i")
	check(err == nil, "test")
	check(string(value) == "1024", "test")

	value, err = parser.Get([]byte(jsonDataTrim), "list")
	check(err == nil, "test")
	check(string(value) == "[1024,3.14,true,false,null,{\"s1\":\"s1\",\"s2\":\"s2\",\"s3\":\"s3\",\"s4\":\"s4\"},[1,2,3,4,5]]", "test")

	value, err = parser.Get([]byte(jsonDataTrim), "list", "[0]")
	check(err == nil, "test")
	check(string(value) == "1024", "test")

	check(true, "test")
}

func BenchmarkJsonParse(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	parser := NewParser("json")
	for i := 0; i < b.N; i++ {
		parser.Parse(unsafe.String2Bytes(jsonData), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	}
}

func BenchmarkJsonParseJsonIterLargeFile(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	parser := NewParser("json")
	for i := 0; i < b.N; i++ {
		parser.Parse(unsafe.String2Bytes(jsonIterLargeFile), func(keyPath []string, value []byte, valueType ValueType) error { return nil })
	}
}
