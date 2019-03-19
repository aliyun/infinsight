package mongoSteps

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
	"inspector/collector_server/model"
	"inspector/dict_server"
	"inspector/util"
	"inspector/util/whatson"
	"testing"
	"time"
)

const (
	jobNameParse       = "redis_1_0"
	mongoAddressParse  = "100.81.245.155:20112" // config server
	mongoUsernameParse = "admin"
	mongoPasswordParse = "admin"
	instanceNameParse  = "test-instance-ooo"
)

type ParseParameter struct {
	Ds *dictServer.DictServer // dict server map string <-> int
}

func NewParseParameter() *ParseParameter {
	dictConf := &dictServer.Conf{
		Address:    mongoAddressParse,
		Username:   mongoUsernameParse,
		Password:   mongoPasswordParse,
		ServerType: jobNameParse,
		DB:         "step_parse_test",
	}
	ds := dictServer.NewDictServer(dictConf, nil)
	if ds == nil {
		glog.Error("create dict server error")
		return nil
	}

	return &ParseParameter{
		Ds: ds,
	}
}

func TestParse(t *testing.T) {
	//flag.Set("stderrthreshold", "info")
	//flag.Set("v", "2")

	var nr int

	p := NewParseParameter()
	assert.NotEqual(t, nil, p, "should be equal")

	sp := NewStepParse("1.2.3.5", jobNameParse, &model.Instance{Addr: instanceNameParse},
		whatson.NewParser(whatson.Bson), p.Ds)

	arrayKeys = []string{"k2"}
	{
		nr++
		fmt.Printf("TestParse case %d.\n", nr)
		s := []string{
			"age",
			"other" + "|" + "p" + "|" + "v",
			"other" + "|" + "p2",
			"map" + "|" + "o" + "|" + "s5" + "|" + "s6" + "|" + "s7",
		}
		for {
			time.Sleep(1000 * time.Millisecond)

			all := true
			for i := 0; i < len(s); i++ {
				if _, err := sp.Ds.GetValue(s[i]); err != nil {
					all = false
					break
				}
			}

			if all {
				break
			}
		}

		// { "_id" : ObjectId("5b42e23937e3c89dc97b4f05"), "name" : "v1", "age" : 15.0, "other" : { "p" : { "v" : 5.0 }, "p2" : 6.0 } }
		// { "_id" : ObjectId("5bd95e9dcd3c94b94724ddcd"), "map" : { "i" : 1024, "f" : 3.14, "bt" : true, "bf" : false, "n" : 999, "o" : { "s1" : "s1", "s2" : "s2", "s3" : "s3", "s4" : "s4", "s5" : { "s6" : { "s7" : true } } } } }
		data := [][]byte{
			{91, 0, 0, 0, 7, 95, 105, 100, 0, 91, 66, 226, 57, 55, 227, 200, 157, 201, 123, 79, 5, 2, 110, 97, 109, 101, 0, 3, 0, 0, 0, 118, 49, 0, 1, 97, 103, 101, 0, 0, 0, 0, 0, 0, 0, 46, 64, 3, 111, 116, 104, 101, 114, 0, 36, 0, 0, 0, 3, 112, 0, 16, 0, 0, 0, 1, 118, 0, 0, 0, 0, 0, 0, 0, 20, 64, 0, 1, 112, 50, 0, 0, 0, 0, 0, 0, 0, 24, 64, 0, 0},
			{150, 0, 0, 0, 7, 95, 105, 100, 0, 91, 217, 94, 157, 205, 60, 148, 185, 71, 36, 221, 205, 3, 109, 97, 112, 0, 123, 0, 0, 0, 1, 105, 0, 0, 0, 0, 0, 0, 0, 144, 64, 1, 102, 0, 31, 133, 235, 81, 184, 30, 9, 64, 8, 98, 116, 0, 1, 8, 98, 102, 0, 0, 1, 110, 0, 0, 0, 0, 0, 0, 56, 143, 64, 3, 111, 0, 72, 0, 0, 0, 2, 115, 49, 0, 3, 0, 0, 0, 115, 49, 0, 2, 115, 50, 0, 3, 0, 0, 0, 115, 50, 0, 2, 115, 51, 0, 3, 0, 0, 0, 115, 51, 0, 2, 115, 52, 0, 3, 0, 0, 0, 115, 52, 0, 3, 115, 53, 0, 19, 0, 0, 0, 3, 115, 54, 0, 10, 0, 0, 0, 8, 115, 55, 0, 1, 0, 0, 0, 0, 0},
		}

		output, err := sp.DoStep(data)
		assert.Equal(t, nil, err, "should be equal")
		mp := output.(map[int]interface{})
		// fmt.Println(mp)

		v1, err := sp.Ds.GetValueOnly(s[0])
		assert.Equal(t, nil, err, "should be equal")
		valInt1, err := util.RepString2Int(v1)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 15.0, mp[valInt1], "should be equal")

		v2, err := sp.Ds.GetValueOnly(s[1])
		assert.Equal(t, nil, err, "should be equal")
		valInt2, err := util.RepString2Int(v2)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 5.0, mp[valInt2], "should be equal")

		v3, err := sp.Ds.GetValueOnly(s[2])
		assert.Equal(t, nil, err, "should be equal")
		valInt3, err := util.RepString2Int(v3)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 6.0, mp[valInt3], "should be equal")

		v4, err := sp.Ds.GetValueOnly(s[3])
		assert.Equal(t, nil, err, "should be equal")
		valInt4, err := util.RepString2Int(v4)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, mp[valInt4], "should be equal")
	}

	// test array
	{
		nr++
		fmt.Printf("TestParse case %d.\n", nr)
		s1 := "aaa|s|k1"
		sp.Ds.GetValue(s1)
		time.Sleep((dictServer.HandlerInterval + 10000) * time.Millisecond)

		// { "_id" : ObjectId("5bd9682aba1dca37b0bb0b6c"), "aaa" : [ { "k1" : 1, "k2" : "s" } ] }
		data := [][]byte{
			{62, 0, 0, 0, 7, 95, 105, 100, 0, 91, 217, 104, 42, 186, 29, 202, 55, 176, 187, 11, 108, 4, 97, 97, 97, 0, 35, 0, 0, 0, 3, 48, 0, 27, 0, 0, 0, 1, 107, 49, 0, 0, 0, 0, 0, 0, 0, 240, 63, 2, 107, 50, 0, 2, 0, 0, 0, 115, 0, 0, 0, 0},
		}

		output, err := sp.DoStep(data)
		assert.Equal(t, nil, err, "should be equal")
		assert.NotEqual(t, nil, output, "should be equal")
		mp := output.(map[int]interface{})

		v1, err := sp.Ds.GetValueOnly(s1)
		assert.Equal(t, nil, err, "should be equal")
		valInt1, err := util.RepString2Int(v1)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(1), mp[valInt1], "should be equal")
	}

	// test array again
	{
		nr++
		fmt.Printf("TestParse case %d.\n", nr)
		s := []string{
			"aaa|s|k1",
			"aaa|s|k3",
			"aaa|s|k4",
			"aaa|s|k5",
			"aaa|p|m1",
			"aaa|q|m2",
			"aaa|q|ooo|xxx",
			"bbb|5|x",
			"bbb|3|y",
			"bbb|-10|cv",
		}
		for {
			time.Sleep(1000 * time.Millisecond)

			all := true
			for i := 0; i < len(s); i++ {
				if _, err := sp.Ds.GetValue(s[i]); err != nil {
					all = false
					break
				}
			}

			if all {
				break
			}
		}

		// { "_id" : ObjectId("5bd97000dc279a455c549fbe"), "aaa" : [ { "k1" : 1, "k2" : "s", "k3" : 5, "k4" : true, "k5" : -10 } ] }
		// { "_id" : ObjectId("5bd9716adc279a455c549fc0"), "aaa" : [ { "k1" : 1, "k2" : "s", "k3" : 5, "k4" : true, "k5" : -10 }, { "k2" : "p", "m1" : 19 }, { "k2" : "q", "m2" : -10000, "ooo" : { "xxx" : -45 } } ] }
		// { "_id" : ObjectId("5bd9716adc279a455c549fc0"), "aaa" : [ { "k1" : 1, "k2" : "s", "k3" : 5, "k4" : true, "k5" : -10 }, { "k2" : "p", "m1" : 19 }, { "k2" : "q", "m2" : -10000, "ooo" : { "xxx" : -45 } } ], "bbb" : [ { "k2" : 5, "x" : true }, { "k2" : 3, "y" : false }, { "k2" : -10, "cv" : 99999 } ] }
		data := [][]byte{
			{91, 0, 0, 0, 7, 95, 105, 100, 0, 91, 217, 112, 0, 220, 39, 154, 69, 92, 84, 159, 190, 4, 97, 97, 97, 0, 64, 0, 0, 0, 3, 48, 0, 56, 0, 0, 0, 1, 107, 49, 0, 0, 0, 0, 0, 0, 0, 240, 63, 2, 107, 50, 0, 2, 0, 0, 0, 115, 0, 1, 107, 51, 0, 0, 0, 0, 0, 0, 0, 20, 64, 8, 107, 52, 0, 1, 1, 107, 53, 0, 0, 0, 0, 0, 0, 0, 36, 192, 0, 0, 0},
			{174, 0, 0, 0, 7, 95, 105, 100, 0, 91, 217, 113, 106, 220, 39, 154, 69, 92, 84, 159, 192, 4, 97, 97, 97, 0, 147, 0, 0, 0, 3, 48, 0, 56, 0, 0, 0, 1, 107, 49, 0, 0, 0, 0, 0, 0, 0, 240, 63, 2, 107, 50, 0, 2, 0, 0, 0, 115, 0, 1, 107, 51, 0, 0, 0, 0, 0, 0, 0, 20, 64, 8, 107, 52, 0, 1, 1, 107, 53, 0, 0, 0, 0, 0, 0, 0, 36, 192, 0, 3, 49, 0, 27, 0, 0, 0, 2, 107, 50, 0, 2, 0, 0, 0, 112, 0, 1, 109, 49, 0, 0, 0, 0, 0, 0, 0, 51, 64, 0, 3, 50, 0, 50, 0, 0, 0, 2, 107, 50, 0, 2, 0, 0, 0, 113, 0, 1, 109, 50, 0, 0, 0, 0, 0, 0, 136, 195, 192, 3, 111, 111, 111, 0, 18, 0, 0, 0, 1, 120, 120, 120, 0, 0, 0, 0, 0, 0, 128, 70, 192, 0, 0, 0, 0},
			{8, 1, 0, 0, 7, 95, 105, 100, 0, 91, 217, 113, 106, 220, 39, 154, 69, 92, 84, 159, 192, 4, 97, 97, 97, 0, 147, 0, 0, 0, 3, 48, 0, 56, 0, 0, 0, 1, 107, 49, 0, 0, 0, 0, 0, 0, 0, 240, 63, 2, 107, 50, 0, 2, 0, 0, 0, 115, 0, 1, 107, 51, 0, 0, 0, 0, 0, 0, 0, 20, 64, 8, 107, 52, 0, 1, 1, 107, 53, 0, 0, 0, 0, 0, 0, 0, 36, 192, 0, 3, 49, 0, 27, 0, 0, 0, 2, 107, 50, 0, 2, 0, 0, 0, 112, 0, 1, 109, 49, 0, 0, 0, 0, 0, 0, 0, 51, 64, 0, 3, 50, 0, 50, 0, 0, 0, 2, 107, 50, 0, 2, 0, 0, 0, 113, 0, 1, 109, 50, 0, 0, 0, 0, 0, 0, 136, 195, 192, 3, 111, 111, 111, 0, 18, 0, 0, 0, 1, 120, 120, 120, 0, 0, 0, 0, 0, 0, 128, 70, 192, 0, 0, 0, 4, 98, 98, 98, 0, 85, 0, 0, 0, 3, 48, 0, 21, 0, 0, 0, 1, 107, 50, 0, 0, 0, 0, 0, 0, 0, 20, 64, 8, 120, 0, 1, 0, 3, 49, 0, 21, 0, 0, 0, 1, 107, 50, 0, 0, 0, 0, 0, 0, 0, 8, 64, 8, 121, 0, 0, 0, 3, 50, 0, 29, 0, 0, 0, 1, 107, 50, 0, 0, 0, 0, 0, 0, 0, 36, 192, 1, 99, 118, 0, 0, 0, 0, 0, 240, 105, 248, 64, 0, 0, 0},
		}

		output, err := sp.DoStep(data)
		assert.Equal(t, nil, err, "should be equal")
		assert.NotEqual(t, nil, output, "should be equal")
		mp := output.(map[int]interface{})

		v1, err := sp.Ds.GetValueOnly(s[0])
		assert.Equal(t, nil, err, "should be equal")
		valInt1, err := util.RepString2Int(v1)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(1), mp[valInt1], "should be equal")

		v3, err := sp.Ds.GetValueOnly(s[1])
		assert.Equal(t, nil, err, "should be equal")
		valInt3, err := util.RepString2Int(v3)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(5), mp[valInt3], "should be equal")

		v4, err := sp.Ds.GetValueOnly(s[2])
		assert.Equal(t, nil, err, "should be equal")
		valInt4, err := util.RepString2Int(v4)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, mp[valInt4], "should be equal")

		v5, err := sp.Ds.GetValueOnly(s[3])
		assert.Equal(t, nil, err, "should be equal")
		valInt5, err := util.RepString2Int(v5)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(-10), mp[valInt5], "should be equal")

		v6, err := sp.Ds.GetValueOnly(s[4])
		assert.Equal(t, nil, err, "should be equal")
		valInt6, err := util.RepString2Int(v6)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(19), mp[valInt6], "should be equal")

		v7, err := sp.Ds.GetValueOnly(s[5])
		assert.Equal(t, nil, err, "should be equal")
		valInt7, err := util.RepString2Int(v7)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(-10000), mp[valInt7], "should be equal")

		v8, err := sp.Ds.GetValueOnly(s[6])
		assert.Equal(t, nil, err, "should be equal")
		valInt8, err := util.RepString2Int(v8)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(-45), mp[valInt8], "should be equal")

		v9, err := sp.Ds.GetValueOnly(s[7])
		assert.Equal(t, nil, err, "should be equal")
		valInt9, err := util.RepString2Int(v9)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, mp[valInt9], "should be equal")

		v10, err := sp.Ds.GetValueOnly(s[8])
		assert.Equal(t, nil, err, "should be equal")
		valInt10, err := util.RepString2Int(v10)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, false, mp[valInt10], "should be equal")

		v11, err := sp.Ds.GetValueOnly(s[9])
		assert.Equal(t, nil, err, "should be equal")
		valInt11, err := util.RepString2Int(v11)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(99999), mp[valInt11], "should be equal")

	}

	// test array again
	{
		nr++
		fmt.Printf("TestParse case %d.\n", nr)
		s := []string{
			"xyz|lp",
			"xyz|i1|i2",
			"xyz|i1|i3",
			"xyz|i1|i4|ffffff",
			"xyz|i1|i4|qqqq",
			"xyz|i1|i4|p|4|l1",
			"xyz|i1|i4|p|-4|l2",
			"xyz|i1|i4|p|-4|k2",
		}
		for {
			time.Sleep(1000 * time.Millisecond)

			all := true
			for i := 0; i < len(s); i++ {
				if _, err := sp.Ds.GetValue(s[i]); err != nil {
					all = false
					break
				}
			}

			if all {
				break
			}
		}

		// { "_id" : ObjectId("5bd991c5dc279a455c549fc1"), "xyz" : { "i1" : { "i2" : 0, "i3" : 1, "i4" : { "ffffff" : 100, "qqqq" : 101, "p" : [ { "k2" : 4, "l1" : 5 }, { "k2" : -4, "l2" : 10 } ] } }, "lp" : 21 } }
		data := [][]byte{
			{188, 0, 0, 0, 7, 95, 105, 100, 0, 91, 217, 145, 197, 220, 39, 154, 69, 92, 84, 159, 193, 3, 120, 121, 122, 0, 161, 0, 0, 0, 3, 105, 49, 0, 140, 0, 0, 0, 1, 105, 50, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 105, 51, 0, 0, 0, 0, 0, 0, 0, 240, 63, 3, 105, 52, 0, 107, 0, 0, 0, 1, 102, 102, 102, 102, 102, 102, 0, 0, 0, 0, 0, 0, 0, 89, 64, 1, 113, 113, 113, 113, 0, 0, 0, 0, 0, 0, 64, 89, 64, 4, 112, 0, 69, 0, 0, 0, 3, 48, 0, 29, 0, 0, 0, 1, 107, 50, 0, 0, 0, 0, 0, 0, 0, 16, 64, 1, 108, 49, 0, 0, 0, 0, 0, 0, 0, 20, 64, 0, 3, 49, 0, 29, 0, 0, 0, 1, 107, 50, 0, 0, 0, 0, 0, 0, 0, 16, 192, 1, 108, 50, 0, 0, 0, 0, 0, 0, 0, 36, 64, 0, 0, 0, 0, 1, 108, 112, 0, 0, 0, 0, 0, 0, 0, 53, 64, 0, 0},
		}

		output, err := sp.DoStep(data)
		assert.Equal(t, nil, err, "should be equal")
		assert.NotEqual(t, nil, output, "should be equal")
		mp := output.(map[int]interface{})

		v1, err := sp.Ds.GetValueOnly(s[0])
		assert.Equal(t, nil, err, "should be equal")
		valInt1, err := util.RepString2Int(v1)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(21), mp[valInt1], "should be equal")

		v2, err := sp.Ds.GetValueOnly(s[1])
		assert.Equal(t, nil, err, "should be equal")
		valInt2, err := util.RepString2Int(v2)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(0), mp[valInt2], "should be equal")

		v3, err := sp.Ds.GetValueOnly(s[2])
		assert.Equal(t, nil, err, "should be equal")
		valInt3, err := util.RepString2Int(v3)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(1), mp[valInt3], "should be equal")

		v4, err := sp.Ds.GetValueOnly(s[3])
		assert.Equal(t, nil, err, "should be equal")
		valInt4, err := util.RepString2Int(v4)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(100), mp[valInt4], "should be equal")

		v5, err := sp.Ds.GetValueOnly(s[4])
		assert.Equal(t, nil, err, "should be equal")
		valInt5, err := util.RepString2Int(v5)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(101), mp[valInt5], "should be equal")

		v6, err := sp.Ds.GetValueOnly(s[5])
		assert.Equal(t, nil, err, "should be equal")
		valInt6, err := util.RepString2Int(v6)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(5), mp[valInt6], "should be equal")

		v7, err := sp.Ds.GetValueOnly(s[6])
		assert.Equal(t, nil, err, "should be equal")
		valInt7, err := util.RepString2Int(v7)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(10), mp[valInt7], "should be equal")

		v8, err := sp.Ds.GetValueOnly(s[7])
		assert.Equal(t, nil, err, "should be equal")
		valInt8, err := util.RepString2Int(v8)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(-4), mp[valInt8], "should be equal")
	}

	// test repeat key
	{
		nr++
		fmt.Printf("TestParse case %d.\n", nr)
		s := []string{
			"xyz|1|k1",
		}
		for {
			time.Sleep(1000 * time.Millisecond)

			all := true
			for i := 0; i < len(s); i++ {
				if _, err := sp.Ds.GetValue(s[i]); err != nil {
					all = false
					break
				}
			}

			if all {
				break
			}
		}

		// { "_id" : ObjectId("5bd9bb3ddc279a455c549fc7"), "xyz" : [ { "k2" : 1, "k1" : true }, { "k2" : 1, "k1" : false } ] }
		data := [][]byte{
			{82, 0, 0, 0, 7, 95, 105, 100, 0, 91, 217, 187, 61, 220, 39, 154, 69, 92, 84, 159, 199, 4, 120, 121, 122, 0, 55, 0, 0, 0, 3, 48, 0, 22, 0, 0, 0, 1, 107, 50, 0, 0, 0, 0, 0, 0, 0, 240, 63, 8, 107, 49, 0, 1, 0, 3, 49, 0, 22, 0, 0, 0, 1, 107, 50, 0, 0, 0, 0, 0, 0, 0, 240, 63, 8, 107, 49, 0, 0, 0, 0, 0},
		}

		output, err := sp.DoStep(data)
		assert.Equal(t, nil, err, "should be equal")
		assert.NotEqual(t, nil, output, "should be equal")
		mp := output.(map[int]interface{})

		v1, err := sp.Ds.GetValueOnly(s[0])
		assert.Equal(t, nil, err, "should be equal")
		valInt1, err := util.RepString2Int(v1)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, false, mp[valInt1], "should be equal")
	}

	// test nested array
	{
		nr++
		fmt.Printf("TestParse case %d.\n", nr)
		s := []string{
			"xyz|1|k1",
			"xyz|2|k1",
			"xyz|2|k3|1|k0",
			"xyz|2|k3|6|k0",
		}
		for {
			time.Sleep(1000 * time.Millisecond)

			all := true
			for i := 0; i < len(s); i++ {
				if _, err := sp.Ds.GetValue(s[i]); err != nil {
					all = false
					break
				}
			}

			if all {
				break
			}
		}

		// { "_id" : ObjectId("5bd9bc46dc279a455c549fc9"), "xyz" : [ { "k2" : 1, "k1" : true }, { "k2" : 2, "k1" : true, "k3" : [ { "k2" : 1, "k0" : 100 }, { "k2" : 6, "k0" : -10 } ] } ] }
		data := [][]byte{
			{155, 0, 0, 0, 7, 95, 105, 100, 0, 91, 217, 188, 70, 220, 39, 154, 69, 92, 84, 159, 201, 4, 120, 121, 122, 0, 128, 0, 0, 0, 3, 48, 0, 22, 0, 0, 0, 1, 107, 50, 0, 0, 0, 0, 0, 0, 0, 240, 63, 8, 107, 49, 0, 1, 0, 3, 49, 0, 95, 0, 0, 0, 1, 107, 50, 0, 0, 0, 0, 0, 0, 0, 0, 64, 8, 107, 49, 0, 1, 4, 107, 51, 0, 69, 0, 0, 0, 3, 48, 0, 29, 0, 0, 0, 1, 107, 50, 0, 0, 0, 0, 0, 0, 0, 240, 63, 1, 107, 48, 0, 0, 0, 0, 0, 0, 0, 89, 64, 0, 3, 49, 0, 29, 0, 0, 0, 1, 107, 50, 0, 0, 0, 0, 0, 0, 0, 24, 64, 1, 107, 48, 0, 0, 0, 0, 0, 0, 0, 36, 192, 0, 0, 0, 0, 0},
		}

		output, err := sp.DoStep(data)
		assert.Equal(t, nil, err, "should be equal")
		assert.NotEqual(t, nil, output, "should be equal")
		mp := output.(map[int]interface{})

		v0, err := sp.Ds.GetValueOnly(s[0])
		assert.Equal(t, nil, err, "should be equal")
		valInt0, err := util.RepString2Int(v0)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, mp[valInt0], "should be equal")

		v1, err := sp.Ds.GetValueOnly(s[1])
		assert.Equal(t, nil, err, "should be equal")
		valInt1, err := util.RepString2Int(v1)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, mp[valInt1], "should be equal")

		v2, err := sp.Ds.GetValueOnly(s[2])
		assert.Equal(t, nil, err, "should be equal")
		valInt2, err := util.RepString2Int(v2)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(100), mp[valInt2], "should be equal")

		v3, err := sp.Ds.GetValueOnly(s[3])
		assert.Equal(t, nil, err, "should be equal")
		valInt3, err := util.RepString2Int(v3)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(-10), mp[valInt3], "should be equal")
	}

	// test nested array again
	{
		nr++
		fmt.Printf("TestParse case %d.\n", nr)
		s := []string{
			"xyz|1|k1",
			"xyz|2|k1",
			"xyz|2|k3|1|k0",
			"xyz|2|k3|6|k0",
			"xyz|2|k3|6|k2",          // 6
			"xyz|2|k3|6|k3|55|k4",    // -1
			"xyz|2|k3|6|k3|55|k5|k2", // nil
			"xyz|2|k3|6|k3|55|k5|k6", // 1
		}
		for {
			time.Sleep(1000 * time.Millisecond)

			all := true
			for i := 0; i < len(s); i++ {
				if _, err := sp.Ds.GetValue(s[i]); err != nil {
					all = false
					break
				}
			}

			if all {
				break
			}
		}

		// { "_id" : ObjectId("5bd9bfdadc279a455c549fca"), "xyz" : [ { "k2" : 1, "k1" : true }, { "k2" : 2, "k1" : true, "k3" : [ { "k2" : 1, "k0" : 100 }, { "k2" : 6, "k0" : -10, "k3" : [ { "k2" : 55, "k4" : -1, "k5" : { "k2" : "hello", "k6" : 1 } } ] } ] } ] }
		data := [][]byte{
			{231, 0, 0, 0, 7, 95, 105, 100, 0, 91, 217, 191, 218, 220, 39, 154, 69, 92, 84, 159, 202, 4, 120, 121, 122, 0, 204, 0, 0, 0, 3, 48, 0, 22, 0, 0, 0, 1, 107, 50, 0, 0, 0, 0, 0, 0, 0, 240, 63, 8, 107, 49, 0, 1, 0, 3, 49, 0, 171, 0, 0, 0, 1, 107, 50, 0, 0, 0, 0, 0, 0, 0, 0, 64, 8, 107, 49, 0, 1, 4, 107, 51, 0, 145, 0, 0, 0, 3, 48, 0, 29, 0, 0, 0, 1, 107, 50, 0, 0, 0, 0, 0, 0, 0, 240, 63, 1, 107, 48, 0, 0, 0, 0, 0, 0, 0, 89, 64, 0, 3, 49, 0, 105, 0, 0, 0, 1, 107, 50, 0, 0, 0, 0, 0, 0, 0, 24, 64, 1, 107, 48, 0, 0, 0, 0, 0, 0, 0, 36, 192, 4, 107, 51, 0, 72, 0, 0, 0, 3, 48, 0, 64, 0, 0, 0, 1, 107, 50, 0, 0, 0, 0, 0, 0, 128, 75, 64, 1, 107, 52, 0, 0, 0, 0, 0, 0, 0, 240, 191, 3, 107, 53, 0, 31, 0, 0, 0, 2, 107, 50, 0, 6, 0, 0, 0, 104, 101, 108, 108, 111, 0, 1, 107, 54, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 0},
		}

		output, err := sp.DoStep(data)
		assert.Equal(t, nil, err, "should be equal")
		assert.NotEqual(t, nil, output, "should be equal")
		mp := output.(map[int]interface{})

		v0, err := sp.Ds.GetValueOnly(s[0])
		assert.Equal(t, nil, err, "should be equal")
		valInt0, err := util.RepString2Int(v0)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, mp[valInt0], "should be equal")

		v1, err := sp.Ds.GetValueOnly(s[1])
		assert.Equal(t, nil, err, "should be equal")
		valInt1, err := util.RepString2Int(v1)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, mp[valInt1], "should be equal")

		v2, err := sp.Ds.GetValueOnly(s[2])
		assert.Equal(t, nil, err, "should be equal")
		valInt2, err := util.RepString2Int(v2)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(100), mp[valInt2], "should be equal")

		v3, err := sp.Ds.GetValueOnly(s[3])
		assert.Equal(t, nil, err, "should be equal")
		valInt3, err := util.RepString2Int(v3)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(-10), mp[valInt3], "should be equal")

		v4, err := sp.Ds.GetValueOnly(s[4])
		assert.Equal(t, nil, err, "should be equal")
		valInt4, err := util.RepString2Int(v4)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(6), mp[valInt4], "should be equal")

		v5, err := sp.Ds.GetValueOnly(s[5])
		assert.Equal(t, nil, err, "should be equal")
		valInt5, err := util.RepString2Int(v5)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(-1), mp[valInt5], "should be equal")

		v6, err := sp.Ds.GetValueOnly(s[6])
		assert.Equal(t, nil, err, "should be equal")
		valInt6, err := util.RepString2Int(v6)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, nil, mp[valInt6], "should be equal")

		v7, err := sp.Ds.GetValueOnly(s[7])
		assert.Equal(t, nil, err, "should be equal")
		valInt7, err := util.RepString2Int(v7)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(1), mp[valInt7], "should be equal")
	}

	// test union key
	arrayKeys = []string{"k1", "k2"}
	{
		nr++
		fmt.Printf("TestParse case %d.\n", nr)
		s := []string{
			"x|h1+h2|k3",
			"x|h1+h3|k3",
			"x|h2+h2|k3",
			"x|h1+|k5",
			"x|+h2|k3",
			"x|+|k5",
		}
		for {
			time.Sleep(1000 * time.Millisecond)

			all := true
			for i := 0; i < len(s); i++ {
				if _, err := sp.Ds.GetValue(s[i]); err != nil {
					all = false
					break
				}
			}

			if all {
				break
			}
		}

		// { "_id" : ObjectId("5bd9c264dc279a455c549fcc"), "x" : [ { "k1" : "h1", "k2" : "h2", "k3" : 1 }, { "k1" : "h1", "k2" : "h3", "k3" : 1 }, { "k1" : "h2", "k2" : "h2", "k3" : 4 }, { "k1" : "h1", "k5" : -1 }, { "k2" : "h2", "k3" : 100 }, { "k5" : 100 } ] }
		data := [][]byte{
			{238, 0, 0, 0, 7, 95, 105, 100, 0, 91, 217, 194, 100, 220, 39, 154, 69, 92, 84, 159, 204, 4, 120, 0, 213, 0, 0, 0, 3, 48, 0, 39, 0, 0, 0, 2, 107, 49, 0, 3, 0, 0, 0, 104, 49, 0, 2, 107, 50, 0, 3, 0, 0, 0, 104, 50, 0, 1, 107, 51, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 3, 49, 0, 39, 0, 0, 0, 2, 107, 49, 0, 3, 0, 0, 0, 104, 49, 0, 2, 107, 50, 0, 3, 0, 0, 0, 104, 51, 0, 1, 107, 51, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 3, 50, 0, 39, 0, 0, 0, 2, 107, 49, 0, 3, 0, 0, 0, 104, 50, 0, 2, 107, 50, 0, 3, 0, 0, 0, 104, 50, 0, 1, 107, 51, 0, 0, 0, 0, 0, 0, 0, 16, 64, 0, 3, 51, 0, 28, 0, 0, 0, 2, 107, 49, 0, 3, 0, 0, 0, 104, 49, 0, 1, 107, 53, 0, 0, 0, 0, 0, 0, 0, 240, 191, 0, 3, 52, 0, 28, 0, 0, 0, 2, 107, 50, 0, 3, 0, 0, 0, 104, 50, 0, 1, 107, 51, 0, 0, 0, 0, 0, 0, 0, 89, 64, 0, 3, 53, 0, 17, 0, 0, 0, 1, 107, 53, 0, 0, 0, 0, 0, 0, 0, 89, 64, 0, 0, 0},
		}
		output, err := sp.DoStep(data)
		assert.Equal(t, nil, err, "should be equal")
		assert.NotEqual(t, nil, output, "should be equal")
		mp := output.(map[int]interface{})

		v0, err := sp.Ds.GetValueOnly(s[0])
		assert.Equal(t, nil, err, "should be equal")
		valInt0, err := util.RepString2Int(v0)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(1), mp[valInt0], "should be equal")

		v1, err := sp.Ds.GetValueOnly(s[1])
		assert.Equal(t, nil, err, "should be equal")
		valInt1, err := util.RepString2Int(v1)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(1), mp[valInt1], "should be equal")

		v2, err := sp.Ds.GetValueOnly(s[2])
		assert.Equal(t, nil, err, "should be equal")
		valInt2, err := util.RepString2Int(v2)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(4), mp[valInt2], "should be equal")

		v3, err := sp.Ds.GetValueOnly(s[3])
		assert.Equal(t, nil, err, "should be equal")
		valInt3, err := util.RepString2Int(v3)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(-1), mp[valInt3], "should be equal")

		v4, err := sp.Ds.GetValueOnly(s[4])
		assert.Equal(t, nil, err, "should be equal")
		valInt4, err := util.RepString2Int(v4)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(100), mp[valInt4], "should be equal")

		v5, err := sp.Ds.GetValueOnly(s[5])
		assert.Equal(t, nil, err, "should be equal")
		valInt5, err := util.RepString2Int(v5)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, float64(100), mp[valInt5], "should be equal")
	}

	// test illegal
	{
		nr++
		fmt.Printf("TestParse case %d.\n", nr)
		s := []string{
			"age",
			"other" + "|" + "p" + "|" + "v",
			"other" + "|" + "p2",
			"map" + "|" + "i",
			"map" + "|" + "f",
			"map" + "|" + "n",
			"map" + "|" + "l" + "|" + "2",
			"list" + "|" + "1",
		}
		for {
			time.Sleep(1000 * time.Millisecond)

			all := true
			for i := 0; i < len(s); i++ {
				if _, err := sp.Ds.GetValue(s[i]); err != nil {
					all = false
					break
				}
			}

			if all {
				break
			}
		}

		// { "_id" : ObjectId("5b42e23937e3c89dc97b4f05"), "name" : "v1", "age" : 15, "other" : { "p" : { "v" : 5 }, "p2" : 6 } }
		// { "_id" : ObjectId("5b431ae2e865f9172bbade2a"), "hello" : "world", "leve0" : [ { "level1" : { "level2" : "end" } }, { "what" : "fuck" } ] }
		// { "_id" : ObjectId("5bac3df4a23d4fc0595560d1"), "map" : { "i" : 1024, "f" : 3.14, "bt" : true, "bf" : false, "n" : 999, "o" : { "s1" : "s1", "s2" : "s2", "s3" : "s3", "s4" : "s4" }, "l" : [ 1, 2, 3, 4, 5 ] }, "list" : [ 1024, 3.14, true, false, 999, { "s1" : "s1", "s2" : "s2", "s3" : "s3", "s4" : "s4" }, [ 1, 2, 3, 4, 5 ] ] }
		// { "_id" : ObjectId("5bd9ababdc279a455c549fc3"), "a" : [ 1, 2, [ 3, 4, [ 5, 6 ] ] ] }
		// { "_id" : ObjectId("5bd9ac2edc279a455c549fc4"), "list" : [ 1024, 3.14, true, false, 999, { "s1" : "s1", "s2" : "s2", "s3" : "s3", "s4" : "s4" }, [ 1, 2, 3, 4, 5 ] ] }
		data := [][]byte{
			{91, 0, 0, 0, 7, 95, 105, 100, 0, 91, 66, 226, 57, 55, 227, 200, 157, 201, 123, 79, 5, 2, 110, 97, 109, 101, 0, 3, 0, 0, 0, 118, 49, 0, 1, 97, 103, 101, 0, 0, 0, 0, 0, 0, 0, 46, 64, 3, 111, 116, 104, 101, 114, 0, 36, 0, 0, 0, 3, 112, 0, 16, 0, 0, 0, 1, 118, 0, 0, 0, 0, 0, 0, 0, 20, 64, 0, 1, 112, 50, 0, 0, 0, 0, 0, 0, 0, 24, 64, 0, 0},
			{111, 0, 0, 0, 7, 95, 105, 100, 0, 91, 67, 26, 226, 232, 101, 249, 23, 43, 186, 222, 42, 2, 104, 101, 108, 108, 111, 0, 6, 0, 0, 0, 119, 111, 114, 108, 100, 0, 4, 108, 101, 118, 101, 48, 0, 65, 0, 0, 0, 3, 48, 0, 34, 0, 0, 0, 3, 108, 101, 118, 101, 108, 49, 0, 21, 0, 0, 0, 2, 108, 101, 118, 101, 108, 50, 0, 4, 0, 0, 0, 101, 110, 100, 0, 0, 0, 3, 49, 0, 20, 0, 0, 0, 2, 119, 104, 97, 116, 0, 5, 0, 0, 0, 102, 117, 99, 107, 0, 0, 0, 0},
			{101, 1, 0, 0, 7, 95, 105, 100, 0, 91, 172, 61, 244, 162, 61, 79, 192, 89, 85, 96, 209, 3, 109, 97, 112, 0, 163, 0, 0, 0, 1, 105, 0, 0, 0, 0, 0, 0, 0, 144, 64, 1, 102, 0, 31, 133, 235, 81, 184, 30, 9, 64, 8, 98, 116, 0, 1, 8, 98, 102, 0, 0, 1, 110, 0, 0, 0, 0, 0, 0, 56, 143, 64, 3, 111, 0, 49, 0, 0, 0, 2, 115, 49, 0, 3, 0, 0, 0, 115, 49, 0, 2, 115, 50, 0, 3, 0, 0, 0, 115, 50, 0, 2, 115, 51, 0, 3, 0, 0, 0, 115, 51, 0, 2, 115, 52, 0, 3, 0, 0, 0, 115, 52, 0, 0, 4, 108, 0, 60, 0, 0, 0, 1, 48, 0, 0, 0, 0, 0, 0, 0, 240, 63, 1, 49, 0, 0, 0, 0, 0, 0, 0, 0, 64, 1, 50, 0, 0, 0, 0, 0, 0, 0, 8, 64, 1, 51, 0, 0, 0, 0, 0, 0, 0, 16, 64, 1, 52, 0, 0, 0, 0, 0, 0, 0, 20, 64, 0, 0, 4, 108, 105, 115, 116, 0, 161, 0, 0, 0, 1, 48, 0, 0, 0, 0, 0, 0, 0, 144, 64, 1, 49, 0, 31, 133, 235, 81, 184, 30, 9, 64, 8, 50, 0, 1, 8, 51, 0, 0, 1, 52, 0, 0, 0, 0, 0, 0, 56, 143, 64, 3, 53, 0, 49, 0, 0, 0, 2, 115, 49, 0, 3, 0, 0, 0, 115, 49, 0, 2, 115, 50, 0, 3, 0, 0, 0, 115, 50, 0, 2, 115, 51, 0, 3, 0, 0, 0, 115, 51, 0, 2, 115, 52, 0, 3, 0, 0, 0, 115, 52, 0, 0, 4, 54, 0, 60, 0, 0, 0, 1, 48, 0, 0, 0, 0, 0, 0, 0, 240, 63, 1, 49, 0, 0, 0, 0, 0, 0, 0, 0, 64, 1, 50, 0, 0, 0, 0, 0, 0, 0, 8, 64, 1, 51, 0, 0, 0, 0, 0, 0, 0, 16, 64, 1, 52, 0, 0, 0, 0, 0, 0, 0, 20, 64, 0, 0, 0},
			{112, 0, 0, 0, 7, 95, 105, 100, 0, 91, 217, 171, 171, 220, 39, 154, 69, 92, 84, 159, 195, 4, 97, 0, 87, 0, 0, 0, 1, 48, 0, 0, 0, 0, 0, 0, 0, 240, 63, 1, 49, 0, 0, 0, 0, 0, 0, 0, 0, 64, 4, 50, 0, 57, 0, 0, 0, 1, 48, 0, 0, 0, 0, 0, 0, 0, 8, 64, 1, 49, 0, 0, 0, 0, 0, 0, 0, 16, 64, 4, 50, 0, 27, 0, 0, 0, 1, 48, 0, 0, 0, 0, 0, 0, 0, 20, 64, 1, 49, 0, 0, 0, 0, 0, 0, 0, 24, 64, 0, 0, 0, 0},
			{189, 0, 0, 0, 7, 95, 105, 100, 0, 91, 217, 172, 46, 220, 39, 154, 69, 92, 84, 159, 196, 4, 108, 105, 115, 116, 0, 161, 0, 0, 0, 1, 48, 0, 0, 0, 0, 0, 0, 0, 144, 64, 1, 49, 0, 31, 133, 235, 81, 184, 30, 9, 64, 8, 50, 0, 1, 8, 51, 0, 0, 1, 52, 0, 0, 0, 0, 0, 0, 56, 143, 64, 3, 53, 0, 49, 0, 0, 0, 2, 115, 49, 0, 3, 0, 0, 0, 115, 49, 0, 2, 115, 50, 0, 3, 0, 0, 0, 115, 50, 0, 2, 115, 51, 0, 3, 0, 0, 0, 115, 51, 0, 2, 115, 52, 0, 3, 0, 0, 0, 115, 52, 0, 0, 4, 54, 0, 60, 0, 0, 0, 1, 48, 0, 0, 0, 0, 0, 0, 0, 240, 63, 1, 49, 0, 0, 0, 0, 0, 0, 0, 0, 64, 1, 50, 0, 0, 0, 0, 0, 0, 0, 8, 64, 1, 51, 0, 0, 0, 0, 0, 0, 0, 16, 64, 1, 52, 0, 0, 0, 0, 0, 0, 0, 20, 64, 0, 0, 0},
		}

		output, err := sp.DoStep(data)
		assert.Equal(t, nil, err, "should be equal")
		mp := output.(map[int]interface{})

		v1, err := sp.Ds.GetValueOnly(s[0])
		assert.Equal(t, nil, err, "should be equal")
		valInt1, err := util.RepString2Int(v1)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 15.0, mp[valInt1], "should be equal")

		v2, err := sp.Ds.GetValueOnly(s[1])
		assert.Equal(t, nil, err, "should be equal")
		valInt2, err := util.RepString2Int(v2)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 5.0, mp[valInt2], "should be equal")

		v3, err := sp.Ds.GetValueOnly(s[2])
		assert.Equal(t, nil, err, "should be equal")
		valInt3, err := util.RepString2Int(v3)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 6.0, mp[valInt3], "should be equal")

		v4, err := sp.Ds.GetValueOnly(s[3])
		assert.Equal(t, nil, err, "should be equal")
		valInt4, err := util.RepString2Int(v4)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 1024.0, mp[valInt4], "should be equal")

		v5, err := sp.Ds.GetValueOnly(s[4])
		assert.Equal(t, nil, err, "should be equal")
		valInt5, err := util.RepString2Int(v5)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 3.14, mp[valInt5], "should be equal")

		v6, err := sp.Ds.GetValueOnly(s[5])
		assert.Equal(t, nil, err, "should be equal")
		valInt6, err := util.RepString2Int(v6)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 999.0, mp[valInt6], "should be equal")

		v7, err := sp.Ds.GetValueOnly(s[6])
		assert.Equal(t, nil, err, "should be equal")
		valInt7, err := util.RepString2Int(v7)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, nil, mp[valInt7], "should be equal")

		v8, err := sp.Ds.GetValueOnly(s[7])
		assert.Equal(t, nil, err, "should be equal")
		valInt8, err := util.RepString2Int(v8)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, nil, mp[valInt8], "should be equal")
	}
}

func TestFilterKey(t *testing.T) {
	var nr int

	{
		nr++
		fmt.Printf("TestParse case %d.\n", nr)
		assert.Equal(t, false, filterKey([]string{"a", "b"}), "should be equal")
		assert.Equal(t, false, filterKey([]string{"", "b"}), "should be equal")
		assert.Equal(t, false, filterKey([]string{"a", ""}), "should be equal")
		assert.Equal(t, false, filterKey([]string{}), "should be equal")
		assert.Equal(t, false, filterKey([]string{"a~"}), "should be equal")
		assert.Equal(t, true, filterKey([]string{"$"}), "should be equal")
		assert.Equal(t, false, filterKey([]string{"~$~~"}), "should be equal")
		assert.Equal(t, true, filterKey([]string{"ts"}), "should be equal")
		assert.Equal(t, true, filterKey([]string{"$", "ts"}), "should be equal")
		assert.Equal(t, false, filterKey([]string{"ts", "$"}), "should be equal")
		assert.Equal(t, false, filterKey([]string{"ts", "tsts"}), "should be equal")
	}
}
