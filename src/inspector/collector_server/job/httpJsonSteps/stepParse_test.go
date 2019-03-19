package httpJsonSteps

import (
	"fmt"
	"testing"
	"time"

	"inspector/collector_server/metric"
	"inspector/collector_server/model"
	"inspector/dict_server"
	"inspector/util"
	"inspector/util/whatson"

	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
)

const (
	jobNameParse       = "redis_2_4"
	mongoAddressParse  = "100.81.245.155:20111" // config server
	mongoUsernameParse = "admin"
	mongoPasswordParse = "admin"
	instanceNameParse  = "test-redis-instance"
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

	metric.CreateMetric()

	return &ParseParameter{
		Ds: ds,
	}
}

func TestParse(t *testing.T) {
	var nr int

	p := NewParseParameter()
	assert.NotEqual(t, nil, p, "should be equal")

	sp := NewStepParse("222.1.1.1", jobNameParse, &model.Instance{Addr: instanceNameParse},
		whatson.NewParser(whatson.Json), p.Ds)

	{
		nr++
		fmt.Printf("TestParse case %d.\n", nr)

		s := []string{
			"age",
			"other" + "|" + "p" + "|" + "v",
			"other" + "|" + "p2",
			"cpu" + "|" + "usage_cpu",
			"cpu" + "|" + "usage_cpu_sys",
			"cpu" + "|" + "usage_cpu_user",
			"cpu" + "|" + "usage_cpu_not_support",
			"what",
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

		data := [][]byte{
			[]byte(`{"age": 155, "other":{"p":{"v":true}, "p2":1.234567}, "cpu": {"usage_cpu": 1.5, "usage_cpu_sys": 12345678.987654321, "usage_cpu_user": -10000.2222222222, "usage_cpu_not_support": -100.543}, "what":"abc"}`),
		}

		output, err := sp.DoStep(data)
		assert.Equal(t, nil, err, "should be equal")
		mp := output.(map[int]interface{})

		v0, err := sp.Ds.GetValueOnly(s[0])
		assert.Equal(t, nil, err, "should be equal")
		valInt0, err := util.RepString2Int(v0)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(155), mp[valInt0], "should be equal")

		v1, err := sp.Ds.GetValueOnly(s[1])
		assert.Equal(t, nil, err, "should be equal")
		valInt1, err := util.RepString2Int(v1)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, mp[valInt1], "should be equal")

		v2, err := sp.Ds.GetValueOnly(s[2])
		assert.Equal(t, nil, err, "should be equal")
		valInt2, err := util.RepString2Int(v2)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(1), mp[valInt2], "should be equal")

		v3, err := sp.Ds.GetValueOnly(s[3])
		assert.Equal(t, nil, err, "should be equal")
		valInt3, err := util.RepString2Int(v3)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(150), mp[valInt3], "should be equal")

		v4, err := sp.Ds.GetValueOnly(s[4])
		assert.Equal(t, nil, err, "should be equal")
		valInt4, err := util.RepString2Int(v4)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(1234567898), mp[valInt4], "should be equal")

		v5, err := sp.Ds.GetValueOnly(s[5])
		assert.Equal(t, nil, err, "should be equal")
		valInt5, err := util.RepString2Int(v5)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(-1000022), mp[valInt5], "should be equal")

		v6, err := sp.Ds.GetValueOnly(s[6])
		assert.Equal(t, nil, err, "should be equal")
		valInt6, err := util.RepString2Int(v6)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(-100), mp[valInt6], "should be equal")

		v7, err := sp.Ds.GetValueOnly(s[7])
		assert.Equal(t, nil, err, "should be equal")
		valInt7, err := util.RepString2Int(v7)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, nil, mp[valInt7], "should be equal")
	}
}
