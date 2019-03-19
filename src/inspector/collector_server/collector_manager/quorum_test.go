package collectorManager

import (
	"fmt"
	"testing"
	"sync"

	"inspector/config"
	"inspector/heartbeat"

	"github.com/stretchr/testify/assert"
	"flag"
	"sync/atomic"
	"time"
)

const(
	mongoAddress         = "10.101.72.137:20001" // config server
	mongoUsername        = "admin"
	mongoPassword        = "admin"
	configServerInterval = 5
	heartbeatInterval    = 3
)

type Parameter struct {
	cs config.ConfigInterface
	hb *heartbeat.Heartbeat
}

func NewParameter(hostName string) (*Parameter, error) {
	// 1. create config server
	factory := config.ConfigFactory{Name: config.MongoConfigName}
	cs, err := factory.Create(mongoAddress, mongoUsername, mongoPassword, "test", configServerInterval*1000)
	if err != nil {
		return nil, err
	}

	// 2. create heartbet
	hbConf := &heartbeat.Conf{
		Module:   heartbeat.ModuleCollector,
		Service:  hostName,
		Interval: heartbeatInterval,
		Address:  mongoAddress,
		Username: mongoUsername,
		Password: mongoPassword,
	}
	hb := heartbeat.NewHeartbeat(hbConf)
	if hb == nil {
		return nil, fmt.Errorf("create heatbeat error")
	}
	if err = hb.Start(); err != nil {
		return nil, fmt.Errorf("start heart beat error[%v]", err)
	}

	return &Parameter{
		cs: cs,
		hb: hb,
	}, nil
}

func (p * Parameter) Close() {
	p.cs.DeleteAll()
	p.cs.Close()
	p.hb.Close()
}

func TestQuorumLeader(t *testing.T) {
	address := fmt.Sprintf("50_5_5_5:%d", 11111)
	p, err := NewParameter(address)
	assert.Equal(t, nil, err, "shouldn't be nil")

	status := QuorumLeader("c1", "k1", address, p.cs, p.hb)
	assert.Equal(t, true, status, "shouldn't be nil")

	p.Close()
}

func TestQuorumLeader2(t *testing.T) {
	flag.Set("stderrthreshold", "info")
	flag.Set("v", "2")

	var cnt uint32
	var group sync.WaitGroup
	inner := 100
	for i := 0; i < 10; i++ {
		group.Add(1)
		go func(port int) {
			address := fmt.Sprintf("50_5_5_5:%d", port)
			p, err := NewParameter(address)
			assert.Equal(t, nil, err, "shouldn't be nil")

			for i := 0; i < inner; i++ {
				status := QuorumLeader("c1", "k1", address, p.cs, p.hb)
				if status {
					atomic.AddUint32(&cnt, 1)
				}
				time.Sleep(1 * time.Second)
			}

			group.Done()

			// p.Close()
		}(i + 1)
	}
	group.Wait()

	assert.Equal(t, inner, int(cnt), "shouldn't be nil")
}