package connector

import (
	"errors"
	"fmt"
	"inspector/client"
	"inspector/util"
	"strings"

	"github.com/go-redis/redis"
	"github.com/golang/glog"
)

type redisConnector struct {
	service  string   // service name: mongodb, redis
	addr     string   // ip:port
	username string   // username
	password string   // password
	cmds     []string // database command of getting data

	client   client.ClientInterface // redis client
	session  *redis.Client          // redis session
	isClosed bool
}

func (rc *redisConnector) Get() (interface{}, error) {
	if rc.isClosed {
		return nil, fmt.Errorf("redis connector session is closed")
	}

	var err error
	if err = rc.ensureNetwork(); err != nil {
		return nil, err
	}

	data := make([][]byte, len(rc.cmds))
	for i, cmd := range rc.cmds {
		var params = strings.Split(cmd, " ")
		var filter = "all"
		if len(params) == 0 || params[0] != "info" {
			var errStr = fmt.Sprintf("cmd[%s] is not support", cmd)
			glog.Errorf(errStr)
			return nil, errors.New(errStr)
		}
		if len(params) == 2 {
			filter = params[1]
		}
		if data[i], err = rc.session.Info(filter).Bytes(); err != nil {
			var errStr = fmt.Sprintf("cmd[%s] run failed", cmd)
			glog.Errorf(errStr)
			return nil, errors.New(errStr)
		}
	}

	return data, nil
}

func (rc *redisConnector) Close() {
	glog.Infof("redisConnector with address[%v] closed", rc.addr)
	if rc.client != nil {
		rc.client.Close()
		rc.client = nil
	}
	rc.isClosed = true
}

func (rc *redisConnector) ensureNetwork() error {
	if rc.client != nil {
		if rc.session != nil {
			return nil
		} else {
			rc.client.Close()
			rc.client = nil
			// do reconnect
		}
	}

	var err error

	// create client
	var address = util.ConvertUnderline2Dot(rc.addr)
	rc.client, err = client.NewClient(util.Redis, address, rc.username, rc.password)
	if err != nil {
		return fmt.Errorf("create client with db-type[%s] address[%s] error[%v]", util.Redis, rc.addr, err)
	}
	rc.session = rc.client.GetSession().(*redis.Client)

	return nil
}
