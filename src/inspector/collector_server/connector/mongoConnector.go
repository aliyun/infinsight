package connector

import (
	"fmt"

	"inspector/client"
	"inspector/util"

	"github.com/golang/glog"
	"github.com/vinllen/mgo"
)

const (
	adminDB = "admin"
)

type mongoConnector struct {
	service  string   // service name: mongodb, redis
	addr     string   // ip:port
	username string   // username
	password string   // password
	cmds     []string // database command of getting data

	// belows are generated inner
	client   client.ClientInterface // mongo client
	session  *mgo.Session           // mongo session
	isClosed bool                   // current connector is closed
}

func (mc *mongoConnector) Get() (interface{}, error) {
	if mc.isClosed {
		return nil, fmt.Errorf("mongo connector session is closed")
	}

	var err error
	if err = mc.ensureNetwork(); err != nil {
		return nil, err
	}

	data := make([][]byte, len(mc.cmds))
	for i, cmd := range mc.cmds {
		if data[i], err = mc.session.DB(adminDB).SimpleRun(cmd); err != nil {
			data[i] = nil
		}
	}
	return data, nil // [][]byte, error
}

func (mc *mongoConnector) Close() {
	glog.Infof("mongoConnector with address[%v] closed", mc.addr)
	if mc.client != nil {
		mc.client.Close()
		mc.client = nil
	}
	mc.isClosed = true
}

func (mc *mongoConnector) ensureNetwork() error {
	if mc.client != nil {
		if mc.session != nil {
			return nil
		} else {
			mc.client.Close()
			mc.client = nil
			// do reconnect
		}
	}

	var err error

	// // 1. get real password
	// realPassword, err := util.DecryptCfb(util.Base64Decode(unsafe.String2Bytes(mc.password)))
	// if err != nil {
	// 	return fmt.Errorf("decrypt with service[%s] address[%s] password error[%v]",
	// 		mc.service, mc.addr, err)
	// }

	address := fmt.Sprintf("%s?connect=direct;maxIdleTimeMS=5000", util.ConvertUnderline2Dot(mc.addr))
	// 2. create client
	mc.client, err = client.NewClient(util.Mongo, address, mc.username, mc.password)
	if err != nil {
		return fmt.Errorf("create client with db-type[%s] address[%s] error[%v]", util.Mongo, mc.addr, err)
	}
	mc.session = mc.client.GetSession().(*mgo.Session)

	return nil
}
