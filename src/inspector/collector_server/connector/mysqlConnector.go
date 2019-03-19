package connector

import (
	"database/sql"
	"errors"
	"fmt"
	"inspector/client"
	"inspector/util"

	"github.com/golang/glog"

	_ "github.com/go-sql-driver/mysql"
)

type mysqlConnector struct {
	service  string   // service name: mongodb, redis
	addr     string   // ip:port
	username string   // username
	password string   // password
	cmds     []string // database command of getting data

	client   client.ClientInterface // mysql client
	session  *sql.DB                // mysql session
	isClosed bool
}

func (mc *mysqlConnector) Get() (interface{}, error) {
	if mc.isClosed {
		return nil, fmt.Errorf("mysql connector session is closed")
	}

	var err error
	if err = mc.ensureNetwork(); err != nil {
		return nil, err
	}

	var result [][]string = make([][]string, 2)
	result[0] = make([]string, 0)
	result[1] = make([]string, 0)
	var rows *sql.Rows
	for _, cmd := range mc.cmds {
		// rows, err = mc.session.Query("show status")
		if rows, err = mc.session.Query(cmd); err != nil {
			var errStr = fmt.Sprintf("query[%s] error: %s", cmd, err.Error())
			glog.Error(errStr)
			return nil, errors.New(errStr)
		}
		for rows.Next() {
			var key string
			var value string
			if err = rows.Scan(&key, &value); err != nil {
				glog.Errorf("scan in query[%s] error: %s", cmd, err.Error())
			}
			result[0] = append(result[0], key)
			result[1] = append(result[1], value)
		}
	}
	return result, nil
}

func (mc *mysqlConnector) Close() {
	glog.Infof("mysqlConnector with address[%v] closed", mc.addr)
	if mc.client != nil {
		mc.client.Close()
		mc.client = nil
	}
	mc.isClosed = true
}

func (mc *mysqlConnector) ensureNetwork() error {
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

	// create client
	var address = util.ConvertUnderline2Dot(mc.addr)
	mc.client, err = client.NewClient(util.Mysql, address, mc.username, mc.password)
	if err != nil {
		return fmt.Errorf("create client with db-type[%s] address[%s] error[%v]", util.Mysql, mc.addr, err)
	}
	mc.session = mc.client.GetSession().(*sql.DB)

	return nil
}
