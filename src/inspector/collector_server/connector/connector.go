package connector

import (
	"github.com/golang/glog"
	"inspector/collector_server/model"
	"inspector/util"
)

type Connector interface {
	Get() (interface{}, error) // get data, return []byte array in which every command returns
	Close()                    // close
}

func NewConnector(service string, ins *model.Instance, params ...string) Connector {
	var tp = util.GetDbType(ins.DBType)
	var addr = util.ConvertUnderline2Dot(ins.Addr)
	switch tp {
	case util.Mysql:
		return &mysqlConnector{
			service:  service,
			addr:     addr,
			username: ins.Username,
			password: ins.Password,
			cmds:     ins.Commands,
		}
	case util.Redis:
		return &redisConnector{
			service:  service,
			addr:     addr,
			username: ins.Username,
			password: ins.Password,
			cmds:     ins.Commands,
		}
	case util.Mongo:
		return &mongoConnector{
			service:  service,
			addr:     addr,
			username: ins.Username,
			password: ins.Password,
			cmds:     ins.Commands,
		}
	case util.HttpJson:
		return NewHttpConnector(service, addr, ins.Commands)
	case util.File: // todo
		return &fileConnector{
			directory: params[0], // service is directory here
			addr:      addr,
			offset:    0, // start from 0
		}
	default:
		glog.Errorf("create connector with unknown type: %s", tp)
		return nil
	}
}
