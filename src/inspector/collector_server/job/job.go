package job

import (
	"inspector/cache"
	"inspector/collector_server/connector"
	"inspector/collector_server/model"
	"inspector/config"
	"inspector/dict_server"
	"inspector/heartbeat"
	"inspector/util/scheduler"

	"inspector/util"

	"github.com/golang/glog"
)

func Create(serviceName string, tcb *scheduler.TCB, connector connector.Connector,
	ringCache *cache.RingCache, cs config.ConfigInterface, ds *dictServer.DictServer,
	hb *heartbeat.Heartbeat, ins *model.Instance, senderMsgChan chan<- *model.SenderContext) Job {
	tp := util.GetDbType(ins.DBType)
	switch tp {
	case util.Mysql:
		return &MysqlJob{TCB: tcb, Connector: connector, RingCache: ringCache,
			Cs: cs, Ds: ds, Hb: hb, ServiceName: serviceName, Instance: ins,
			SenderMsgChan: senderMsgChan}
	case util.Redis:
		return &RedisJob{TCB: tcb, Connector: connector, RingCache: ringCache,
			Cs: cs, Ds: ds, Hb: hb, ServiceName: serviceName, Instance: ins,
			SenderMsgChan: senderMsgChan}
	case util.Mongo:
		return &MongoJob{TCB: tcb, Connector: connector, RingCache: ringCache,
			Cs: cs, Ds: ds, Hb: hb, ServiceName: serviceName, Instance: ins,
			SenderMsgChan: senderMsgChan}
	case util.HttpJson:
		return &HttpJsonJob{TCB: tcb, Connector: connector, RingCache: ringCache,
			Cs: cs, Ds: ds, Hb: hb, ServiceName: serviceName, Instance: ins,
			SenderMsgChan: senderMsgChan}
	default:
		glog.Errorf("specific type[%s] not support", serviceName)
		return nil
	}
}

type Job interface {
	/*
	 * equip the TCB.
	 */
	Equip(debug bool) error

	/*
	 * get the TCB
	 */
	GetTCB() *scheduler.TCB

	/*
	 * get the connector
	 */
	GetConnector() connector.Connector

	/*
	 * get the ring cache
	 */
	GetRingCache() *cache.RingCache

	/*
	 * get the base information: 'interval', 'count'
	 */
	GetBaseInfo() (int, int)
}
