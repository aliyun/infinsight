package collectorManager

import (
	"inspector/config"
	"inspector/heartbeat"
	"inspector/util"

	"github.com/golang/glog"
)

const (
	notExistLeader = "random_not_exist_leader"
)

// quorum leader, only uses collector server inside currently which is hard code
func QuorumLeader(collectionName, keyName, currentCollectorAddress string, cs config.ConfigInterface,
		hb *heartbeat.Heartbeat) bool {
	// get leader
	leader, err := cs.GetString(collectionName, keyName)
	if err != nil {
		if !util.IsNotFound(err) {
			glog.Errorf("get leader from config server error[%v]", err)
			return false
		}
		glog.Info("leader not exists, try to elect one")
		leader = notExistLeader
	}

	if leader == currentCollectorAddress {
		glog.Infof("current[%s] is the leader", leader)
		return true
	}

	// hard code of collector server
	status := hb.IsAlive(heartbeat.ModuleCollector, leader)
	if status == heartbeat.ServiceAlive {
		glog.Infof("current isn't the leader but the [%s]", leader)
		return false
	} else if status == heartbeat.ServiceUnknown {
		glog.Infof("current isn't the leader but the [%s], however the status is unknown", leader)
		return false
	}
	glog.Infof("current leader[%v] is dead, current[%v] try to quorum", leader, currentCollectorAddress)

	// lock collection
	if err := cs.Lock(collectionName, ""); err != nil {
		glog.Infof("lock TaskListCollection error[%v]", err)
		return false
	}
	defer cs.Unlock(collectionName, "")

	// double check
	leader2, err2 := cs.GetString(collectionName, keyName)
	if err2 != nil {
		if util.IsNotFound(err) {
			leader2 = notExistLeader
		} else {
			glog.Warningf("double check get leader2 failed, leader is [%s]", leader)
			return false
		}
	}
	if leader != leader2 || hb.IsAlive(heartbeat.ModuleCollector, leader2) == heartbeat.ServiceAlive ||
			hb.IsAlive(heartbeat.ModuleCollector, leader2) == heartbeat.ServiceUnknown {
		// not the same or alive, give up election
		glog.Warningf("double check fail, leader[%s] leader2[%s]", leader, leader2)
		return false
	}

	if err := cs.SetItem(collectionName, keyName, currentCollectorAddress); err != nil {
		glog.Errorf("set leader of address[%s] error[%v]", currentCollectorAddress, err)
		return false
	}
	glog.Infof("current[%v] quorum successfully", currentCollectorAddress)

	return true
}