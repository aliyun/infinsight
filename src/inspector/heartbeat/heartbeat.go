/*
// =====================================================================================
//
//       Filename:  heartbeat.go
//
//    Description:  用MongoDB的形式实现Config接口
//
//        Version:  1.0
//        Created:  08/06/2018 03:21:01 PM
//       Revision:  none
//       Compiler:  go1.10.1
//
//         Author:  zhuzhao.cx, zhuzhao.cx@alibaba-inc.com
//        Company:  Alibaba Group
//
// =====================================================================================
*/

package heartbeat

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"inspector/config"
	"inspector/util"

	"github.com/golang/glog"
)

type ModuleType string
type WatchEvent uint
type ServiceStatus uint

const (
	ModuleApi       ModuleType = "api_server"
	ModuleCollector ModuleType = "collector_server"
	ModuleStore     ModuleType = "store_server"
	ModuleAll       ModuleType = "all" // used only in register, do not set in Conf
	ServiceAll      string     = "all"

	SectionName  = "heartbeat"
	intervalName = "interval"
	updateName   = "update"
	idName       = "gid" // increase id number from 0

	WatcherConnect    WatchEvent = 0x00000001
	WatcherDisconnect WatchEvent = 0x00000010
	WatcherAll        WatchEvent = 0x00000011

	registerTry         = 20   // retry times when fail
	registerTryInterval = 1000 // ms

	ServiceNotExist ServiceStatus = 0
	ServiceDead     ServiceStatus = 1 // open for user
	ServiceAlive    ServiceStatus = 2 // open for user
	ServiceBoth     ServiceStatus = 3 // open for user
	ServiceUnknown  ServiceStatus = 4
)

type Conf struct {
	Module   ModuleType // type
	Service  string     // current service address: ip+port
	Interval int        // unit: second

	// mongo configuration
	Address  string
	Username string
	Password string
	DB       string
}

type Watcher struct {
	Event   WatchEvent
	Handler func(event WatchEvent) error
}

type NodeStatus struct {
	Gid   int32
	Alive bool
	Name  string
}

// not support dot in Service
func NewHeartbeat(c *Conf) *Heartbeat {
	if len(c.Service) == 0 || c.Interval == 0 || len(c.Address) == 0 ||
		strings.Contains(c.Service, config.NamespaceSplitter) {
		glog.Warning("input parameter invalid")
		return nil
	}
	return &Heartbeat{
		Conf:           c,
		timeout:        5, // 5 times of conf.Interval
		sigChanAlive:   make(chan struct{}),
		watcherMap:     make(map[ModuleType]map[string]map[WatchEvent]func(event WatchEvent) error),
		previousStatus: make(map[ModuleType]map[string]*NodeStatus),
	}
}

type Heartbeat struct {
	Conf         *Conf                  // configuration given by user
	timeout      uint32                 // timeout to exit goroutine
	cfgHandler   config.ConfigInterface // configuration handler
	sigChanAlive chan struct{}          // used to close

	// map: moduleType->service->watchEvent->handler
	watcherMap     map[ModuleType]map[string]map[WatchEvent]func(event WatchEvent) error
	previousStatus map[ModuleType]map[string]*NodeStatus // store previous module status, used to compare
}

func (h *Heartbeat) Start() error {
	var err error
	factory := config.ConfigFactory{Name: config.MongoConfigName}
	h.cfgHandler, err = factory.Create(h.Conf.Address, h.Conf.Username, h.Conf.Password, h.Conf.DB,
		-1)
	if err != nil {
		return err
	}

	// register service
	if err = h.register(); err != nil {
		return err
	}

	// star a goroutine to keep alive
	go h.keepAlive()

	return nil
}

func (h *Heartbeat) Close() {
	close(h.sigChanAlive)
	if h.cfgHandler != nil {
		h.cfgHandler.Close()
	}
}

/*
 * get all modules including alive and dead, return map.
 * @return:
 *     map[ModuleType][]*NodeStatus
 *         first level is the module type
 *         second level is the NodeStatus(ip:port, alive, gid)
 *     error
 */
func (h *Heartbeat) GetModules(status ServiceStatus) (map[ModuleType][]*NodeStatus, error) {
	if status == ServiceNotExist {
		return nil, fmt.Errorf("invalid input status[%v]", status)
	}

	mp := make(map[ModuleType][]*NodeStatus, 3)
	allType := [3]ModuleType{ModuleApi, ModuleCollector, ModuleStore}
	for _, tp := range allType {
		if ret, err := h.GetServices(tp, status); err != nil {
			if !util.IsNotFound(err) {
				return nil, fmt.Errorf("GetAllModules get module[%v] error[%v]", tp, err)
			} else {
				mp[tp] = []*NodeStatus{} // set empty array
			}
		} else {
			mp[tp] = ret
		}
	}
	return mp, nil
}

func (h *Heartbeat) ConvertArray2Map(input []*NodeStatus) map[string]*NodeStatus {
	mp := make(map[string]*NodeStatus, len(input))
	for _, ele := range input {
		mp[ele.Name] = ele
	}
	return mp
}

/*
 * get all service address of given module type, return list.
 * @return:
 *     map[string]bool
 *         first level is the service address(ip:port)
 *         second level is alive or not
 *     error
 */
func (h *Heartbeat) GetServices(module ModuleType, status ServiceStatus) ([]*NodeStatus, error) {
	if status == ServiceNotExist || status == ServiceUnknown {
		return nil, fmt.Errorf("invalid input status[%v]", status)
	}

	ret, err := h.cfgHandler.GetMap(SectionName, string(module))
	if err != nil {
		return nil, err
	}

	ans := make([]*NodeStatus, 0, len(ret))
	now := h.currentTime()
	for key, value := range ret {
		// fmt.Println(module, key, value)
		// omit key is idName
		if key == idName {
			continue
		}
		// key: ip:port, value:
		mp, ok := value.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("module[%v] service[%s] parse inner map in GetAllServices error",
				h.Conf.Module, h.Conf.Service)
		}

		// fmt.Println("1111")
		// map is empty
		if len(mp) == 0 {
			continue
		}

		interval, ok := mp[intervalName].(int32)
		update := uint32(mp[updateName].(float64))
		gid := mp[idName].(float64)

		//if module == ModuleStore {
		//	fmt.Printf("update:[%d], interval:[%d], h.timeout:[%d], now[%d]", update, interval, h.timeout, now)
		//}
		var exist bool
		if update + h.timeout * uint32(interval) >= now {
			exist = true
		}

		if status == ServiceBoth || exist && status == ServiceAlive || !exist && status == ServiceDead {
			ans = append(ans, &NodeStatus{
				Gid:   int32(gid),
				Alive: exist,
				Name:  key,
			})
		}
	}

	// sort based on gid
	sort.Slice(ans, func(i, j int) bool {
		if ans[i].Gid != ans[j].Gid {
			return ans[i].Gid < ans[j].Gid
		}
		return ans[i].Name < ans[i].Name
	})

	return ans, nil
}

// is alive?
func (h *Heartbeat) IsAlive(module ModuleType, service string) ServiceStatus {
	mp, status := h.isExist(string(module), service)
	if status == 0 {
		return ServiceNotExist
	} else if status == 1 {
		return ServiceUnknown
	}

	var (
		interval int32
		update   float64
		ok       bool
	)

	if interval, ok = mp[intervalName].(int32); !ok {
		return ServiceDead
	}
	if update, ok = mp[updateName].(float64); !ok {
		return ServiceDead
	}

	now := h.currentTime()
	if uint32(update) + h.timeout * uint32(interval) >= now {
		return ServiceAlive
	}
	glog.Infof("module[%v] service[%s] dead, update[%v] timeout[%v] interval[%v] now[%v]",
		module, service, uint32(update), h.timeout, uint32(interval), now)
	return ServiceDead
}

// get the number of given service
func (h *Heartbeat) GetServiceCount(module ModuleType) (int, error) {
	return h.cfgHandler.GetInt(SectionName, string(module), idName)
}

func (h *Heartbeat) keepAlive() {
	var count uint32
	duration := time.Duration(h.Conf.Interval) * time.Second
	for {
		select {
		case _, ok := <-h.sigChanAlive:
			if !ok {
				glog.Infof("heartbeat goroutine close")
				return
			}
		case <-time.NewTicker(duration).C:
			// do check others
			current, err := h.GetModules(ServiceBoth)
			if err != nil {
				glog.Errorf("heartbeat GetAliveModules error[%v]", err)
			} else {
				for module, serviceList := range current {
					if len(serviceList) == 0 {
						continue
					}
					if _, ok := h.previousStatus[module]; !ok {
						h.previousStatus[module] = make(map[string]*NodeStatus)
					}

					serviceMp := h.ConvertArray2Map(serviceList)
					previousServices := h.previousStatus[module]
					for service, exist := range serviceMp {
						if previousServices[service] == nil || exist.Alive != previousServices[service].Alive {
							if callback := h.getWatcher(module, service, WatcherConnect); callback != nil && exist.Alive {
								callback(WatcherConnect)
							}
							if callback := h.getWatcher(module, service, WatcherDisconnect); callback != nil && !exist.Alive {
								callback(WatcherDisconnect)
							}
						}
					}

					// copy current to previous used to next iteration
					h.previousStatus[module] = serviceMp
				}

				// copy current to previous used to next iteration
				// h.previousStatus = current
			}

			// do check itself
			if err := h.updateTime(); err != nil {
				count++
			} else {
				count = 1
			}

			if count > h.timeout {
				glog.Errorf("heartbeat goroutine exit")
				if callback := h.getWatcher(h.Conf.Module, h.Conf.Service, WatcherDisconnect); callback != nil {
					callback(WatcherDisconnect)
				}
				return
			}
		}
	}
	glog.Errorf("heartbeat goroutine close abnormally")
}

/*
 * register watcher, module == ModuleAll means watcher all modules and all event
 * service == ServiceAll means watcher all services and all event
 * watcher == WatcherAll means watch all watcherEvent.
 * pay attention:
 * when module == ModuleAll, service must be ServiceAll and watcher must be WatcherAll.
 * when service == ServiceAll, watcher must be WatcherAll.
 * If user want to monitor itself, watcher also should be registered.
 */
func (h *Heartbeat) RegisterGlobalWatcher(module ModuleType, service string, watcher *Watcher) error {
	if module == ModuleAll && service != ServiceAll && watcher.Event != WatcherAll {
		return fmt.Errorf("when module == ModuleAll, service must be ServiceAll and watcher must be WatcherAll")
	}
	if service == ServiceAll && watcher.Event != WatcherAll {
		return fmt.Errorf("when service == ServiceAll, watcher must be WatcherAll")
	}

	if _, ok := h.watcherMap[module]; !ok {
		h.watcherMap[module] = make(map[string]map[WatchEvent]func(Event WatchEvent) error)
	}

	mp1 := h.watcherMap[module]
	if _, ok := mp1[service]; !ok {
		mp1[service] = make(map[WatchEvent]func(Event WatchEvent) error)
	}

	mp2 := mp1[service]
	mp2[watcher.Event] = watcher.Handler

	return nil
}

func (h *Heartbeat) RemoveGlobalWatcher(module ModuleType, service string) error {
	if module == ModuleAll {
		// remove all map
		h.watcherMap = make(map[ModuleType]map[string]map[WatchEvent]func(event WatchEvent) error)
		return nil
	}

	if _, ok := h.watcherMap[module]; !ok {
		return fmt.Errorf("module[%v] not exist", module)
	}

	if service == ServiceAll {
		h.watcherMap[module] = make(map[string]map[WatchEvent]func(event WatchEvent) error)
		return nil
	}
	mp1 := h.watcherMap[module]
	if _, ok := mp1[service]; !ok {
		return fmt.Errorf("service[%s] not exist in module[%v] ", service, module)
	}

	delete(mp1, service)
	return nil
}

// judge whether a given module/service/event is exist in the watcherMap, return callback
func (h *Heartbeat) getWatcher(module ModuleType, service string, event WatchEvent) func(event WatchEvent) error {
	if _, ok := h.watcherMap[ModuleAll]; ok {
		return h.watcherMap[ModuleAll][ServiceAll][WatcherAll]
	}

	if _, ok := h.watcherMap[module]; !ok {
		return nil
	}

	mp1 := h.watcherMap[module]
	if _, ok := mp1[ServiceAll]; ok {
		return mp1[ServiceAll][WatcherAll]
	}

	if _, ok := mp1[service]; !ok {
		return nil
	}

	mp2 := mp1[service]
	if _, ok := mp2[WatcherAll]; ok {
		return mp2[WatcherAll]
	}

	if _, ok := mp2[event]; !ok {
		return nil
	}
	return mp2[event]
}

/*
 * register heartbeat.
 * old gid won't be reused by other service
 */
func (h *Heartbeat) register() error {
	var status ServiceStatus
	module := string(h.Conf.Module)

	// try to get status
	ok := util.CallbackRetry(registerTry, registerTryInterval, func() bool {
		status = h.IsAlive(h.Conf.Module, h.Conf.Service)
		if status == ServiceUnknown {
			return true
		}
		return false
	})
	if !ok {
		return fmt.Errorf("module[%s] service[%s] get alive status error", module, h.Conf.Service)
	}

	if status == ServiceAlive {
		return fmt.Errorf("module[%s] service[%s] is already exist", h.Conf.Module, h.Conf.Service)
	} else if status == ServiceDead {
		glog.Infof("module[%s] service[%s] dead, try to update", module, h.Conf.Service)
		// update
		// 1. fetch gid again, this request is redundant because this info can get in the above function,
		// however, we do fetch again to make code clearly
		_, err := h.cfgHandler.GetInt(SectionName, module, idName)
		if err != nil {
			// if not found, goto next step: distribute new gid
			if util.IsNotFound(err) {
				glog.Warningf("module[%s] service[%s] ServiceDead but fetch gid empty",
					module, h.Conf.Service)
			} else {
				return fmt.Errorf("module[%s] service[%s] ServiceDead but fetch gid failed[%v]",
					module, h.Conf.Service, err)
			}
		} else {
			// reused previous id if restart
			var id int
			var err error
			ok := util.CallbackRetry(3, registerTryInterval, func() bool {
				id, err = h.cfgHandler.GetInt(SectionName, module, h.Conf.Service, idName)
				if err != nil {
					return true
				}
				return false
			})
			if !ok {
				return fmt.Errorf("module[%s] service[%s] fetch gid error[%v]",
					module, h.Conf.Service, err)
			}

			ok = util.CallbackRetry(registerTry, registerTryInterval, func() bool {
				if err := h.cfgHandler.Lock(SectionName, ""); err != nil {
					glog.Warningf("module[%s] service[%s] update lock remote failed[%v]", err)
					return true
				}
				defer h.cfgHandler.Unlock(SectionName, "")
				if err = h.updateGid(id, false); err != nil {
					glog.Warningf("module[%s] service[%s] reuse gid error[%v]",
						module, h.Conf.Service, err)
					return true
				}

				return false
			})
			if ok {
				glog.Infof("module[%s] service[%s] update successfully", module, h.Conf.Service)
				return nil
			} else {
				return fmt.Errorf("module[%s] service[%s] update fail", module, h.Conf.Service)
			}
		}
	}
	glog.Infof("module[%s] service[%s] not exist, try to register", module, h.Conf.Service)

	// 2. increase global id and update info if not exist
	ok = util.CallbackRetry(registerTry, registerTryInterval, func() bool {
		id, err := h.cfgHandler.GetInt(SectionName, module, idName)
		if err != nil && !util.IsNotFound(err) { // id == 0 if err != nil
			glog.Errorf("module[%s] service[%s] get increase id fail[%v]",
				module, h.Conf.Service, err)
			return true
		}

		// do lock
		if err = h.cfgHandler.Lock(SectionName, ""); err != nil {
			glog.Warningf("module[%s] service[%s] register lock remote failed[%v]",
				module, h.Conf.Service, err)
			return true
		}
		defer h.cfgHandler.Unlock(SectionName, "") // unlock

		// double check, get global id again
		id2, err2 := h.cfgHandler.GetInt(SectionName, module, idName)
		if err2 != nil && !util.IsNotFound(err2) { // id == 0 if err != nil
			glog.Errorf("module[%s] service[%s] double check get increase id fail[%v]",
				module, h.Conf.Service, err2)
			return true
		}

		if id2 != id {
			glog.Infof("module[%s] service[%s] double check id2[%d] != id[%d]",
				module, h.Conf.Service, id2, id)
			return true
		}

		// update gid
		if err = h.updateGid(id, true); err != nil {
			glog.Errorf("module[%s] service[%s] update gid error[%v]",
				module, h.Conf.Service, err)
			return true
		}

		return false
	})
	if ok {
		glog.Infof("module[%s] service[%s] register successfully", module, h.Conf.Service)
		return nil
	} else {
		return fmt.Errorf("module[%s] service[%s] register fail", module, h.Conf.Service)
	}
}

func (h *Heartbeat) updateGid(id int, globalUpdate bool) error {
	mp := map[string]interface{}{
		intervalName: h.Conf.Interval,
		updateName:   float64(h.currentTime()), // convert to float64, it's very nasty!
		idName:       float64(id),
	}
	module := string(h.Conf.Module)

	if globalUpdate {
		// set global gid.
		if err := h.cfgHandler.SetItem(SectionName, module, id + 1, idName); err != nil {
			return fmt.Errorf("set increase id[%d] fail[%v]", id + 1, err)
		}
	}

	// set current gid.
	// if error happens here, inconsistency may happen which must be resolved because id is update while
	// registration info isn't.
	if err := h.cfgHandler.SetItem(SectionName, module, mp, h.Conf.Service); err != nil {
		return fmt.Errorf("set register info fail[%v], inconsistence may happen, must be resolved", err)
	}
	return nil
}

func (h *Heartbeat) currentTime() uint32 {
	return uint32(time.Now().Unix())
}

func (h *Heartbeat) updateTime() error {
	return h.cfgHandler.SetItem(SectionName, string(h.Conf.Module), h.currentTime(), h.Conf.Service, updateName)
}

// return int: 0(not exist), 1(unknown), 2(exist)
func (h *Heartbeat) isExist(module string, service string) (map[string]interface{}, int) {
	mp, err := h.cfgHandler.GetMap(SectionName, module, service)
	if err != nil {
		if util.IsNotFound(err) {
			return nil, 0
		}
		return nil, 1
	}
	return mp, 2
}
