/*
// =====================================================================================
//
//       Filename:  dictServer.go
//
//    Description:  用于采集时进行string到int的互转
//
//        Version:  1.0
//        Created:  08/09/2018 11:51:01 PM
//       Revision:  none
//       Compiler:  go1.10.1
//
//         Author:  zhuzhao.cx, zhuzhao.cx@alibaba-inc.com
//        Company:  Alibaba Group
//
// =====================================================================================
*/

package dictServer

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"inspector/config"
	"inspector/util"

	"github.com/golang/glog"
	"reflect"
	"inspector/util/unsafe"
)

const (
	KeyNotFound     = "key not found"
	KeyInProcessing = "local in processing"
	KeyInValid      = "key invalid"

	HandlerInterval = 3000 // ms
	WatcherInterval = 1000 // ms

	sectionName = "dict_server"
	md5Name     = string(util.InnerLeadingMark) + "dict_server_key_md5"

	emptyKey = ""
	emptyMd5 = ""
	// notExistKey int = -1
)

type Conf struct {
	// mongo configuration
	Address    string
	Username   string
	Password   string
	DB         string
	ServerType string // mongo, redis, ...
}

// map string <-> int
type DictServer struct {
	conf            *Conf                  // user configuration
	mp              map[string]string      // long key -> short key
	handlingSet     map[string]struct{}    // set store all handling key
	handlingSetLock sync.Mutex             // lock for handlingSet
	deleteLock      sync.Mutex             // used in delete
	keyList         []string               // value(index) -> key
	sigChan         chan struct{}          // use to close goroutine
	cfgHandler      config.ConfigInterface // configuration handler
}

// if cfgHandler is nil, dictServer will create a new one inside
func NewDictServer(c *Conf, cfgHandler config.ConfigInterface) *DictServer {
	if c.ServerType == "" {
		glog.Error("ServerType can't be empty")
		return nil
	}

	if cfgHandler == nil || reflect.ValueOf(cfgHandler).IsNil() {
		glog.Info("config server input is empty, create new one")
		var err error
		factory := config.ConfigFactory{Name: config.MongoConfigName}
		if cfgHandler, err = factory.Create(c.Address, c.Username, c.Password, c.DB, WatcherInterval); err != nil {
			glog.Errorf("DictServer create cfgHandler error[%v]", err)
			return nil
		}
	} else {
		glog.Info("config server input is not empty, use old one")
	}

	ds := &DictServer{
		cfgHandler: cfgHandler,
		conf:       c,
	}

	// register callback. Without these code, client may read dirty data if no update triggered.
	if err := ds.registerCallback(); err != nil {
		glog.Errorf("DictServer register callback error[%v]", err)
		return nil
	}

	// move some variables' initialization before return
	// ds.mp = new(sync.Map)
	ds.mp = make(map[string]string)
	ds.handlingSet = make(map[string]struct{})
	ds.sigChan = make(chan struct{})

	// load remote data
	ds.loadSetRemote()
	// debugPrintMap(ds.mp.mp)

	// start handler routine
	go ds.handler()

	return ds
}

func (ds *DictServer) Close() {
	close(ds.sigChan)
	// don't close configServer which is not owned
}

// get value, value will be sent to handler if missing
func (ds *DictServer) GetValue(key string) (string, error) {
	// v, ok := ds.mp.Load(key)
	v, ok := ds.mp[key]
	if ok && v != emptyKey {
		return v, nil
	} else {
		// fmt.Println("kkkkk:", key, ok, v)
		// debugPrintMap(ds.mp.mp)
		ds.handlingSetLock.Lock()
		newKey := string(unsafe.String2Bytes(key)) // make a deep copy
		ds.handlingSet[newKey] = struct{}{}
		ds.handlingSetLock.Unlock()
		return emptyKey, fmt.Errorf(KeyNotFound)
	}
}

// get value only, won't trigger add
func (ds *DictServer) GetValueOnly(key string) (string, error) {
	// v, ok := ds.mp.Load(key)
	v, ok := ds.mp[key]
	if  !ok || v == emptyKey {
		return emptyKey, fmt.Errorf(KeyNotFound)
	} else {
		return v, nil
	}
}

// get key, value won't be sent to handler if missing
func (ds *DictServer) GetKey(value string) (string, error) {
	if intVal, err := util.RepString2Int(value); err != nil {
		return "", fmt.Errorf("input key[%s] convert error[%v]", value, err)
	} else if intVal < len(ds.keyList) && ds.keyList[intVal] != KeyInValid {
		return ds.keyList[intVal], nil
	}

	return "", fmt.Errorf(KeyNotFound)
}

func (ds *DictServer) GetKeyList() ([]string, error) {
	mp, err := ds.cfgHandler.GetMap(sectionName, ds.conf.ServerType)
	if err != nil {
		return nil, fmt.Errorf("get key list error[%v]", err)
	}

	ret := make([]string, 0, len(mp))
	for key := range mp {
		if key[0] == util.InnerLeadingMark {
			// inner mark
			continue
		}
		ret = append(ret, key)
	}
	return ret, nil
}

// @deprecated, lock mechanism need be improved
// delete by key, this may fail when several operations concurrence
func (ds *DictServer) Delete(key string) error {
	// find local
	v, ok := ds.mp[key]
	if !ok || v == emptyKey {
		return fmt.Errorf(KeyNotFound)
	}

	// delete local, delete map
	// ds.mp.Store(key, emptyKey)
	ds.mp[key] = emptyKey
	intVal, err := util.RepString2Int(v)
	if err != nil {
		return fmt.Errorf("input key[%s] convert error[%v]", v, err)
	}

	ds.keyList[intVal] = emptyKey // reset keyList
	// generate new md5
	// copyMp := ds.copyMp()
	copyMd5, err := ds.generateLocalMd5(ds.mp)
	if err != nil {
		return fmt.Errorf("generate map[%v] to local md5 error[%v]", ds.mp, err)
	}

	// delete remote
	if err := ds.cfgHandler.Lock(sectionName, ""); err != nil {
		return fmt.Errorf("call Delete lock fail[%v]", err)
	}
	defer ds.cfgHandler.Unlock(sectionName, "")
	// mark as delete
	if err := ds.cfgHandler.SetItem(sectionName, ds.conf.ServerType, emptyKey, key); err != nil {
		return err
	}
	// if crash here, md5 is not update while data is update which lead to dirty data read
	if err := ds.cfgHandler.SetItem(sectionName, ds.conf.ServerType, copyMd5, md5Name); err != nil {
		return err
	}

	return nil
}

// @deprecated, lock mechanism need be improved
// delete all
func (ds *DictServer) DeleteAll() error {
	// lock
	ds.deleteLock.Lock()
	defer ds.deleteLock.Unlock()

	// delete all ds.handlingSet
	ds.handlingSet = make(map[string]struct{})

	// delete local map
	ds.mp = make(map[string]string)
	// ds.mp = new(sync.Map)

	// delete keyList
	for i := range ds.keyList {
		ds.keyList[i] = emptyKey
	}

	if err := ds.cfgHandler.Lock(sectionName, ""); err != nil {
		return fmt.Errorf("DeleteAll lock fail[%v]", err)
	}
	defer ds.cfgHandler.Unlock(sectionName, "")

	// delete remote dict-server content
	return ds.cfgHandler.DeleteSection(sectionName)
}

// handler goroutine, used to register key
func (ds *DictServer) handler() {
	duration := time.Duration(HandlerInterval) * time.Millisecond
	for {
		select {
		case _, ok := <-ds.sigChan:
			if !ok {
				glog.Infof("DictServer handler goroutine close")
				return
			}
		case <-time.NewTicker(duration).C:
			ds.deleteLock.Lock() // not allow delete when handling

			ds.handlingSetLock.Lock() // lock handlingSet
			if len(ds.handlingSet) == 0 {
				ds.handlingSetLock.Unlock()
				ds.deleteLock.Unlock()
				continue
			}
			glog.V(1).Infof("trying update [%v]", ds.handlingSet)

			// copy handlingSet
			copyHandlingSet := make(map[string]struct{}, len(ds.handlingSet))
			for key, val := range ds.handlingSet {
				copyHandlingSet[key] = val
			}
			ds.handlingSetLock.Unlock() // unlock handlingSet

			// get remote map
			remoteMap, err := ds.cfgHandler.GetMap(sectionName, ds.conf.ServerType)
			if err != nil {
				if util.IsNotFound(err) {
					remoteMap = make(map[string]interface{})
				} else {
					glog.Errorf("handler get remoteMap error[%v]", err)
					ds.deleteLock.Unlock()
					continue
				}
			}

			// fmt.Printf("remoteMap: %v\n", remoteMap)
			// fetch remote md5sum
			remoteMd5, ok := remoteMap[md5Name].(string)
			if !ok {
				remoteMd5 = emptyMd5
			}

			// compare copy(ds.mp) and remote
			copyMp := ds.copyMp()
			glog.V(1).Infof("copyMp[%v]", copyMp)
			copyMd5, err := ds.generateLocalMd5(copyMp)
			if err != nil {
				glog.Errorf("generate map[%v] to local md5 error[%v]", copyMp, err)
				ds.clearLocal(nil, nil, nil)
				ds.deleteLock.Unlock()
				continue
			}
			glog.V(1).Infof("copyMd5:%s remoteMd5:%s", copyMd5, remoteMd5)
			if copyMd5 != remoteMd5 {
				// load remote map and clear handlingSet
				ds.clearLocal(remoteMap, nil, copyHandlingSet)
				ds.deleteLock.Unlock()
				continue
			}

			// get remote unused index
			remoteUnused, remoteSize, err := ds.generateRemoteUnused(remoteMap)
			if err != nil {
				glog.Errorf("generate remote unused list error[%v]", err)
				ds.clearLocal(nil, nil, nil)
				ds.deleteLock.Unlock()
				continue
			}
			glog.V(1).Infof("remoteUnused[%v] remoteSize[%v]", remoteUnused, remoteSize)

			// generate local map: from ds.mp and copyHandlingSet
			localMap := ds.generateLocalMap(copyMp, remoteUnused, remoteSize, copyHandlingSet)
			glog.V(1).Infof("localMap[%v]", localMap)

			// try to lock
			if err := ds.cfgHandler.Lock(sectionName, ""); err != nil {
				glog.Info("handler lock fail")
				// do nothing when lock fail
				ds.clearLocal(nil, nil, nil)
				ds.deleteLock.Unlock()
				continue
			}

			// get remote map again, double check
			remoteMd5Double, err := ds.cfgHandler.GetString(sectionName, ds.conf.ServerType, md5Name)
			if err != nil && !util.IsNotFound(err) {
				glog.Errorf("handler double check get md5 value error[%v]", err)
				ds.clearLocal(nil, nil, nil)
				ds.cfgHandler.Unlock(sectionName, "")
				ds.deleteLock.Unlock()
				continue
			}
			// fmt.Printf("remoteMd5Double:%v remoteMd5:%v\n", remoteMd5Double, remoteMd5)
			// remote double check md5 != remote md5, no need to continue when remote md5 is changed,
			// discard local change
			if remoteMd5Double != remoteMd5 {
				// remote is update, fetch remote and update local
				glog.Infof("remote is updated[%v], previous[%v]", remoteMd5Double, remoteMd5)
				remoteMap, err := ds.cfgHandler.GetMap(sectionName, ds.conf.ServerType)
				if err != nil {
					glog.Errorf("handler double check fetch remote map error[%v]", err)
					ds.clearLocal(nil, nil, nil)
				} else {
					ds.clearLocal(remoteMap, nil, copyHandlingSet)
				}

				ds.cfgHandler.Unlock(sectionName, "")
				ds.deleteLock.Unlock()
				continue
			}

			// fmt.Printf("localMp:%v\n", localMap)
			// generate new map with md5sum to write to remote
			localMd5, err := ds.generateLocalMd5(localMap)
			if err != nil {
				glog.Errorf("generate map[%v] to local md5 error[%v]", copyMp, err)
				ds.clearLocal(nil, nil, nil)
				ds.cfgHandler.Unlock(sectionName, "")
				ds.deleteLock.Unlock()
				continue
			}

			newMap := ds.generateRemoteMap(localMap, localMd5)
			if err := ds.cfgHandler.SetItem(sectionName, ds.conf.ServerType, newMap); err != nil {
				glog.Errorf("handler set map error[%v]", err)
				ds.clearLocal(nil, nil, nil)
				ds.cfgHandler.Unlock(sectionName, "")
				ds.deleteLock.Unlock()
				continue
			}

			glog.Info("handler set map successfully")
			ds.clearLocal(nil, localMap, copyHandlingSet) // use localMap update ds.mp
			ds.cfgHandler.Unlock(sectionName, "")
			ds.deleteLock.Unlock()
		}
	}
}

// no need to register callback
func (ds *DictServer) registerCallback() error {
	callback := func(event config.WatcheEvent) error {
		glog.Info("dictServer callback called")
		// don't allow to call delete in callback
		ds.deleteLock.Lock()
		defer ds.deleteLock.Unlock()
		/*
		 * this callback will be called when changed even though current ds makes this change,
		 * so in this case, the current mp will be set twice which is acceptable.
		 */
		ret, err := ds.cfgHandler.GetMap(sectionName, ds.conf.ServerType)
		if err != nil {
			if util.IsNotFound(err) == false {
				glog.Errorf("callback get data error[%v]", err)
			}
			return err
		}

		// cover local map no matter same or not and reset ds.handlingSet
		// it may remove some keys that just be added and haven't been handled.
		// but it's acceptable that it will be added into handleSet again when user call ds.GetValue().
		ds.clearLocal(ret, nil, ds.handlingSet)
		return nil
	}
	watcherChange := &config.Watcher{
		Event:   config.NODECHANGED,
		Handler: callback,
	}
	if err := ds.cfgHandler.RegisterGlobalWatcher(sectionName, ds.conf.ServerType, watcherChange); err != nil {
		return nil
	}

	watcherCreate := &config.Watcher{
		Event:   config.NODECREATED,
		Handler: callback,
	}
	return ds.cfgHandler.RegisterGlobalWatcher(sectionName, ds.conf.ServerType, watcherCreate)
}

// get next available index
func (ds *DictServer) getNextIndex(remoteUnused *[]int, remoteSize *int) int {
	// get from recycle list if not empty
	if len(*remoteUnused) != 0 {
		front := (*remoteUnused)[0]
		*remoteUnused = (*remoteUnused)[1:]
		return front
	} else {
		*remoteSize = *remoteSize + 1
		return *remoteSize - 1
	}
}

func (ds *DictServer) copyMp() map[string]string {
	// copy ds.mp
	localMp := make(map[string]string)
	for key, val := range ds.mp {
		localMp[key] = val
	}
	//ds.mp.Range(func(key interface{}, val interface{}) bool {
	//	localMp[key.(string)] = val.(string)
	//	return true
	//})
	return localMp
}

func (ds *DictServer) generateLocalMap(localMp map[string]string, remoteUnused []int,
		remoteSize int, copyHandlingSet map[string]struct{}) map[string]string {
	for key := range copyHandlingSet {
		nxt := ds.getNextIndex(&remoteUnused, &remoteSize)
		localMp[key] = util.RepInt2String(nxt)
	}
	return localMp
}

// generate md5sum base on given map, return string type
func (ds *DictServer) generateLocalMd5(mp map[string]string) (string, error) {
	le := len(mp) // local map size == max index
	if le == 0 {
		return emptyMd5, nil
	}
	ans := make([]string, le)
	for key, val := range mp {
		if val != emptyKey { // md5 only calculate from exist key
			valInt, _ := util.RepString2Int(val)
			if valInt < 0 || valInt >= le {
				return "", fmt.Errorf("convert long-key[%s] short-key[%s] to int[%d] invalid[max:[%v]]",
					key, val, valInt, le)
			}
			ans[valInt] = key
		}
	}

	// fmt.Printf("ans: %v\n", ans)
	join := strings.Join(ans, ";")
	md5 := util.Md5([]byte(join))
	// return unsafe.Bytes2String(md5[:])
	return string(md5[:]), nil
}

// generate new map that will be set to remote
func (*DictServer) generateRemoteMap(localMap map[string]string, localMd5 string) map[string]interface{} {
	newMap := make(map[string]interface{})
	for key, val := range localMap {
		newMap[key] = val
	}
	newMap[md5Name] = localMd5

	return newMap
}

func (ds *DictServer) generateRemoteUnused(remoteMap map[string]interface{}) ([]int, int, error) {
	n := ds.getMaxIndex(remoteMap)
	vis := make([]bool, n)
	for key, val := range remoteMap {
		valS := val.(string)
		if util.FilterName(key)|| valS == emptyKey {
			continue
		}

		if v, err := util.RepString2Int(valS); err != nil {
			return nil, 0, fmt.Errorf("convert short key[%s] to int error[%v]", valS, err)
		} else if v >= n {
			return nil, 0, fmt.Errorf("convert short key[%s] value[%d] bigger than n[%d]", valS, v, n)
		} else {
			vis[v] = true
		}
	}

	var ans []int
	for i := 0; i < n; i++ {
		if vis[i] == false {
			ans = append(ans, i)
		}
	}
	return ans, n, nil
}

/*
 * only one of remoteMap and localMap can be given, means update ds.mp from remote or local.
 * If no parameters given, mp won't change.
 */
func (ds *DictServer) clearLocal(remoteMap map[string]interface{}, localMap map[string]string,
	copyHandlingSet map[string]struct{}) {
	// convert remote map to local map at beginning
	if remoteMap != nil {
		localMap = make(map[string]string)
		for key, val := range remoteMap {
			if util.FilterName(key) {
				continue
			}
			localMap[key] = val.(string)
		}
	}

	// remove ds.handlingSet element from copyHandlingSet
	if copyHandlingSet != nil {
		ds.handlingSetLock.Lock()
		for key := range copyHandlingSet {
			delete(ds.handlingSet, key)
		}
		ds.handlingSetLock.Unlock()
	}

	if localMap != nil {
		//ds.mp = new(sync.Map)
		//for key, val := range localMap {
		//	ds.mp.Store(key, val)
		//}
		ds.mp = localMap
	}

	// re-generate keyList if changed
	if remoteMap != nil || localMap != nil {
		n := len(localMap)
		keyList := make([]string, n)
		//ds.mp.Range(func(key interface{}, val interface{}) bool {
		//	if v, err := util.RepString2Int(val.(string)); err == nil {
		//		keyList[v] = key.(string)
		//	}
		//	return true
		//})
		for key, val := range ds.mp {
			if v, err := util.RepString2Int(val); err == nil {
				keyList[v] = key
			}
		}
		ds.keyList = keyList[:]
	}
}

func (*DictServer) getMaxIndex(remoteMap map[string]interface{}) int {
	n := len(remoteMap) // length is useful
	if _, ok := remoteMap[md5Name]; ok {
		n--
	}
	return n
}

// load remote data and set local, this only used at beginning
func (ds *DictServer) loadSetRemote() {
	remoteMap, err := ds.cfgHandler.GetMap(sectionName, ds.conf.ServerType)
	if err != nil {
		glog.Warningf("loadSetRemote error[%v]", err)
	} else {
		ds.clearLocal(remoteMap, nil, nil)
		glog.Info("initial remote map: ", remoteMap)
		glog.Info("loadSetRemote successfully")
	}
}

func debugPrintMap(mp map[string]string) {
	//mp.Range(func(key, value interface{}) bool {
	//	fmt.Println("2kkkkk: ", key, value)
	//	return true
	//})
	for key, val := range mp {
		fmt.Println("2kkkkk: ", key, val)
	}
}