/*
// =====================================================================================
//
//       Filename:  MongoConfig.go
//
//    Description:  用MongoDB的形式实现Config接口
//
//        Version:  1.0
//        Created:  07/06/2018 01:48:01 PM
//       Revision:  none
//       Compiler:  go1.10.3
//
//         Author:  zhuzhao.cx, zhuzhao.cx@alibaba-inc.com
//        Company:  Alibaba Group
//
// =====================================================================================
*/

// Pay attention: not support add dot(".") in key and path!!!
package config

import (
	"encoding/binary"
	json2 "encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"inspector/client"
	"inspector/util/whatson"

	"github.com/golang/glog"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"gopkg.in/ini.v1"
	"inspector/util"
	"reflect"
)

const (
	// DB name
	// RecordDB = "inspectorConfig"
	// collection lock name
	RecordCollectionLock = string(util.InnerLeadingMark) + "inspector_collection_lock"
	// unique key in lock-collection
	RecordCollectionLockUniqueKey = string(util.InnerLeadingMark) + "inspector_collection_lock_uk"
	// key lock name
	RecordKeyLock = "inspector_key_lock"
	// key lock time
	RecordKeyLockTime = "inspector_key_lock_time"
	// timeout of lock, unit is second
	RecordLockTimeout = 10 // seconds
	// unique key
	RecordCollectionUK = "key_unique"

	OpSet   = "$set"
	OpOr    = "$or"
	OpAnd   = "$and"
	OpLe    = "$lte"
	OpExist = "$exists"
	FiledId = "_id"

	PathSplitter      = ";"
	NamespaceSplitter = "."

	emptyKey = ""
	allKeys  = "all_key"
)

/*
 * Use MongoDB as config center. The db name is "Inspector", and collection match to section.
 * In each collection, we store redundancy "key-unique" for convenience
 * lookup: {_id: xxx, "key-unique": key, key: {...}}
 * watcherMap store the set that need to be monitor
 */
type MongoConfig struct {
	Parser          whatson.Parser
	address         string
	username        string
	password        string
	db              string
	session         *mgo.Session
	watcherInterval int
	watcherMap
}

type watcherMap struct {
	// key map: section -> key(may be empty) -> event -> handler
	registerMap *sync.Map
	// registerMap map[string]map[string]map[WatcheEvent]func(event WatcheEvent) error
	// lock        sync.RWMutex
	sigChan chan struct{} // used to close

}

type node struct {
	// type
	tp whatson.ValueType
	// value
	value interface{}
}

func (mc *MongoConfig) Init() ConfigInterface {
	glog.Infof("start mongo config server scheduler")

	return mc
}

func (mc *MongoConfig) ConnectString(input string) ConfigInterface {
	mc.address = input
	return mc
}

func (mc *MongoConfig) Username(input string) ConfigInterface {
	mc.username = input
	return mc
}

func (mc *MongoConfig) Password(input string) ConfigInterface {
	mc.password = input
	return mc
}

func (mc *MongoConfig) DB(input string) ConfigInterface {
	mc.db = input
	return mc
}

// unit: ms
func (mc *MongoConfig) SetWatcherStep(watcherInterval int) ConfigInterface {
	mc.watcherInterval = watcherInterval
	return mc
}

func (mc *MongoConfig) EstablishConnect() error {
	glog.Infof("start establishing connection[%s]", mc.address)
	if mc.Parser == nil {
		return fmt.Errorf("parser can't be null")
	}

	client, err := client.NewMongoClient().
		ConnectString(mc.address).
		Username(mc.username).
		Password(mc.password).
		UseDB(mc.db).
		SetOpt(map[string]interface{}{
			"SafeMode":        "majority",
			"ConsistencyMode": "Monotonic",
			"PoolLimit":       256,
			"Timeout":         time.Duration(10 * time.Minute),
		}).
		EstablishConnect()
	if err != nil {
		return err
	}

	session := client.GetSession()
	if session == nil {
		return fmt.Errorf("session is empty")
	}

	ok := true
	if mc.session, ok = session.(*mgo.Session); !ok {
		return fmt.Errorf("session illegal")
	}

	if mc.watcherInterval > 0 {
		// start watcher before return
		// mc.registerMap = make(map[string]map[string]map[WatcheEvent]func(event WatcheEvent) error)
		mc.registerMap = new(sync.Map)
		mc.sigChan = make(chan struct{})
		mc.startWatcher()
	}

	return nil
}

func (mc *MongoConfig) Close() {
	if mc.watcherInterval > 0 {
		close(mc.sigChan)
	}
}

func (mc *MongoConfig) GetSectionList() ([]string, error) {
	glog.V(3).Info("execute GetSectionList")

	arr, err := mc.session.DB(mc.db).CollectionNames()
	if err != nil {
		return nil, err
	}

	ret := make([]string, 0)
	for _, ele := range arr {
		if util.FilterName(ele) == false {
			ret = append(ret, ele)
		}
	}
	return ret, nil
}

func (mc *MongoConfig) GetKeyList(section string) ([]string, error) {
	glog.V(3).Infof("execute GetKeyList section[%s]", section)

	// recover when meets panic especially in bson parse.
	defer func() {
		if p := recover(); p != nil {
			err := fmt.Errorf("GetKeyList internal error[%v]", p)
			glog.Errorf(err.Error())
		}
	}()

	var ret []string
	var element bson.Raw
	it := mc.session.DB(mc.db).C(section).Find(bson.M{}).Iter()
	defer it.Close()

	// must use address here
	for it.Next(&element) {
		if s, err := mc.Parser.Get(element.Data, RecordCollectionUK); err != nil {
			return nil, err
		} else {
			name := string(s)
			if util.FilterName(name) == false {
				ret = append(ret, name)
			}
		}
	}
	return ret, nil
}

func (mc *MongoConfig) GetSection(section string) (map[string]string, error) {
	glog.V(3).Infof("execute GetSection section[%s]", section)

	return nil, fmt.Errorf("not supported current")
}

func (mc *MongoConfig) GetString(section, key string, path ...string) (string, error) {
	glog.V(3).Infof("execute GetString section[%s] key[%s] path[%v]", section, key, path)

	ret, err := mc.getValue(section, key, path...)
	if err != nil {
		return "", err
	}
	return string(ret), nil
}

func (mc *MongoConfig) GetBool(section, key string, path ...string) (bool, error) {
	glog.V(3).Infof("execute GetBool section[%s] key[%s] path[%v]", section, key, path)

	ret, err := mc.getValue(section, key, path...)
	if err != nil {
		return false, err
	}
	return ret[0] == 1, nil
}

func (mc *MongoConfig) GetInt(section, key string, path ...string) (int, error) {
	glog.V(3).Infof("execute GetInt section[%s] key[%s] path[%v]", section, key, path)

	ret, err := mc.GetFloat64(section, key, path...)
	if err != nil {
		return 0, err
	}

	return int(ret), nil
}

func (mc *MongoConfig) GetUint(section, key string, path ...string) (uint, error) {
	glog.V(3).Infof("execute GetInt section[%s] key[%s] path[%v]", section, key, path)

	ret, err := mc.GetFloat64(section, key, path...)
	if err != nil {
		return 0, err
	}

	return uint(ret), nil
}

// not support, value will be trancated
func (mc *MongoConfig) GetInt64(section, key string, path ...string) (int64, error) {
	glog.V(3).Infof("execute GetInt64 section[%s] key[%s] path[%v]", section, key, path)

	ret, err := mc.GetFloat64(section, key, path...)
	if err != nil {
		return 0, err
	}

	return int64(ret), nil
}

func (mc *MongoConfig) GetFloat32(section, key string, path ...string) (float32, error) {
	glog.V(3).Infof("execute GetFloat32 section[%s] key[%s] path[%v]", section, key, path)

	ret, err := mc.GetFloat64(section, key, path...)
	if err != nil {
		return 0, err
	}

	return float32(ret), nil
}

func (mc *MongoConfig) GetFloat64(section, key string, path ...string) (float64, error) {
	glog.V(3).Infof("execute GetFloat64 section[%s] key[%s] path[%v]", section, key, path)

	ret, err := mc.getValue(section, key, path...)
	if err != nil {
		return 0, err
	}

	return math.Float64frombits(binary.LittleEndian.Uint64(ret)), nil
}

// this interface is not good enough which only return string array, actually, we support any type.
func (mc *MongoConfig) GetArray(section, key string, path ...string) ([]string, error) {
	glog.V(3).Infof("execute GetArray section[%s] key[%s] path[%v]", section, key, path)

	ret, err := mc.getValue(section, key, path...)
	if err != nil {
		return nil, err
	}

	/*
	 * The return of array is a bson document which needs to parse. At first, I want to
	 * parse the array into a specific format, however it needs user to known this format
	 * first and then to parse it based on this format.
	 * So my format is: length1+type1+value1+length2+type1+value2+....
	 * Here '+' is only the connection symbol not in the real byte array.
	 */
	var resArr []string
	for i := 0; i < len(ret); {
		// parse length
		length := int(binary.LittleEndian.Uint32(ret[i: i+4]))
		i += 5 // length + type
		// parse body
		resArr = append(resArr, string(ret[i: i + length]))
		i += length
	}
	return resArr, nil
}

// all the integer value type in then return map is float64
func (mc *MongoConfig) GetMap(section, key string, path ...string) (map[string]interface{}, error) {
	glog.V(3).Infof("execute GetMap section[%s] key[%s] path[%v]", section, key, path)

	ret, err := mc.getValue(section, key, path...)
	if err != nil {
		return nil, err
	}

	return mc.exportSingleMap(ret)
}

func (mc *MongoConfig) GetBytes(section, key string, path ...string) ([]byte, error) {
	return mc.getValue(section, key, path...)
}

/*
 * pay attention: dot(.) is special character in mongo so we must convert "." to "\." first in
 * all input string
 */
//func (mc *MongoConfig) SetItem(section, key string, value interface{}, path ...string) error {
//	newSection := strings.Replace(section, NamespaceSplitter, NamespaceSplitterConvert, -1)
//	newKey := strings.Replace(key, NamespaceSplitter, NamespaceSplitterConvert, -1)
//	var newPath = make([]string, len(path))
//	for i, p := range path {
//		newPath[i] = strings.Replace(p, NamespaceSplitter, NamespaceSplitterConvert, -1)
//	}
//	return mc.setItem(newSection, newKey, value, newPath...)
//}

/*
 * please pay a lot attention that if store map, the value type won't be transfer, otherwise number will
 * will be convert to float64
 */
func (mc *MongoConfig) SetItem(section, key string, value interface{}, path ...string) error {
	glog.V(3).Infof("execute SetItem section[%s] key[%s] path[%v] value[%v]", section, key, path, value)

	collectionHandler := mc.session.DB(mc.db).C(section)

	// create unique index first
	err := mc.ensureIndex(collectionHandler)
	if err != nil {
		return fmt.Errorf("create unique key meets error[%v]", err)
	}

	// doesn't support int64, convert all number to float64 because json only support float64
	var newValue interface{}
	switch src := value.(type) {
	case int64:
		return fmt.Errorf("not suppot int64")
	case uint64:
		return fmt.Errorf("not suppot uint64")
	case int32:
		newValue = float64(src)
	case uint32:
		newValue = float64(src)
	case int16:
		newValue = float64(src)
	case uint16:
		newValue = float64(src)
	case int8:
		newValue = float64(src)
	case uint8:
		newValue = float64(src)
	case int:
		newValue = float64(src)
	case uint:
		newValue = float64(src)
	case float32:
		newValue = float64(src)
	default:
		newValue = value
	}

	path = append([]string{key}, path...)
	namespace := mc.convertPath2Namespace(path)
	selector := bson.M{RecordCollectionUK: key}
	update := bson.M{OpSet: bson.M{namespace: newValue}}
	_, err = collectionHandler.Upsert(selector, update)
	if err != nil {
		return err
	}

	return nil
}

// SetItem also support SetArray in deep path
func (mc *MongoConfig) SetArray(section, key string, array []string) error {
	glog.V(3).Infof("execute SetArray section[%s] key[%s] array[%v]", section, key, array)

	collectionHandler := mc.session.DB(mc.db).C(section)

	selector := bson.M{RecordCollectionUK: key}
	update := bson.M{OpSet: bson.M{key: array}}
	_, err := collectionHandler.Upsert(selector, update)
	if err != nil {
		return err
	}

	return nil
}

// support set map
func (mc *MongoConfig) setRemoteMap(section, key string, input interface{}) error {
	glog.V(3).Infof("execute setRemoteMap section[%s] key[%s] input[%v]", section, key, input)

	collectionHandler := mc.session.DB(mc.db).C(section)

	selector := bson.M{RecordCollectionUK: key}
	update := bson.M{OpSet: bson.M{key: input}}
	_, err := collectionHandler.Upsert(selector, update)
	if err != nil {
		return err
	}

	return nil
}

func (mc *MongoConfig) DeleteAll() error {
	glog.V(3).Info("execute DeleteAll")

	dbHandler := mc.session.DB(mc.db)
	return dbHandler.DropDatabase()
}

func (mc *MongoConfig) DeleteItem(section, key string, path ...string) error {
	glog.V(3).Infof("execute DeleteItem section[%s] key[%s] path[%v]", section, key, path)

	collectionHandler := mc.session.DB(mc.db).C(section)

	path = append([]string{key}, path...)
	path = path[:len(path)-1] // remove last one
	selector := bson.M{RecordCollectionUK: key}
	var update bson.M
	if len(path) == 0 {
		return collectionHandler.Remove(selector)
	}
	update = bson.M{OpSet: mc.convertPath2BsonM(path)}
	return collectionHandler.Update(selector, update)
}

func (mc *MongoConfig) DeleteSection(section string) error {
	glog.V(3).Infof("execute DeleteSection section[%s]", section)

	collectionHandler := mc.session.DB(mc.db).C(section)
	return collectionHandler.DropCollection()
}

/*
 * Currently, only support lock section. non-blocking
 */
func (mc *MongoConfig) Lock(section, key string, path ...string) error {
	glog.V(3).Infof("execute Lock section[%s] key[%s] path[%v]", section, key, path)

	if key != "" || len(path) != 0 {
		return fmt.Errorf("only support lock section currently")
	}
	/*
		if key == "" {
			return mc.lockSection(section)
		}
		return mc.lockKey(section, key)
	*/
	return mc.lockSection(section)
}

/*
 * Unlock the section. non-blocking
 */
func (mc *MongoConfig) Unlock(section, key string, path ...string) {
	glog.V(3).Infof("execute Unlock section[%s] key[%s] path[%v]", section, key, path)

	if key != "" || len(path) != 0 {
		glog.Warningf("only support unlock section currently")
		return
	}

	var collectionHandler *mgo.Collection
	/*
		if key == "" {
			// lock section
			collectionHandler = mc.session.DB(mc.db).C(RecordCollectionLock)
			key = RecordCollectionLockUniqueKey
		} else {
			// lock key
			collectionHandler = mc.session.DB(mc.db).C(section)
		}*/
	collectionHandler = mc.session.DB(mc.db).C(section)
	key = RecordCollectionLockUniqueKey

	selector := bson.M{
		RecordCollectionUK: key,
	}
	update := bson.M{
		OpSet: bson.M{
			RecordKeyLock: false,
		},
	}
	collectionHandler.Update(selector, update)
}

/*
 * main function of watcher.
 * Pay attention: the callback maybe called only once because watchAll will handle different key
 * respectively.
 */
func (mc *MongoConfig) startWatcher() {
	glog.Info("execute startWatcher")

	localSectionMap := make(map[string]interface{})
	var (
		remote interface{}
		local  interface{}
		err    error
		ok     bool
	)

	// do update local if diff happens
	handleDiff := func(event WatcheEvent, section, key string, remote interface{}) {
		switch event {
		case NODEDELETED:
			if _, ok := localSectionMap[section]; !ok {
				return
			}
			delete(localSectionMap[section].(map[string]interface{}), key)
		case NODECREATED:
			fallthrough
		case NODECHANGED:
			if _, ok := localSectionMap[section]; !ok {
				localSectionMap[section] = make(map[string]interface{})
			}
			keyLayer := localSectionMap[section].(map[string]interface{})
			keyLayer[key] = remote
		}
	}

	// handle callback
	handleCallback := func(event WatcheEvent, currentKeyMap, allKeyMap *sync.Map) {
		// call current key map callback function
		if currentKeyMap != nil {
			if callback, ok := currentKeyMap.Load(event); ok {
				cb := callback.(func(event WatcheEvent) error)
				cb(event)
			}
			if callback, ok := currentKeyMap.Load(NODEALL); ok {
				cb := callback.(func(event WatcheEvent) error)
				cb(NODEALL)
			}
		}

		// call all-key map callback function
		if allKeyMap != nil {
			if callback, ok := allKeyMap.Load(event); ok {
				cb := callback.(func(event WatcheEvent) error)
				cb(event)
			}
			if callback, ok := allKeyMap.Load(NODEALL); ok {
				cb := callback.(func(event WatcheEvent) error)
				cb(NODEALL)
			}
		}
	}

	// judge single key
	judgeSingleKey := func(section, key string, currentKeyMap, allKeyMap *sync.Map) bool {
		// fetch remote
		remote, err = mc.getValue(section, key)

		// fetch local
		keyLayer, ok2 := localSectionMap[section].(map[string]interface{})
		if !ok2 {
			ok = false
		} else {
			local, ok = keyLayer[key]
		}

		if err != nil && !util.IsNotFound(err) {
			glog.Errorf("watcher get map section[%v] key[%v] error[%v]", section, key, err)
			return false
		}
		// fmt.Printf("local:%v remote:%v\n", local, remote)

		// remote not exist
		if err != nil && util.IsNotFound(err) {
			if !ok { // local not exist
				// same
				return false
			} else {
				// local exist
				glog.Infof("watcher section[%v] key[%v] remote deleted", section, key)

				handleCallback(NODEDELETED, currentKeyMap, allKeyMap)
				handleDiff(NODEDELETED, section, key, nil)
			}
		} else { // remote exist
			if !ok {
				// local not exist
				glog.Infof("watcher section[%v] key[%v] remote created", section, key)

				handleCallback(NODECREATED, currentKeyMap, allKeyMap)
				handleDiff(NODECREATED, section, key, remote)
			} else {
				// local exist
				if reflect.DeepEqual(local, remote) == false {
					glog.Infof("watcher section[%v] key[%v] remote updated", section, key)
					handleCallback(NODECHANGED, currentKeyMap, allKeyMap)
					handleDiff(NODECHANGED, section, key, remote)
				}
			}
		}
		return true
	}

	// main job
	periodicalJob := func() {
		// mc.lock.RLock()
		mc.registerMap.Range(func(key1, value1 interface{}) bool {
			section := key1.(string)
			sectionMap := value1.(*sync.Map)

			// judge all-key exists
			if value2, ok := sectionMap.Load(allKeys); ok {
				// all-key exists
				// fetch all remote keys and do iterate respectively
				keyList, err := mc.GetKeyList(section)
				if err != nil {
					glog.Errorf("watcher get section[%s] key lists error[%v]", section, err)
					return true
				}

				// fetch local keys that not exist on the remote key map and add into keyList.
				// this is for "delete".
				if v, ok := localSectionMap[section]; ok {
					// generate remote key map
					remoteKeyMap := make(map[string]struct{}, len(keyList))
					for _, key := range keyList {
						remoteKeyMap[key] = struct{}{}
					}

					localKeyMap := v.(map[string]interface{})
					for key := range localKeyMap {
						if _, ok = remoteKeyMap[key]; !ok {
							keyList = append(keyList, key)
						}
					}
				}

				allKeyMap := value2.(*sync.Map)
				for _, key := range keyList {
					var currentKeyMap *sync.Map
					if keyMap, ok := sectionMap.Load(key); ok {
						currentKeyMap = keyMap.(*sync.Map)
					}
					judgeSingleKey(section, key, currentKeyMap, allKeyMap)
				}
			} else {
				// all-key isn't exists, so we only iterate keys that registered.
				sectionMap.Range(func(key2, value2 interface{}) bool {
					key := key2.(string)
					keyMap := value2.(*sync.Map)

					judgeSingleKey(section, key, keyMap, nil)
					return true
				})
			}
			return true
		})
		// mc.lock.RUnlock()
	}

	go func() {
		// periodical job
		interval := time.Duration(mc.watcherInterval) * time.Millisecond
		for {
			select {
			case _, ok := <-mc.sigChan:
				if !ok {
					glog.Infof("configServer watcher closed")
					return
				}
			case <-time.NewTicker(interval).C:
				periodicalJob()
			}
		}

		glog.Errorf("Error happens, watcher exit")
	}()
}

// only support watch section/key, path is useless here currently. Not support watching all sections.
// pay attention: watching all keys is very time consuming.
func (mc *MongoConfig) RegisterGlobalWatcher(section, key string, watcher *Watcher, path ...string) error {
	glog.V(3).Infof("execute RegisterGlobalWatcher: section[%s] key[%s] path[%v]", section, key, path)
	if mc.watcherInterval <= 0 {
		return fmt.Errorf("not enable watcher")
	}

	if section == "" {
		return fmt.Errorf("not support watch all sections")
	}

	if key == "" {
		key = allKeys
	}

	// mc.lock.Lock()
	ret, ok := mc.registerMap.Load(section)
	if !ok {
		// mc.registerMap.Store(section, make(map[string]map[WatcheEvent]func(event WatcheEvent) error))
		ret = new(sync.Map)
		mc.registerMap.Store(section, ret)
	}

	sectionLayer := ret.(*sync.Map)
	ret, ok = sectionLayer.Load(key)
	if !ok {
		// sectionLayer[key] = make(map[WatcheEvent]func(event WatcheEvent) error)
		ret = new(sync.Map)
		sectionLayer.Store(key, ret)
	}

	keyLayer := ret.(*sync.Map)
	if _, ok := keyLayer.Load(watcher.Event); !ok {
		keyLayer.Store(watcher.Event, watcher.Handler)
	}
	// mc.lock.Unlock()
	return nil
}

// only support watch section/key, path is useless here currently. key empty means monitor section
func (mc *MongoConfig) RemoveGlobalWatcher(section, key string, path ...string) error {
	glog.V(3).Infof("execute RemoveGlobalWatcher: section[%s] key[%s] path[%v]", section, key, path)
	if mc.watcherInterval <= 0 {
		return fmt.Errorf("not enable watcher")
	}

	ret, ok := mc.registerMap.Load(section)
	if !ok {
		return fmt.Errorf("section[%s] not exist", section)
	}

	// if key == "", delete whole section
	if key == emptyKey {
		mc.registerMap.Delete(section)
		return nil
	}

	sectionLayer := ret.(*sync.Map)
	if _, ok := sectionLayer.Load(key); !ok {
		return fmt.Errorf("section[%s] key[%s] not exist", section, key)
	}

	sectionLayer.Delete(key)
	return nil
}

func (mc *MongoConfig) Import(filename string) error {
	glog.Infof("execute Import file[%s]", filename)

	f, err := ini.Load(filename)
	if err != nil {
		return fmt.Errorf("load ini file[%s] error[%v]", filename, err)
	}

	sections := f.Sections()
	glog.Infof("import file[%s] with sections[%v]", filename, sections)
	for _, section := range sections {
		sectionName := section.Name()
		for _, key := range section.Keys() {
			keyName := key.Name()
			value := key.Value()

			var valueJson interface{}
			// json parse value to map
			if err = json2.Unmarshal([]byte(value), &valueJson); err != nil {
				return fmt.Errorf("file[%s], section[%s], key[%s], value[%s], json unmarshal error[%v]",
					filename, sectionName, keyName, value, err)
			}

			// write map to mongodb
			if err = mc.setRemoteMap(sectionName, keyName, valueJson); err != nil {
				return fmt.Errorf("file[%s], section[%s], key[%s], value[%s], set map error[%v]",
					filename, sectionName, keyName, value, err)
			}
		}
	}
	return nil
}

func (mc *MongoConfig) Export(filename string) error {
	glog.Infof("execute Export file[%s]", filename)

	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("open/create file[%s] error[%v]", filename, err)
	}

	mp, err := mc.exportWholeMap()
	if err != nil {
		return err
	}

	// convert map to ini
	for section, collection := range mp {
		// write file
		s := fmt.Sprintf("[%s]\n", section)
		if _, err := f.WriteString(s); err != nil {
			return fmt.Errorf("write string[%s] into file[%s] error[%v]", s, filename, err)
		}
		for key, value := range collection.(map[string]interface{}) {
			// convert to json
			json, err := json2.Marshal(value)
			if err != nil {
				return fmt.Errorf("convert map[%v] to json error[%v]", value, err)
			}

			// write file
			s := fmt.Sprintf("%s = %s\n", key, json)
			if _, err := f.WriteString(s); err != nil {
				return fmt.Errorf("write string[%s] into file[%s] error[%v]", s, filename, err)
			}
		}
		// write file
		s = "\n"
		if _, err := f.WriteString(s); err != nil {
			return fmt.Errorf("write string[%s] into file[%s] error[%v]", s, filename, err)
		}
	}
	return nil
}

// export as map
func (mc *MongoConfig) exportWholeMap() (map[string]interface{}, error) {
	glog.V(3).Info("execute exportWholeMap")

	rootMap := make(map[string]interface{})

	// get all section list
	sectionList, err := mc.GetSectionList()
	if err != nil {
		return nil, fmt.Errorf("get section list error[%v]", err)
	}

	// iterate all sections
	for _, section := range sectionList {
		var element bson.Raw
		it := mc.session.DB(mc.db).C(section).Find(bson.M{}).Iter()
		defer it.Close() // acceptable
		rootMap[section] = make(map[string]interface{})
		sectionMap := rootMap[section].(map[string]interface{})
		for it.Next(&element) {
			// parse key of current document
			mp, err := mc.exportSingleMap(element.Data)
			if err != nil {
				return nil, fmt.Errorf("do exportSingleMap error[%v] in section[%s]",
					err, section)
			}

			key, ok := mp[RecordCollectionUK].(string)
			if !ok || util.FilterName(key) {
				continue
			}
			sectionMap[key] = mp[key]
		}
	}
	return rootMap, nil
}

func (mc *MongoConfig) exportSingleMap(input []byte) (map[string]interface{}, error) {
	glog.V(3).Info("execute exportSingleMap")
	var mp map[string]*node

	// callback function. same with callback in exportWholeMap
	callback := func(keyPath []string, value []byte, valueType whatson.ValueType) error {
		now := mp
		lengthPath := len(keyPath)
		for i, key := range keyPath {
			// others
			if i == lengthPath-1 {
				now[key] = &node{valueType, mc.Parser.ValueType2Interface(valueType, value)}
			} else {
				if _, ok := now[key]; !ok {
					now[key] = &node{whatson.DOCUMENT, make(map[string]interface{})}
				}
				// type assert of value
				if _, ok := now[key].value.(map[string]*node); !ok {
					// change to map[string]interface{} if type is document
					if now[key].tp == whatson.DOCUMENT || now[key].tp == whatson.ARRAY {
						now[key].value = make(map[string]*node)
					} else {
						return fmt.Errorf("parse error[type assert error]")
					}
				}
				now = now[key].value.(map[string]*node)
			}
		}

		return nil
	}

	mp = make(map[string]*node)
	if err := mc.Parser.Parse(input, callback); err != nil {
		return nil, fmt.Errorf("parse data[%v] meets error[%v]", input, err)
	}

	// construct fake root node
	fakeNode := &node{
		tp:    whatson.DOCUMENT,
		value: mp,
	}
	// use dfs to change map format
	changeMp, err := mc.exportMapDfs(fakeNode)
	if err != nil {
		return nil, fmt.Errorf("do parse map in dfs error[%v]", err)
	}

	return changeMp.(map[string]interface{}), nil
}

// get value of given path
func (mc *MongoConfig) getValue(section, key string, path ...string) ([]byte, error) {
	glog.V(3).Infof("execute getValue section[%s] key[%s] path[%v]", section, key, path)

	// recover when meets panic especially in bson parse.
	defer func() {
		if p := recover(); p != nil {
			err := fmt.Errorf("getValue internal error[%v]", p)
			glog.Errorf(err.Error())
		}
	}()

	var err error
	collectionHandler := mc.session.DB(mc.db).C(section)

	resBson := bson.Raw{}
	if err = collectionHandler.Find(bson.M{RecordCollectionUK: key}).One(&resBson); err != nil {
		return []byte{}, err
	}

	path = append([]string{key}, path...)
	var res []byte
	if res, err = mc.Parser.Get(resBson.Data, path...); err != nil {
		return []byte{}, err
	}
	return res, nil
}

// create unique index
func (mc *MongoConfig) ensureIndex(collectionHandler *mgo.Collection) error {
	glog.V(3).Infof("execute ensureIndex collection[%s]", collectionHandler.FullName)

	index := mgo.Index{
		Key:        []string{RecordCollectionUK},
		Unique:     true,
		DropDups:   true,
		Background: false,
	}

	return collectionHandler.EnsureIndex(index)
}

// convert path array to bson.M{}
func (mc *MongoConfig) convertPath2BsonM(path []string) interface{} {
	mp := bson.M{}
	now := mp
	for _, s := range path {
		now[s] = bson.M{}
		now = now[s].(bson.M)
	}
	return mp
}

// convert path array to bson.M{} and then set the value
func (mc *MongoConfig) updatePath2BsonM(path []string, value interface{}) interface{} {
	mp := bson.M{}
	now := mp
	for i, s := range path {
		if i == len(path)-1 {
			now[s] = value
		} else {
			now[s] = bson.M{}
			now = now[s].(bson.M)
		}
	}
	return mp
}

func (mc *MongoConfig) convertPath2Namespace(path []string) string {
	return strings.Join(path, NamespaceSplitter)
}

// lock section, input "section" is useless
func (mc *MongoConfig) lockSection(section string) error {
	glog.V(3).Infof("execute lockSection section[%s]", section)

	collectionHandler := mc.session.DB(mc.db).C(section)
	if err := mc.ensureIndex(collectionHandler); err != nil {
		return fmt.Errorf("create index of lock collection[%s] error[%v]", RecordCollectionLock, err)
	}
	key := RecordCollectionLockUniqueKey

	_, err := mc.getValue(section, key)
	if err != nil {
		// fmt.Printf("section[%s] key[%s] get value error[%v]\n", section, key, err)
		// insert first
		err := collectionHandler.Insert(bson.M{RecordCollectionUK: key, key: bson.M{}})
		if err != nil {
			return fmt.Errorf("section[%s] create unique-key failed[%v]", section, err)
		}
	}

	return mc.findAndLock(collectionHandler, key)
}

// lock key
func (mc *MongoConfig) lockKey(section, key string) error {
	glog.V(3).Infof("execute lockKey section[%s] key[%s]", section, key)

	collectionHandler := mc.session.DB(mc.db).C(section)

	// judge if key/path exists, return error if not
	_, err := mc.getValue(section, key)
	if err != nil {
		return fmt.Errorf("section[%s] with key[%s] not exist[%v]", section, key, err)
	}

	// find in the lock-collection
	selectorCollectionLock := bson.M{
		RecordCollectionUK: RecordCollectionLockUniqueKey,
		OpOr: []bson.M{
			{ // not exist
				RecordKeyLock: bson.M{
					OpExist: false,
				},
			},
			{RecordKeyLock: false}, // exist but false
			{ // exist and true but time expired
				OpAnd: []bson.M{
					{
						RecordKeyLock:     true,
						RecordKeyLockTime: bson.M{OpLe: time.Now().Add(-RecordLockTimeout * time.Second)},
					},
				},
			},
		},
	}

	collectionHandlerLock := mc.session.DB(mc.db).C(RecordCollectionLock)
	resBson := &bson.Raw{}
	if err := collectionHandlerLock.Find(selectorCollectionLock).One(resBson); err != nil {
		return fmt.Errorf("section[%s] find lock-collection meets error[%v]", section, err)
	}

	if len(resBson.Data) == 0 {
		return fmt.Errorf("section[%s] has already been locked", section)
	}

	return mc.findAndLock(collectionHandler, key)
}

// update the lock mark
func (mc *MongoConfig) findAndLock(collectionHandler *mgo.Collection, key string) error {
	glog.V(3).Infof("execute findAndLock collection[%s] key[%s]", collectionHandler.FullName, key)

	// find & lock
	selector := bson.M{
		RecordCollectionUK: key,
		OpOr: []bson.M{
			{ // not exist
				RecordKeyLock: bson.M{
					OpExist: false,
				},
			},
			{RecordKeyLock: false}, // exist but false
			{ // exist and true but time expired
				OpAnd: []bson.M{
					{
						RecordKeyLock:     true,
						RecordKeyLockTime: bson.M{OpLe: time.Now().Add(-RecordLockTimeout * time.Second)},
					},
				},
			},
		},
	}
	update := bson.M{
		OpSet: bson.M{
			RecordKeyLock:     true,
			RecordKeyLockTime: time.Now(),
		},
	}

	return collectionHandler.Update(selector, update)

}

/* use depth first search method to change map[string]node to map[string]interface{} and handle the array.
 * In the input map, array is store as document: {[0]: first, [1]: second, [2]: third}, and the map after
 * convert is [first, second, third]
 */
func (mc *MongoConfig) exportMapDfs(input *node) (interface{}, error) {
	// glog.V(3).Infof("execute exportMapDfs input[%v]", input)

	if input == nil {
		return nil, nil
	}

	if input.tp != whatson.DOCUMENT && input.tp != whatson.ARRAY {
		return input.value, nil
	}

	// assert error
	mp, ok := input.value.(map[string]*node)
	if !ok {
		return input.value, nil
	}

	if input.tp == whatson.DOCUMENT {
		curMp := make(map[string]interface{})
		for key, val := range mp {
			ret, err := mc.exportMapDfs(val)
			if err != nil {
				return nil, err
			}
			curMp[key] = ret
		}
		return curMp, nil
	} else { // must be whatson.ARRAY
		curArr := make([]interface{}, len(mp))
		for key, val := range mp {
			// convert key from string to int
			var idx int
			for i := 1; i < len(key)-1; i++ {
				idx = idx*10 + int(key[i]-'0')
			}

			ret, err := mc.exportMapDfs(val)
			if err != nil {
				return nil, err
			}
			curArr[idx] = ret
		}
		return curArr, nil
	}
}
