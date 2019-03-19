/*
// =====================================================================================
//
//       Filename:  LocalConfig.go
//
//    Description:  用读写文件的形式实现Config接口
//
//        Version:  1.0
//        Created:  06/10/2018 03:15:03 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"syscall"
	//" os/exec"
	"strconv"
	"strings"

	. "github.com/Unknwon/goconfig"
	"github.com/golang/glog"

	"inspector/util/scheduler"
	. "inspector/util/unsafe"
	"inspector/util/whatson"
)

const checkDuration = 100 // time.Millisecond

type LocalConfig struct {
	Parser     whatson.Parser
	filename   string
	locker     *os.File
	watcherMap map[string]*scheduler.TCB
	cf         *ConfigFile
	skd        *scheduler.Scheduler
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Init
//  Description:  使用文件名作为连接字符串
// =====================================================================================
*/
func (cfg *LocalConfig) Init() ConfigInterface {
	glog.Infof("start config server[%s] scheduler", cfg.filename)
	cfg.skd = new(scheduler.Scheduler)
	cfg.skd.Init("local config server Scheduler", checkDuration)
	go cfg.skd.Run()

	cfg.watcherMap = make(map[string]*scheduler.TCB)
	return cfg
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  ConnectString
//  Description:  使用文件名作为连接字符串
// =====================================================================================
*/
func (cfg *LocalConfig) ConnectString(filename string) ConfigInterface {
	cfg.filename = filename
	return cfg
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Auth
//  Description:  本地文件配置不需要权限管理
// =====================================================================================
*/
func (cfg *LocalConfig) Username(authString string) ConfigInterface {
	return cfg
}

func (cfg *LocalConfig) Password(authString string) ConfigInterface {
	return cfg
}

func (cfg *LocalConfig) DB(db string) ConfigInterface {
	return cfg
}

func (cfg *LocalConfig) SetWatcherStep(watcherInterval int) ConfigInterface {
	glog.Info("open all watcher in LocalConfig")
	return cfg
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  EstablishConnect
//  Description:  本地文件配置建立连接即加载文件
// =====================================================================================
*/
func (cfg *LocalConfig) EstablishConnect() error {
	glog.Infof("load config file[%s]: %s", cfg.filename)
	var err error
	cfg.cf, err = LoadConfigFile(cfg.filename)
	if err != nil {
		glog.Errorf("load config file[%s] error: %s", cfg.filename, err.Error())
	}

	return err
}

func (cfg *LocalConfig) Close() {
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  GetSectionList
//  Description:
// =====================================================================================
*/
func (cfg *LocalConfig) GetSectionList() ([]string, error) {
	list := cfg.cf.GetSectionList()
	return list, nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  GetKeyList
//  Description:
// =====================================================================================
*/
func (cfg *LocalConfig) GetKeyList(section string) ([]string, error) {
	list := cfg.cf.GetKeyList(section)
	return list, nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  GetSection
//  Description:
// =====================================================================================
*/
func (cfg *LocalConfig) GetSection(section string) (map[string]string, error) {
	return cfg.cf.GetSection(section)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  GetString
//  Description:
// =====================================================================================
*/
func (cfg *LocalConfig) GetString(section, key string, path ...string) (string, error) {
	var value string
	var result []byte
	var err error

	value, err = cfg.cf.GetValue(section, key)
	if err != nil {
		return "", err
	}

	result, err = cfg.Parser.Get(String2Bytes(value), path...)
	return string(result), err
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  GetBool
//  Description:
// =====================================================================================
*/
func (cfg *LocalConfig) GetBool(section, key string, path ...string) (bool, error) {
	value, err := cfg.GetString(section, key, path...)
	if err != nil {
		return false, err
	}
	return strconv.ParseBool(value)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  GetInt
//  Description:
// =====================================================================================
*/
func (cfg *LocalConfig) GetInt(section, key string, path ...string) (int, error) {
	value, err := cfg.GetString(section, key, path...)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(value)
}

func (cfg *LocalConfig) GetUint(section, key string, path ...string) (uint, error) {
	value, err := cfg.GetString(section, key, path...)
	if err != nil {
		return 0, err
	}
	ret, err := strconv.Atoi(value)
	return uint(ret), err
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  GetInt64
//  Description:
// =====================================================================================
*/
func (cfg *LocalConfig) GetInt64(section, key string, path ...string) (int64, error) {
	value, err := cfg.GetString(section, key, path...)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(value, 10, 64)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  GetFloat32
//  Description:
// =====================================================================================
*/
func (cfg *LocalConfig) GetFloat32(section, key string, path ...string) (float32, error) {
	f64, err := cfg.GetFloat64(section, key, path...)
	return float32(f64), err
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  GetFloat64
//  Description:
// =====================================================================================
*/
func (cfg *LocalConfig) GetFloat64(section, key string, path ...string) (float64, error) {
	value, err := cfg.GetString(section, key, path...)
	if err != nil {
		return 0.0, err
	}
	return strconv.ParseFloat(value, 64)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  GetArray
//  Description:
// =====================================================================================
*/
func (cfg *LocalConfig) GetArray(section, key string, path ...string) ([]string, error) {
	value, err := cfg.GetString(section, key, path...)
	if err != nil {
		return nil, err
	}
	if value[0:1] != "[" || value[len(value)-1:len(value)] != "]" {
		return nil, errors.New("invalid array")
	}
	result := strings.Split(value[1:len(value)-1], ",")
	for i, _ := range result {
		result[i] = strings.Trim(result[i], " \t\n\"")
	}
	return result, nil
}

func (cfg *LocalConfig) GetMap(section, key string, path ...string) (map[string]interface{}, error) {
	return nil, fmt.Errorf("not support")
}

func (cfg *LocalConfig) GetBytes(section, key string, path ...string) ([]byte, error) {
	return nil, fmt.Errorf("not support")
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  SetItem
//  Description:  SetItem允许对部分json进行更新
// =====================================================================================
*/
func (cfg *LocalConfig) SetItem(section, key string, value interface{}, path ...string) error {
	var ret bool
	var err error
	var result string
	if len(path) > 0 {
		// 如果value是[]byte类型则将其转化为json内存结构
		var valueObj interface{}
		if vbytes, ok := value.([]byte); ok {
			if err = json.Unmarshal(vbytes, &valueObj); err != nil {
				return err
			}
		}

		// 读取对应key
		var orgJson string
		orgJson, err = cfg.GetString(section, key)
		if err != nil {
			return err
		}
		var jsonObj interface{}

		// 尝试将json字符串转化为内存结构，如果转化失败就抹掉之前的数据
		if err = json.Unmarshal([]byte(orgJson), &jsonObj); err != nil {
			if path[0][0:1] == "[" {
				jsonObj = make([]interface{}, 0)
			} else {
				jsonObj = make(map[string]interface{})
			}
		}

		// 遍历内存结构，并修改数据
		// 吐槽：之所以遍历的这么复杂，主要是因为golang的map元素无法取地址
		var tempObj interface{} = jsonObj
		for idx, it := range path {
			if it[0:1] == "[" {
				// is array
				var l *[]interface{}
				var ok bool
				var offset int
				if l, ok = tempObj.(*[]interface{}); !ok {
					return errors.New(fmt.Sprintf("invalid type of key[%s]", it))
				}
				// parse target offset
				if offset, err = strconv.Atoi(it[1 : len(it)-1]); err != nil {
					return err
				}
				// if len is not enough, append new
				if offset >= len(*l) {
					n := offset - len(*l) + 1
					for i := 0; i < n; i++ {
						*l = append(*l, nil)
					}
				}
				if idx == len(path)-1 {
					// last element
					if valueObj == nil {
						(*l)[offset] = value
					} else {
						(*l)[offset] = valueObj
					}
				} else {
					// not last element, check next type
					if (*l)[offset] == nil {
						// if sub node not exist, create it
						if idx+1 < len(path) && path[idx+1][0:1] == "[" {
							// next is array
							tmp := make([]interface{}, 0)
							(*l)[offset] = &tmp
						} else {
							// next is map
							(*l)[offset] = make(map[string]interface{})
						}
					} else {
						// if tempObj.type is []interface, rewrite it's pointer to map
						if tmp, ok := (*l)[offset].([]interface{}); ok {
							(*l)[offset] = &tmp
						}
					}
					tempObj = (*l)[offset]
				}
			} else {
				// is map
				if m, ok := tempObj.(map[string]interface{}); ok {
					if idx == len(path)-1 {
						// last path
						if valueObj == nil {
							m[it] = value
						} else {
							m[it] = valueObj
						}
					} else {
						// not last element, check next type
						if _, ok = m[it]; !ok {
							if idx+1 < len(path) && path[idx+1][0:1] == "[" {
								// next is array
								tmp := make([]interface{}, 0)
								m[it] = &tmp
							} else {
								// next is map
								m[it] = make(map[string]interface{})
							}
						} else {
							// if tempObj.type is []interface, rewrite it's pointer to map
							if tmp, ok := m[it].([]interface{}); ok {
								m[it] = &tmp
							}
						}
						tempObj = m[it]
					}
				} else {
					return errors.New(fmt.Sprintf("invalid type of key[%s]", it))
				}
			}
		}

		var resultBytes []byte
		resultBytes, err = json.Marshal(jsonObj)
		if err != nil {
			return err
		}
		result = string(resultBytes)
	} else {
		var ok bool
		if result, ok = value.(string); !ok {
			return errors.New("value type error: expect type[string]")
		}
	}
	// 先删除key值，因为goconfig库无法支持更新试写入
	cfg.cf.DeleteKey(section, key)
	ret = cfg.cf.SetValue(section, key, result)
	if ret == false {
		glog.Errorf("set section[%s] key[%s] value[%s] error", section, key, result)
		return errors.New("set item error")
	}
	return SaveConfigFile(cfg.cf, cfg.filename)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  DeleteItem
//  Description:  由于gloang不支持map取地址，导致DeleteItem与SetItem大量代码重复
// =====================================================================================
*/
func (cfg *LocalConfig) DeleteItem(section, key string, path ...string) error {
	var ret bool
	var err error
	if len(path) > 0 {
		// del partable json
		// 读取对应key
		var orgJson string
		orgJson, err = cfg.GetString(section, key)
		if err != nil {
			return err
		}

		// 解析Json
		var jsonObj interface{}
		if err = json.Unmarshal([]byte(orgJson), &jsonObj); err != nil {
			return err
		}

		// 遍历内存结构，并删除数据
		var tempObj interface{} = jsonObj
		for idx, it := range path {
			if it[0:1] == "[" {
				// is array
				var l *[]interface{}
				var ok bool
				var offset int
				if l, ok = tempObj.(*[]interface{}); !ok {
					return errors.New(fmt.Sprintf("invalid type of key[%s]", it))
				}
				// parse target offset
				if offset, err = strconv.Atoi(it[1 : len(it)-1]); err != nil {
					return err
				}
				// check if offset is outof len
				if offset >= len(*l) {
					return errors.New(fmt.Sprintf("offset[%d] is out of len[%d]", offset, len(*l)))
				}
				// last path, do delete
				if idx == len(path)-1 {
					*l = append((*l)[:offset], (*l)[offset+1:]...)
				} else {
					// not last element, check next type
					// if tempObj.type is []interface, rewrite it's pointer to map
					if tmp, ok := (*l)[offset].([]interface{}); ok {
						(*l)[offset] = &tmp
					}
					tempObj = (*l)[offset]
				}
			} else {
				// is map
				if m, ok := tempObj.(map[string]interface{}); ok {
					if idx == len(path)-1 {
						// last path, do delete
						if _, ok = m[it]; ok {
							delete(m, it)
						} else {
							return errors.New(fmt.Sprintf("key[%s] is not exist", it))
						}
					} else {
						// not last element, check next type
						// if tempObj.type is []interface, rewrite it's pointer to map
						if tmp, ok := m[it].([]interface{}); ok {
							m[it] = &tmp
						}
						tempObj = m[it]
					}
				} else {
					return errors.New(fmt.Sprintf("invalid type of key[%s]", it))
				}
			}
		}

		var resultBytes []byte
		var result string
		resultBytes, err = json.Marshal(jsonObj)
		if err != nil {
			return err
		}
		result = string(resultBytes)

		// 先删除key值，因为goconfig库无法支持更新试写入
		cfg.cf.DeleteKey(section, key)
		ret = cfg.cf.SetValue(section, key, result)
		if ret == false {
			glog.Errorf("set section[%s] key[%s] value[%s] error", section, key, result)
			return errors.New("set item error")
		}
	} else {
		// del entire key
		ret := cfg.cf.DeleteKey(section, key)
		if ret == false {
			glog.Errorf("del section[%s] key[%s] error", section, key)
			return errors.New("del item error")
		}
	}

	return SaveConfigFile(cfg.cf, cfg.filename)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  SetArray
//  Description:
// =====================================================================================
*/
func (cfg *LocalConfig) SetArray(section, key string, array []string) error {
	if array == nil {
		return errors.New("SetArray error: array is nil")
	}

	var buf strings.Builder
	buf.Grow(1024) // set cap
	buf.WriteRune('[')
	for i, it := range array {
		buf.WriteString(it)
		if i < len(array)-1 {
			buf.WriteRune(',')
		}
	}
	buf.WriteRune(']')

	// 先删除key值，因为goconfig库无法支持更新试写入
	cfg.cf.DeleteKey(section, key)
	ret := cfg.cf.SetValue(section, key, buf.String())
	if ret == false {
		glog.Errorf("set section[%s] key[%s] value[%s] error", section, key, buf.String())
		return errors.New("set item error")
	}
	return SaveConfigFile(cfg.cf, cfg.filename)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  DeleteSection
//  Description:
// =====================================================================================
*/
func (cfg *LocalConfig) DeleteSection(section string) error {
	var ret bool
	ret = cfg.cf.DeleteSection(section)
	if ret == false {
		glog.Errorf("del section[%s] error", section)
		return errors.New("del section error")
	}
	return SaveConfigFile(cfg.cf, cfg.filename)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Lock
//  Description:  由于golang没有可用于进程互锁的信号量，所以临时用pipe文件解决
// =====================================================================================
*/
func (cfg *LocalConfig) Lock(section, key string, path ...string) error {
	file, err := os.Open(cfg.filename)
	if nil != err {
		return err
	}
	if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return err
	}
	cfg.locker = file
	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Unlock
//  Description:
// =====================================================================================
*/
func (cfg *LocalConfig) Unlock(section, key string, path ...string) {
	defer cfg.locker.Close()
	syscall.Flock(int(cfg.locker.Fd()), syscall.LOCK_UN)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  RegisterGlobalWatcher
//  Description:
// =====================================================================================
*/
func (cfg *LocalConfig) RegisterGlobalWatcher(section, key string, watcher *Watcher, path ...string) error {
	var reload = cfg.EstablishConnect

	// merge key
	var buf strings.Builder
	buf.Grow(1024) // set cap
	buf.WriteString(section)
	buf.WriteRune('/')
	buf.WriteString(key)
	buf.WriteRune('/')
	for _, it := range path {
		buf.WriteString(it)
		buf.WriteRune('/')
	}
	var mergeKey = buf.String()

	// new step
	var step = new(stepStatConfig)
	step.filename = cfg.filename
	step.callback = func(event WatcheEvent) error {
		if err := reload(); err != nil {
			return err
		}
		if event == watcher.Event {
			return watcher.Handler(event)
		}
		return nil
	}

	// new TCB
	var tcb = new(scheduler.TCB)
	tcb.Init(mergeKey)
	tcb.AddWorkflowStep(step)
	cfg.watcherMap[mergeKey] = tcb
	cfg.skd.AddTCB([]*scheduler.TCB{tcb})
	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  RemoveGlobalWatcher
//  Description:
// =====================================================================================
*/
func (cfg *LocalConfig) RemoveGlobalWatcher(section, key string, path ...string) error {
	// merge key
	var buf strings.Builder
	buf.Grow(1024) // set cap
	buf.WriteString(section)
	buf.WriteRune('/')
	buf.WriteString(key)
	buf.WriteRune('/')
	for _, it := range path {
		buf.WriteString(it)
		buf.WriteRune('/')
	}
	var mergeKey = buf.String()

	// del tcb
	if tcb, ok := cfg.watcherMap[mergeKey]; ok {
		cfg.skd.DelTCB([]*scheduler.TCB{tcb})
		delete(cfg.watcherMap, mergeKey)
	}
	return nil
}

func (cfg *LocalConfig) DeleteAll() error {
	return fmt.Errorf("not support now")
}

func (cfg *LocalConfig) Export(filename string) error {
	return fmt.Errorf("not support now")
}

func (cfg *LocalConfig) Import(filename string) error {
	return fmt.Errorf("not support now")
}

// =====================================================================================
// private method
// -------------------------------------------------------------------------------------
