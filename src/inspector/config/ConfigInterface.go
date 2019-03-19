/*
// =====================================================================================
//
//       Filename:  ConfigInterface.go
//
//    Description:  将Config服务进行抽象，封装分布式配置管理服务
//                  抽象Config结构分为两个逻辑单元：section与item(key, value)，默认section为default
//                  key与value并不是简单字符串
//                  key的形式为JsonPath，例如：/mongoDB/instances/127.0.0.1:8080/tag
//                  value的形式根据key的不同，可能是一个Json子结构或Json基础数据类型(string, int, float, bool)
//                  逻辑上key-value的结构配合可以理解为从一个巨大的Json中获取子数据的过程
//
//        Version:  1.0
//        Created:  06/10/2018 03:02:24 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package config

import (
	"fmt"
	"inspector/util/whatson"
)

// =====================================================================================
//         Type:  WatchEvent
//  Description:  watcher的触发事件
// =====================================================================================
type WatcheEvent int

const (
	DISCONNECTED WatcheEvent = iota
	EXPIRED
	NODECREATED
	NODEDELETED
	NODECHANGED
	NODEALL
	UNKNOWN

	LocalConfigName = "local"
	MongoConfigName = "mongo"
)

// =====================================================================================
//       Struct:  Watcher
//  Description:
// =====================================================================================
type Watcher struct {
	Event   WatcheEvent
	Handler func(event WatcheEvent) error
}

type ConfigInterface interface {
	// ===  Method  ========================================================================
	// Description: 用于设置参数并创建连接，采用Fluent风格
	//              for example:
	//              var cfg ConfigInterface = new(ConfigInterfaceImpl)
	//              cfg.Init()
	//                 .ConnectString("127.0.0.1:8080")
	//                 .Auth("user=elwin, passwd=123456")
	//                 .EstablishConnect()
	// =====================================================================================
	Init() ConfigInterface
	ConnectString(string) ConfigInterface
	Username(string) ConfigInterface
	Password(string) ConfigInterface
	DB(string) ConfigInterface
	SetWatcherStep(int) ConfigInterface // unit: ms, enable if input parameter > 0
	EstablishConnect() error

	// ===  Method  ========================================================================
	// Description: 读接口
	// =====================================================================================
	GetSectionList() ([]string, error)
	GetKeyList(section string) ([]string, error)

	GetSection(section string) (map[string]string, error)

	GetString(section, key string, path ...string) (string, error)
	GetBool(section, key string, path ...string) (bool, error)
	GetInt(section, key string, path ...string) (int, error)
	GetUint(section, key string, path ...string) (uint, error)
	GetInt64(section, key string, path ...string) (int64, error)
	GetFloat32(section, key string, path ...string) (float32, error)
	GetFloat64(section, key string, path ...string) (float64, error)
	GetArray(section, key string, path ...string) ([]string, error)
	GetMap(section, key string, path ...string) (map[string]interface{}, error)
	GetBytes(section, key string, path ...string) ([]byte, error)

	// ===  Method  ========================================================================
	// Description: 写接口
	// =====================================================================================
	SetItem(section, key string, value interface{}, path ...string) error
	DeleteItem(section, key string, path ...string) error

	SetArray(section, key string, array []string) error

	DeleteSection(section string) error

	//delete all sections
	DeleteAll() error

	// ===  Method  ========================================================================
	// Description: 分布式锁
	// =====================================================================================
	Lock(section, key string, path ...string) error
	Unlock(section, key string, path ...string)

	// ===  Method  ========================================================================
	// Description: Watcher触发器
	// =====================================================================================
	RegisterGlobalWatcher(section, key string, watcher *Watcher, path ...string) error
	RemoveGlobalWatcher(section, key string, path ...string) error

	// ===  Method  ========================================================================
	// Description: 导出/导入接口, 符合ini和json格式
	// =====================================================================================
	Export(filename string) error
	Import(filename string) error

	// do close explicitly
	Close()
}

type ConfigFactory struct {
	Name string
}

func (factory *ConfigFactory) Create(address, username, password, db string, watcherInterval int) (ConfigInterface, error) {
	var handler ConfigInterface
	switch factory.Name {
	case LocalConfigName:
		handler = &LocalConfig{Parser: whatson.NewParser(whatson.Json)}
	case MongoConfigName:
		handler = &MongoConfig{Parser: whatson.NewParser(whatson.Bson)}
	default:
		return nil, fmt.Errorf("type[%s] not supported", factory.Name)
	}

	err := handler.Init().
		ConnectString(address).
		Username(username).
		Password(password).
		DB(db).
		SetWatcherStep(watcherInterval).
		EstablishConnect()
	// connect
	return handler, err
}
