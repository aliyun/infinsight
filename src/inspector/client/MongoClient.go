/*
// =====================================================================================
//
//       Filename:  MongoClient.go
//
//    Description:
//
//        Version:  1.0
//        Created:  07/05/2018 08:32:04 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package client

import (
	"time"

	"github.com/golang/glog"
	"github.com/vinllen/mgo"
)

type MongoClient struct {
	connectString      string
	username           string
	password           string
	db                 string
	session            *mgo.Session
	safeMode           *mgo.Safe
	useConsistencyMode bool
	consistencyMode    mgo.Mode
	poolLimit          int           // pool limit
	timeout            time.Duration // connection timeout
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  NewMongoClient
//  Description:
// =====================================================================================
*/
func NewMongoClient() ClientInterface {
	return new(MongoClient)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  ConnectString
//  Description:
// =====================================================================================
*/
func (client *MongoClient) ConnectString(connectString string) ClientInterface {
	client.connectString = connectString
	return client
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Username
//  Description:
// =====================================================================================
*/
func (client *MongoClient) Username(username string) ClientInterface {
	client.username = username
	return client
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Password
//  Description:
// =====================================================================================
*/
func (client *MongoClient) Password(password string) ClientInterface {
	client.password = password
	return client
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  UseDB
//  Description:
// =====================================================================================
*/
func (client *MongoClient) UseDB(db string) ClientInterface {
	client.db = db
	return client
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  SetOpt
//  Description:  目前对mongo的参数配置并不是很熟悉，对此测试并不完全
//                目前只用到majority和Monotonic配置
// =====================================================================================
*/
func (client *MongoClient) SetOpt(opts map[string]interface{}) ClientInterface {
	for k, v := range opts {
		switch k {
		case "SafeMode":
			switch v {
			case "unack":
				client.safeMode = &mgo.Safe{W: 0}
			case "ack":
				client.safeMode = &mgo.Safe{W: 1}
			case "journaled":
				client.safeMode = &mgo.Safe{J: true}
			case "majority":
				client.safeMode = &mgo.Safe{WMode: "majority"}
			default:
				glog.Warningf("opt[%s] of MongoClient: unknown value[%s]", k, v)
			}
		case "ConsistencyMode":
			switch v {
			case "Eventual":
				client.useConsistencyMode = true
				client.consistencyMode = mgo.Eventual
			case "Monotonic":
				client.useConsistencyMode = true
				client.consistencyMode = mgo.Monotonic
			case "Strong":
				client.useConsistencyMode = true
				client.consistencyMode = mgo.Strong
			default:
				client.useConsistencyMode = false
				glog.Warningf("unknown opts of MongoClient: key[%s], value[%s]", k, v)
			}
		case "PoolLimit":
			client.poolLimit = v.(int)
		case "Timeout":
			client.timeout = v.(time.Duration)
		default:
			glog.Warningf("unknown opts of MongoClient: key[%s]", k)
		}
	}
	return client
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  EstablishConnect
//  Description:
// =====================================================================================
*/
func (client *MongoClient) EstablishConnect() (ClientInterface, error) {
	var err error

	if client.session != nil {
		return client, nil
	}

	glog.Infof("Connect to MongoDB: %v", client)

	// connect
	if client.session, err = mgo.Dial(client.connectString); err != nil {
		glog.Errorf("fail to connect to %s", client.connectString)
		return nil, err
	}
	if client.safeMode != nil {
		client.session.SetSafe(client.safeMode)
	}
	if client.useConsistencyMode {
		client.session.SetMode(client.consistencyMode, true)
	}
	if client.poolLimit > 0 {
		client.session.SetPoolLimit(client.poolLimit)
	}
	if client.timeout > 0 {
		client.session.SetSocketTimeout(client.timeout)
	}

	// auth
	if len(client.username) > 0 {
		if err = client.session.DB("admin").Login(client.username, client.password); err != nil {
			glog.Errorf("fail to connect login to %s, username[%s] password[%s] is invalid",
				client.connectString, client.username, client.password)
		}
	}
	return client, err
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Close
//  Description:
// =====================================================================================
*/
func (client *MongoClient) Close() {
	glog.Infof("Close MongoClient session %v", client)
	client.session.Close()
	client.session = nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  GetSession
//  Description:
// =====================================================================================
*/
func (client *MongoClient) GetSession() interface{} {
	return client.session
}
