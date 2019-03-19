/*
/ =====================================================================================
//
//       Filename:  RedisClient.go
//
//    Description:  redis client
//
//        Version:  1.0
//        Created:  07/05/2018 08:32:04 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package client

import "strconv"

import "github.com/golang/glog"
import "github.com/go-redis/redis"

type RedisClient struct {
	connectString string
	username      string
	password      string
	db            int
	session       *redis.Client
	poolSize      int
	authCommand   string
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  NewRedisClient
//  Description:
// =====================================================================================
*/
func NewRedisClient() ClientInterface {
	return new(RedisClient)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  ConnectString
//  Description:
// =====================================================================================
*/
func (client *RedisClient) ConnectString(connectString string) ClientInterface {
	client.connectString = connectString
	return client
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Username
//  Description:
// =====================================================================================
*/
func (client *RedisClient) Username(username string) ClientInterface {
	client.username = username
	return client
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Password
//  Description:
// =====================================================================================
*/
func (client *RedisClient) Password(password string) ClientInterface {
	client.password = password
	return client
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  UseDB
//  Description:
// =====================================================================================
*/
func (client *RedisClient) UseDB(db string) ClientInterface {
	var num int
	var err error
	if num, err = strconv.Atoi(db); err == nil {
		glog.Warningf("db not a invalid integer, DB[%s] set to 0", db)
		client.db = 0
	} else {
		client.db = num
	}
	return client
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  SetOpt
//  Description:
// =====================================================================================
*/
func (client *RedisClient) SetOpt(opts map[string]interface{}) ClientInterface {
	for k, v := range opts {
		switch k {
		case "PoolSize":
			if size, ok := v.(int); ok {
				client.poolSize = size
			} else {
				client.poolSize = 1
				glog.Warningf("redis opt[PoolSize] not a invalid type, PoolSize set to 1")
			}
		case "AuthCommand":
			var cmd string
			var ok bool
			if cmd, ok = v.(string); ok {
				glog.Warningf("redis opt[AuthCommand] not a string set to \"auth\"", v)
				client.authCommand = "auth"
				break
			}
			switch cmd {
			case "auth":
				fallthrough
			case "adminauth":
				client.authCommand = cmd
			default:
				client.authCommand = "auth"
				glog.Warningf("redis opts[AuthCommand] is invalid: AuthCommand[%s] set to \"auth\"", v)
			}
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
func (client *RedisClient) EstablishConnect() (ClientInterface, error) {
	var err error

	if client.session != nil {
		return client, nil
	}

	glog.Infof("Connect to Redis: %v", client)

	// connect
	client.session = redis.NewClient(&redis.Options{
		Addr:        client.connectString,
		Password:    client.password,
		DB:          client.db,
		PoolSize:    client.poolSize,
		AuthCommand: client.authCommand,
	})
	// ping
	if _, err = client.session.Ping().Result(); err != nil {
		glog.Errorf("connect to redis Failed, connectString[%s], password[%d], db[%d], err[%s]",
			client.connectString, client.password, client.db, err)
		return nil, err
	}

	return client, nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Close
//  Description:
// =====================================================================================
*/
func (client *RedisClient) Close() {
	glog.Infof("Close RedisClient session %v", client)
	client.session.Close()
	client.session = nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  GetSession
//  Description:
// =====================================================================================
*/
func (client *RedisClient) GetSession() interface{} {
	return client.session
}
