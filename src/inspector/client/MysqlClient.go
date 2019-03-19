/*
// =====================================================================================
//
//       Filename:  MysqlClient.go
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

import "fmt"
import "database/sql"
//import _ "github.com/go-sql-driver/mysql"
import "github.com/golang/glog"

type MysqlClient struct {
	connectString string
	username      string
	password      string
	db            string
	session       *sql.DB
	maxOpenConn   int
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  NewMysqlClient
//  Description:
// =====================================================================================
*/
func NewMysqlClient() ClientInterface {
	return new(MysqlClient)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  ConnectString
//  Description:
// =====================================================================================
*/
func (client *MysqlClient) ConnectString(connectString string) ClientInterface {
	client.connectString = connectString
	return client
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Username
//  Description:
// =====================================================================================
*/
func (client *MysqlClient) Username(username string) ClientInterface {
	client.username = username
	return client
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Password
//  Description:
// =====================================================================================
*/
func (client *MysqlClient) Password(password string) ClientInterface {
	client.password = password
	return client
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  UseDB
//  Description:
// =====================================================================================
*/
func (client *MysqlClient) UseDB(db string) ClientInterface {
	client.db = db
	return client
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  SetOpt
//  Description:
// =====================================================================================
*/
func (client *MysqlClient) SetOpt(opts map[string]interface{}) ClientInterface {
	for k, v := range opts {
		switch k {
		case "MaxOpenConns":
			if num, ok := v.(int); ok {
				client.maxOpenConn = num
			} else {
				client.maxOpenConn = 1
				glog.Warningf("mysql opt[MaxOpenConns] not a invalid type, MaxOpenConns set to 1")
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
func (client *MysqlClient) EstablishConnect() (ClientInterface, error) {
	var err error

	if client.session != nil {
		return client, nil
	}

	glog.Infof("Connect to Mysql: %v", client)

	// connect
	if client.session, err = sql.Open("mysql", fmt.Sprintf("%s:%s@(%s)/%s",
		client.username, client.password, client.connectString, client.db)); err != nil {
		glog.Errorf("connect to Mysql Failed, connectString[%s], password[%d], db[%d], err[%s]",
			client.connectString, client.password, client.db, err)
		return nil, err
	}
	client.session.SetMaxOpenConns(client.maxOpenConn)

	return client, nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Close
//  Description:
// =====================================================================================
*/
func (client *MysqlClient) Close() {
	glog.Infof("Close MysqlClient session %v", client)
	client.session.Close()
	client.session = nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  GetSession
//  Description:
// =====================================================================================
*/
func (client *MysqlClient) GetSession() interface{} {
	return client.session
}
