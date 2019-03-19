/*
// =====================================================================================
//
//       Filename:  MongoClient_test.go
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
import "runtime"
import "testing"

import "github.com/vinllen/mgo"
import "github.com/vinllen/mgo/bson"

func TestMongoClient(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var err error
	var ok bool
	var client ClientInterface
	var session interface{}
	var mongoSession *mgo.Session
	client, err = NewMongoClient().
		ConnectString("localhost:47017").
		Username("admin").
		Password("admin").
		UseDB("test").
		SetOpt(map[string]interface{}{"SafeMode": "majority", "ConsistencyMode": "Monotonic"}).
		EstablishConnect()
	check(client != nil, "test")
	check(err == nil, "test")

	session = client.GetSession()
	check(session != nil, "test session")
	mongoSession, ok = session.(*mgo.Session)
	check(ok, "test session check")
	_, err = mongoSession.DB("admin").SimpleRun(bson.M{"ping": 1})
	check(err == nil, "test run")

	check(true, "test")
}
