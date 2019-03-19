/*
// =====================================================================================
//
//       Filename:  RedisClient_test.go
//
//    Description:  redis client test
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

import "github.com/go-redis/redis"

func TestRedisClient(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var client ClientInterface
	var session interface{}
	var redisSession *redis.Client
	var err error
	var ok bool
	client, err = NewRedisClient().
		ConnectString("localhost:3002").
		// Password("admin").
		// UseDB("0").
		// SetOpt(map[string]interface{}{"PoolSize": 1, "AuthCommand": "auth"}).
		EstablishConnect()
	check(err == nil, "test")
	check(client != nil, "test")

	session = client.GetSession()
	redisSession, ok = session.(*redis.Client)
	check(ok, "test")

	_, err = redisSession.Info("all").Bytes()
	check(err == nil, "test")

	check(true, "test")
}
