/*
// =====================================================================================
//
//       Filename:  MysqlClient_test.go
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

import "database/sql"
import _ "github.com/go-sql-driver/mysql"

func TestMysqlClient(t *testing.T) {
	check := func(statement bool, msg string) {
		var line int
		_, _, line, _ = runtime.Caller(1)
		if statement == false {
			t.Error(fmt.Sprintf("line[%d] msg: %s\n", line, msg))
		}
	}
	var ok bool
	var err error
	var client ClientInterface
	var session interface{}
	var mysqlSession *sql.DB
	var rows *sql.Rows
	client, err = NewMysqlClient().
		ConnectString("localhost:3306").
		Username("root").
		Password("root").
		UseDB("mysql").
		SetOpt(map[string]interface{}{"MaxOpenConns": 1}).
		EstablishConnect()
	check(client != nil, "test")
	check(err == nil, "test")

	session = client.GetSession()
	mysqlSession, ok = session.(*sql.DB)
	check(ok == true, "test")
	rows, err = mysqlSession.Query("show status")
	check(err == nil, "test")
	for rows.Next() {
		var key string
		var value string
		err := rows.Scan(&key, &value)
		check(err == nil, "test")
		fmt.Printf("%s:%s\n", key, value)
	}

	check(true, "test")
}
