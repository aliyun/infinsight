/*
// =====================================================================================
//
//       Filename:  mysqlHelper.go
//
//    Description:
//
//        Version:  1.0
//        Created:  09/27/2018 07:25:30 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package handler

import (
	"database/sql"

	"github.com/golang/glog"
)

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  mysqlFindList
 *  Description:  根据query返回一个List，即假设查询结果是数组类型
 * =====================================================================================
 */
func mysqlFindList(session *sql.DB, query string) []string {
	var result []string = make([]string, 0)
	var rows *sql.Rows
	var err error

	if rows, err = session.Query(query); err != nil {
		glog.Errorf("query[%s] error: %s", query, err.Error())
		return nil
	}

	for rows.Next() {
		var value string
		if err = rows.Scan(&value); err != nil {
			glog.Errorf("scan in query[%s] error: %s", query, err.Error())
		}
		result = append(result, value)
	}

	return result
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  mysqlFindMap
 *  Description:  根据query返回一个Map，即假设查询结果是k-v类型
 * =====================================================================================
 */
func mysqlFindMap(session *sql.DB, query string) map[string]string {
	var result map[string]string = make(map[string]string)
	var rows *sql.Rows
	var err error

	if rows, err = session.Query(query); err != nil {
		glog.Errorf("query[%s] error: %s", query, err.Error())
		return nil
	}

	for rows.Next() {
		var key string
		var value string
		if err = rows.Scan(&key, &value); err != nil {
			glog.Errorf("scan in query[%s] error: %s", query, err.Error())
		}
		result[key] = value
	}

	return result
}
