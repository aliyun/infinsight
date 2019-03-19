package sqlParse

import (
	"fmt"
	"database/sql"

	"inspector/util"
	"inspector/collector_server/model"

	"github.com/golang/glog"
)

// convert sql.rows to map list and return md5sum
func Parse(rows *sql.Rows, service string) ([]map[string]interface{}, []byte, error) {
	tp := util.GetDbType(service)
	switch tp {
	case util.Mongo:
		return parseMongo(rows)
	case util.RedisProxy:
		return parseRedisProxy(rows)
	default:
		return nil, nil, fmt.Errorf("service type[%v] not supported", tp)
	}
}

func parseMongo(rows *sql.Rows) ([]map[string]interface{}, []byte, error) {
	result := make([]map[string]interface{}, 0, 128) // size unknown, set 128 by default
	var (
		hid           int32
		pid           uint32
		dbType        string
		dbVersion     string
		ip            string
		port          int
		username      string
		password      string
		characterType string
	)

	for rows.Next() {
		if err := rows.Scan(&hid, &pid, &dbType, &dbVersion, &characterType, &ip, &port, &username, &password); err != nil {
			glog.Errorf("scan hid[%d] pid[%d] ip[%s] port[%d] failed[%v]", hid, pid, ip, port, err)
			continue
		}

		// confirmed, do not filter mongos
		//if characterType == "mongos" {
		//	continue
		//}

		result = append(result, make(map[string]interface{}, model.InstanceFieldNumber))
		mp := result[len(result) - 1]
		mp[model.HostName] = util.ConvertDot2Underline(fmt.Sprintf("%s:%d", ip, port))
		mp[model.PidName] = pid
		mp[model.HidName] = hid
		mp[model.UsernameName] = username
		mp[model.PasswordName] = password
		mp[model.DbTypeName] = util.ConvertDot2Underline(fmt.Sprintf("%s%s", dbType, dbVersion))
		mp[model.CharacterTypeName] = characterType
	}

	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	// calculate md5
	md5 := util.CalInstanceListMd5(result, []string{model.HostName})
	return result, md5, nil
}

func parseRedisProxy(rows *sql.Rows) ([]map[string]interface{}, []byte, error) {
	result := make([]map[string]interface{}, 0, 128) // size unknown, set 128 by default
	var (
		hid  int32
		ip   string
		port int
	)

	for rows.Next() {
		if err := rows.Scan(&hid, &ip, &port); err != nil {
			glog.Errorf("scan hid[%d] ip[%s] port[%d] failed[%v]", hid, ip, port, err)
			continue
		}

		result = append(result, make(map[string]interface{}, model.InstanceFieldNumber))
		mp := result[len(result) - 1]
		mp[model.HostName] = util.ConvertDot2Underline(fmt.Sprintf("%s:%d", ip, port))
		mp[model.HidName] = hid
		mp[model.DbTypeName] = "redis_proxy"
	}

	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	// calculate md5
	md5 := util.CalInstanceListMd5(result, []string{model.HostName})
	return result, md5, nil
}