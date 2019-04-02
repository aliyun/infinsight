/*
// =====================================================================================
//
//       Filename:  handler.go
//
//    Description:  为http请求每次调用提供公共存储空间
//
//        Version:  1.0
//        Created:  10/31/2018 02:11:56 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package handler

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
)

type ApiHandler struct {
	timeStart           time.Time
	timeTickerCount     int
	perfTimeConsumeList []perfTimeConsume
	allTimeConsume      time.Duration
}

/*
// ===  STRUCT  ========================================================================
//         Name:  StandardInfoModel
//  Description:
// =====================================================================================
*/
type StandardInfoModel struct {
	instanceId  int
	diskMaxSize int
	memMaxSize  int // 用户购买的内存大小
	memMaxLimit int // 实际分配的内存大小，一般比购买的大
	cpuMaxCore  int
	iopsMax     int
	connMax     int
}

var StandardInfoMapLocker *sync.RWMutex
var StandardInfoMap map[string]StandardInfoModel

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  mysqlFindStandardInfo
 *  Description:
 * =====================================================================================
 */
func mysqlFindStandardInfo(session *sql.DB, query string) {
	var rows *sql.Rows
	var err error

	var info = StandardInfoModel{}

	if rows, err = session.Query(query); err != nil {
		glog.Errorf("query[%s] error: %s", query, err.Error())
		return
	}

	for rows.Next() {
		if err = rows.Scan(
			&info.instanceId,
			&info.diskMaxSize,
			&info.iopsMax,
			&info.connMax,
			&info.cpuMaxCore,
			&info.memMaxSize,
			&info.memMaxLimit,
		); err != nil {
			glog.Errorf("scan in standard info[%s] error: %s", query, err.Error())
		} else {
			info.memMaxSize = info.memMaxLimit
		}

		var v StandardInfoModel
		var ok bool

		var idStr = fmt.Sprintf("%d", info.instanceId)
		// 这里不加读写锁是通过程序逻辑保证此处是唯一写者
		v, ok = StandardInfoMap[idStr]
		if !ok || v != info {
			StandardInfoMapLocker.Lock()
			StandardInfoMap[idStr] = info
			StandardInfoMapLocker.Unlock()
		}
	}
}

/*
// =====================================================================================
// perf time consume data model
// =====================================================================================
*/
type perfTimeConsume struct {
	name     string
	step     int
	duration time.Duration
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  timeReset
 *  Description:
 * =====================================================================================
 */
func (h *ApiHandler) timeReset() {
	h.timeTickerCount = 0
	h.perfTimeConsumeList = make([]perfTimeConsume, 0, 16)
	h.timeStart = time.Now()
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  timeTick
 *  Description:
 * =====================================================================================
 */
func (h *ApiHandler) timeTick(name string) {
	h.perfTimeConsumeList = append(h.perfTimeConsumeList, perfTimeConsume{
		name:     name,
		step:     h.timeTickerCount,
		duration: time.Since(h.timeStart),
	})
	h.timeTickerCount++
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  getTimeConsumeResult
 *  Description:
 * =====================================================================================
 */
func (h *ApiHandler) getTimeConsumeResult() (time.Duration, []perfTimeConsume) {
	h.allTimeConsume = time.Since(h.timeStart)
	var preDuration time.Duration = 0
	for i, it := range h.perfTimeConsumeList {
		h.perfTimeConsumeList[i].duration = it.duration - preDuration
		preDuration = it.duration
	}
	return h.allTimeConsume, h.perfTimeConsumeList
}
