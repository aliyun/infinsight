/*
// =====================================================================================
//
//       Filename:  handler.go
//
//    Description:  为rpc请求每次调用提供公共存储空间
//
//        Version:  1.0
//        Created:  10/31/2018 02:11:56 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package handler

import "time"

// =====================================================================================
//       Struct:  RpcHandler
//  Description:  struct for grpc
// =====================================================================================
type RpcHandler struct {
	timeStart           time.Time
	timeTickerCount     int
	perfTimeConsumeList []perfTimeConsume
	allTimeConsume      time.Duration
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
func (h *RpcHandler) timeReset() {
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
func (h *RpcHandler) timeTick(name string) {
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
func (h *RpcHandler) getTimeConsumeResult() (time.Duration, []perfTimeConsume) {
	h.allTimeConsume = time.Since(h.timeStart)
	var preDuration time.Duration = 0
	for i, it := range h.perfTimeConsumeList {
		h.perfTimeConsumeList[i].duration = it.duration - preDuration
		preDuration = it.duration
	}
	return h.allTimeConsume, h.perfTimeConsumeList
}
