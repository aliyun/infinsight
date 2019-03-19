/*
// =====================================================================================
//
//       Filename:  ringcache.go
//
//    Description:  基于时间的环形队列，只支持int的存储（thread-unsafe）
//                  当前ringcache采用array的存储形式
//
//        Version:  1.0
//        Created:  07/02/2018 05:54:42 PM
//       Compiler:  g++
//
// =====================================================================================
*/

package cache

import (
	"errors"
	"fmt"
	"inspector/util"

	"github.com/golang/glog"
)

type RingCache struct {
	name    string      // name of instance
	reserve int         // how many second reserved
	items   []*ringData // ring data
}

type ringData struct {
	timestamp uint32 // first timestamp of all item's timeflow
	// highest bit for index for data[index], the others bits for idx for data[index][idx]
	index int // first index of ring buffer(data)
	// util.NullData for null
	data []int64 // double ring buffer for saving data
}

var allNullData []int64

/*
// ===  FUNCTION  ======================================================================
//         Name:  Close
//  Description:  释放资源
// =====================================================================================
*/
func (rc *RingCache) Close() {
	rc.items = nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Init
//  Description:  reserve表示以多长时间（单位：秒）为一个ring，进行双buffer切换
// =====================================================================================
*/
func (rc *RingCache) Init(name string, reserve int) {
	glog.Infof("RingCache.init(): name[%s]|reserve[%d]", name, reserve)
	rc.name = name
	rc.reserve = reserve
	rc.items = make([]*ringData, 0)
	allNullData = make([]int64, reserve)
	for i := 0; i < reserve; i++ {
		allNullData[i] = util.NullData
	}
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  PuahBack
//  Description:
// =====================================================================================
*/
func (rc *RingCache) PushBack(offset int, timestamp uint32, data int64) error {
	var it *ringData

	// 如果数组不够，则新增数据
	if len(rc.items) <= offset {
		for i := len(rc.items) - 1; i < offset; i++ {
			rc.items = append(rc.items, nil)
		}
	}

	// 获取指定的数组项，如果为空则新建
	if rc.items[offset] == nil {
		it = new(ringData)
		it.timestamp = 0
		it.index = 0
		it.data = make([]int64, rc.reserve)
		for i := 0; i < len(it.data); i++ {
			it.data[i] = util.NullData
		}
		rc.items[offset] = it

		it.data[0] = data
		it.timestamp = timestamp
		it.index = 1
		return nil
	} else {
		it = rc.items[offset]
	}

	// 添加数据
	if timestamp <= it.timestamp-uint32(rc.reserve) {
		// time too early
		var errStr = fmt.Sprintf("timestamp[%d] is too early than[%d]", timestamp, it.timestamp-uint32(rc.reserve))
		glog.Error(errStr)
		return errors.New(errStr)
	} else if timestamp <= it.timestamp {
		// write previous data
		var timeDiff = int(it.timestamp - timestamp)
		var index = it.index
		if index <= timeDiff {
			index += rc.reserve
		}
		index -= timeDiff + 1
		it.data[index] = data
	} else {
		// write new data
		var timeDiff = int(timestamp - it.timestamp)
		var index = it.index
		for i := 0; i < timeDiff-1; i++ {
			it.data[index] = util.NullData
			index = (index + 1) % rc.reserve
		}
		it.data[index] = data
		it.index = (index + 1) % rc.reserve
		it.timestamp = timestamp
	}

	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Query
//  Description:  获取最新的数据，最新数据包含最少半分钟，最多一分半的数据。
//                传入终止时间（闭区间），返回起始时间（闭区间）
//                output参数为后加参数，为了解决collector在压缩时候大量内存分配
//                output数组要求必须是rc.reserve，否则就传nil，不懂就不要玩高科技
// =====================================================================================
*/
func (rc *RingCache) Query(offset int, timestamp uint32, output []int64) (uint32, []int64) {
	var it *ringData

	// params check
	if rc.items == nil || len(rc.items) <= offset || rc.items[offset] == nil {
		return 0, nil
	}
	it = rc.items[offset]

	var queryBegin = timestamp - uint32(rc.reserve) + 1
	var queryEnd = timestamp
	var cacheBegin = it.timestamp - uint32(rc.reserve) + 1
	var cacheEnd = it.timestamp

	// quick return when time is too early
	if queryEnd < cacheBegin {
		return queryBegin, allNullData
	}

	// do query
	var timeIndex = queryBegin
	var dataIndex = it.index
	if queryBegin > cacheBegin {
		dataIndex = (dataIndex + int(queryBegin-cacheBegin)) % rc.reserve
	}
	if output == nil {
		output = make([]int64, rc.reserve)
	} else {
		if len(output) != rc.reserve {
			panic(fmt.Sprintf("len(output)[%d] must be rc.reserve[%d]", len(output), rc.reserve))
		}
	}
	for i, _ := range output {
		if timeIndex < cacheBegin {
			output[i] = util.NullData
		} else if timeIndex <= cacheEnd {
			output[i] = it.data[dataIndex]
			dataIndex = (dataIndex + 1) % rc.reserve
		} else {
			output[i] = util.NullData
		}
		timeIndex++
	}

	return queryBegin, output
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  GetMaxOffset
 *  Description:  clean expire item by timestamp
 * =====================================================================================
 */
func (rc *RingCache) GetMaxOffset() int {
	return len(rc.items)
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  CleanExpire
 *  Description:  clean expire item by timestamp
 * =====================================================================================
 */
func (rc *RingCache) CleanExpire(timestamp uint32) {
	for i, it := range rc.items {
		if it != nil && it.timestamp+2*uint32(rc.reserve) < timestamp {
			rc.items[i] = nil
		}
	}
}
