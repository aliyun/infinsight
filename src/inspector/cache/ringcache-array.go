/*
// =====================================================================================
//
//       Filename:  ringcache.go
//
//    Description:  基于时间的环形队列，只支持int的存储（thread-unsafe）
//                  考虑到所有的字符串都可以转化为一个整数，当字符串编码人为可控且数值较小时，
//                  可以考虑使用slice代替map，从而提高效率，经过实测，slice效率高于map一个数量级
//
//        Version:  1.0
//        Created:  07/02/2018 05:54:42 PM
//       Compiler:  g++
//
// =====================================================================================
*/

package cache

import (
	"github.com/golang/glog"
)

type RingCacheArray struct {
	name    string           // name of instance
	reserve int              // how many second reserved
	items   []*ringDataArray // ring data
}

type ringDataArray struct {
	timestamp uint32 // first timestamp of all item's timeflow
	// highest bit for index for data[index], the others bits for idx for data[index][idx]
	index int // first index of ring buffer(data)
	// 0xffffffff for null
	data []int64 // double ring buffer for saving data
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Init
//  Description:  reserve表示以多长时间（单位：秒）为一个ring，进行双buffer切换
// =====================================================================================
*/
func (rc *RingCacheArray) Init(name string, reserve int) {
	glog.Infof("RingCacheArray.init(): name[%s]|reserve[%d]", name, reserve)
	rc.name = name
	rc.reserve = reserve
	rc.items = make([]*ringDataArray, 0)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  PuahBack
//  Description:
// =====================================================================================
*/
func (rc *RingCacheArray) PushBack(offset int, timestamp uint32, data int64) bool {
	var it *ringDataArray
	// 如果数组不够，则新增数据，并填充初始值
	if len(rc.items) <= offset {
		for i := len(rc.items); i < offset+1; i++ {
			it = new(ringDataArray)
			it.timestamp = timestamp - uint32(rc.reserve)
			it.index = 2*rc.reserve - 1
			it.data = make([]int64, 2*rc.reserve)
			for i := 0; i < len(it.data); i++ {
				// 全1代表空
				it.data[i] = 0xffffffff
			}
		}
		rc.items = append(rc.items, it)
	} else if rc.items[offset] == nil {
		it = new(ringDataArray)
		it.timestamp = timestamp - uint32(rc.reserve)
		it.index = 2*rc.reserve - 1
		it.data = make([]int64, 2*rc.reserve)
		for i := 0; i < len(it.data); i++ {
			// 全1代表空
			it.data[i] = 0xffffffff
		}
		rc.items[offset] = it
	} else {
		it = rc.items[offset]
	}

	var diff int
	if timestamp < it.timestamp {
		diff = int(it.timestamp - timestamp)
		if diff > rc.reserve {
			glog.Errorf("RingCacheArray[%d] push back data[%d], timestamp[%d] < a reservation[%d] of current timestampe[%d]",
				offset, data, timestamp, rc.reserve, it.timestamp)
			return false
		}
		// 对历史数据进行修改，主要用于数据返回超时的情况
		if it.index < rc.reserve {
			it.data[2*rc.reserve-diff] = data
		} else {
			it.data[rc.reserve-diff] = data
		}
		return true
	}
	// 如果不是连续写入，需要把中间的部分置空
	diff = int(timestamp - it.timestamp)
	if it.index < rc.reserve {
		diff -= it.index
	} else {
		diff -= it.index - rc.reserve
	}
	for i := 1; i < diff; i++ {
		it.index = (it.index + 1) % (2 * rc.reserve)
		if it.index%rc.reserve == 0 {
			it.timestamp += uint32(rc.reserve)
		}
		it.data[it.index] = 0xffffffff
	}
	// 将目标位置写入数据
	it.index = (it.index + 1) % (2 * rc.reserve)
	if it.index%rc.reserve == 0 {
		it.timestamp += uint32(rc.reserve)
	}
	it.data[it.index] = data

	return true
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  QueryPrevious
//  Description:  获取上一个时间周期的数据
// =====================================================================================
*/
func (rc *RingCacheArray) QueryPrevious(offset int) (uint32, []int64) {
	var it *ringDataArray
	var result []int64

	if len(rc.items) <= offset {
		glog.Errorf("item[%d] not exist", offset)
		return 0, nil
	}

	it = rc.items[offset]

	if it.index < rc.reserve {
		result = it.data[rc.reserve:]
	} else {
		result = it.data[:rc.reserve]
	}
	return it.timestamp - uint32(rc.reserve), result
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  QueryCurrent
//  Description:  获取最新的数据，最新数据包含最少半分钟，最多一分半的数据
// =====================================================================================
*/
func (rc *RingCacheArray) QueryCurrent(offset int) (uint32, []int64) {
	var it *ringDataArray
	var result []int64

	if len(rc.items) <= offset {
		glog.Errorf("item[%d] not exist", offset)
		return 0, nil
	}

	it = rc.items[offset]

	if it.index < rc.reserve {
		result = it.data[0 : it.index+1]
	} else {
		result = it.data[rc.reserve : it.index+1]
	}
	return it.timestamp, result
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  CleanExpire
 *  Description:  clean expire item by timestamp
 * =====================================================================================
 */
func (rc *RingCacheArray) CleanExpire(timestamp uint32) {
	for i, it := range rc.items {
		if it.timestamp+5*uint32(rc.reserve) < timestamp {
			rc.items[i] = nil
		}
	}
}
