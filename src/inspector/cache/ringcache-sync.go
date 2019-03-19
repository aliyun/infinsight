/*
// =====================================================================================
//
//       Filename:  ringcache-sync.go
//
//    Description:  基于时间的环形队列，只支持int的存储（partable thread-safe）
//
//        Version:  1.0
//        Created:  07/02/2018 05:54:42 PM
//       Compiler:  g++
//
// =====================================================================================
*/

package cache

import (
	"sync"

	"github.com/golang/glog"
)

type RingCacheSync struct {
	name    string     // name of instance
	reserve int        // how many second reserved
	locker  sync.Mutex // mutex lock for sync
	items   sync.Map   // ring data
}

type ringDataSync struct {
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
func (rc *RingCacheSync) Init(name string, reserve int) {
	glog.Infof("RingCacheSync.init(): name[%s]|reserve[%d]", name, reserve)
	rc.name = name
	rc.reserve = reserve
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  PuahBack
//  Description:
// =====================================================================================
*/
func (rc *RingCacheSync) PushBack(name string, timestamp uint32, data int64) bool {
	var ok bool
	var it *ringDataSync
	var i interface{}
	if i, ok = rc.items.Load(name); !ok {
		it = new(ringDataSync)
		it.timestamp = timestamp - uint32(rc.reserve)
		it.index = 2*rc.reserve - 1
		it.data = make([]int64, 2*rc.reserve)
		for i := 0; i < len(it.data); i++ {
			// 全1代表空
			it.data[i] = 0xffffffff
		}
		// 这里有可能被别人push（几乎不可能），但也无所谓，谁后写谁厉害
		rc.items.LoadOrStore(name, it)
	} else {
		if it, ok = i.(*ringDataSync); !ok {
			panic("ring cache type error")
		}
	}

	var diff int
	if timestamp < it.timestamp {
		diff = int(it.timestamp - timestamp)
		if diff > rc.reserve {
			glog.Errorf("RingCacheSync[%s] push back data[%d], timestamp[%d] < a reservation[%d] of current timestampe[%d]",
				name, data, timestamp, rc.reserve, it.timestamp)
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
func (rc *RingCacheSync) QueryPrevious(name string) (uint32, []int64) {
	var ok bool
	var it *ringDataSync
	var result []int64
	var i interface{}

	if i, ok = rc.items.Load(name); !ok {
		glog.Errorf("item[%s] not exist", name)
		return 0, nil
	}

	if it, ok = i.(*ringDataSync); !ok {
		panic("ring cache type error")
	}

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
func (rc *RingCacheSync) QueryCurrent(name string) (uint32, []int64) {
	var ok bool
	var it *ringDataSync
	var result []int64
	var i interface{}

	if i, ok = rc.items.Load(name); !ok {
		glog.Errorf("item[%s] not exist", name)
		return 0, nil
	}

	if it, ok = i.(*ringDataSync); !ok {
		panic("ring cache type error")
	}

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
func (rc *RingCacheSync) CleanExpire(timestamp uint32) {
	var deleteList []string = make([]string, 0)
	rc.items.Range(func(k, v interface{}) bool {
		var ok bool
		var key string
		var value *ringDataSync

		if key, ok = k.(string); !ok {
			panic("ring cache key type error")
		}

		if value, ok = v.(*ringDataSync); !ok {
			panic("ring cache value type error")
		}

		// 超过5倍保留时间（一般5分钟）没有新数据，则删除key
		if value.timestamp+5*uint32(rc.reserve) < timestamp {
			deleteList = append(deleteList, key)
		}

		return true
	})

	// 这里在极端极端情况下会导致数据丢失，不过目前暂不处理这种极端场景
	for _, it := range deleteList {
		rc.items.Delete(it)
	}
}
