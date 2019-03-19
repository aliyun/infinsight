/*
 * =====================================================================================
 *
 *       Filename:  timecache.go
 *
 *    Description:  cache based on time flow
 *
 *        Version:  1.0
 *        Created:  04/13/2018 12:48:22 PM
 *      Copyright:  All rights reserved
 *       Compiler:  go
 *
 *   Organization:  Alibaba-Inc
 *
 * =====================================================================================
 */

package cache

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
	"sync/atomic"

	"github.com/golang/glog"

	"inspector/proto/core"
	"inspector/util"
)

type Hash func(data []byte) uint32

var GlobalStat core.Stat

type instanceList struct {
	lockers       sync.Mutex // locker for list
	timeLevelList *list.List // instance list
}

type TimeCache struct {
	concurrencty uint32                     // how much pieces the cache divided
	timeReserve  uint32                     // max duration of time remain(unit:second)
	cache        []map[string]*instanceList // cache data
	lockers      []sync.RWMutex             // rwmutex for each instance map
	mm           []*memManager              // memory manager for cache
	hash         Hash                       // hash for split instance
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  NewTimeCache
 *  Description:  initialization and return a timecache
 *   Parameters:
 *	concurrencty: split instances set into n bucket, using hash to decide bucket
 * 	timeReserve: how long times that cache can reserve(unit: minute)
 *  ReturnValue:
 * =====================================================================================
 */
func NewTimeCache(concurrencty uint32, timeReserve uint32) *TimeCache {
	glog.Info("NewTimeCache: concurrencty[%d], timeReserve[%d]", concurrencty, timeReserve)
	tc := &TimeCache{}
	tc.concurrencty = concurrencty
	tc.timeReserve = timeReserve
	tc.cache = make([]map[string]*instanceList, concurrencty, concurrencty)
	tc.mm = make([]*memManager, concurrencty, concurrencty)
	for i := range tc.cache {
		tc.cache[i] = make(map[string]*instanceList)
		tc.mm[i] = new(memManager)
		tc.mm[i].init()
	}
	tc.lockers = make([]sync.RWMutex, concurrencty, concurrencty)
	tc.hash = crc32.ChecksumIEEE
	return tc
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  Set
 *  Description:
 * =====================================================================================
 */
func (tc *TimeCache) Set(info *core.Info) error {
	var insList *instanceList
	var tlList *list.List
	var ok bool

	// 将info的步长进行虚拟化，这里在结构上处理的并不好
	// 应该在统一位置，对时间戳，时间区间和步长做统一虚拟化
	// 但是由于之前已经对时间进行了虚拟化，所以目前只能以打补丁的形式再对步长进行虚拟化
	// 并且，这里直接对步长进行虚拟化，会导致数据保留时间为预设时间的N倍，过度占用内存
	info.Step = 1

	host := info.Header.Host
	timestamp := info.Timestamp
	items := info.Items

	index := tc.hash([]byte(host)) % tc.concurrencty

	// create new instance list
	if insList, ok = tc.cache[index][host]; !ok { // check if the instance exist
		tc.lockers[index].Lock()
		if _, ok = tc.cache[index][host]; !ok { // double check
			tc.cache[index][host] = new(instanceList)
			tc.cache[index][host].timeLevelList = new(list.List)
			tc.newInstance(host, index, info)
		}
		tc.lockers[index].Unlock()
		insList = tc.cache[index][host]
		atomic.AddUint64(&GlobalStat.InstanceCount, 1)
	}
	tlList = insList.timeLevelList

	tc.lockers[index].RLock()
	tc.cache[index][host].lockers.Lock()
	var instanceHit *instance = nil
	// find target instance in list and clean timeout data
	for e := tlList.Front(); e != nil; {
		ins := e.Value.(*instance)
		// var debugStart = time.Now()

		// count := int(info.Count)
		// step := int(info.Step)
		// if ins.count != count || ins.step != step || // check if instance's meta has changed
		// 	(timestamp-ins.lastTime)%uint32(count*step) != 0 { // check if data time is continuous
		if isSameTimeLevel(ins, info) == false {
			// remove element if timeout
			if ins.lastTime+tc.timeReserve < timestamp {
				var tmp = e.Next()
				tlList.Remove(e)
				e = tmp
				continue
			}
		} else {
			instanceHit = ins
		}
		e = e.Next()

		// var debugElapsed = time.Since(debugStart)
		// glog.V(3).Infof("Set instance[%s], time used[%v]", host, debugElapsed)
	}
	// create a new instance branch
	if instanceHit == nil {
		tc.newInstance(host, index, info)
		instanceHit = tlList.Back().Value.(*instance)

	}
	// insert data to exist instance
	unlock := instanceHit.wRangeLocker()
	for _, it := range items {
		if instanceHit.pushBack(it.Key, timestamp, it.Value) == false {
			var errmsg = fmt.Sprintf("instance[%s] pushback error: key[%s] timestamp[%d] value[%v]",
				host, it.Key, timestamp, it.Value)
			glog.Error(errmsg)
			unlock()
			tc.cache[index][host].lockers.Unlock()
			tc.lockers[index].RUnlock()
			return errors.New(errmsg)
		}
		atomic.AddUint64(&GlobalStat.ItemCount, 1)
		atomic.AddUint64(&GlobalStat.CacheSize, uint64(len(it.Key)+len(it.Value)))
	}
	unlock()
	tc.cache[index][host].lockers.Unlock()
	tc.lockers[index].RUnlock()
	return nil
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  Get
 *  Description:
 * =====================================================================================
 */
func (tc *TimeCache) Get(query *core.Query) ([]*core.InfoRange, uint32, uint32, error) {
	var insList *instanceList
	var tlList *list.List
	var timeBegin uint32 = util.UINT32_MAX
	var timeEnd uint32 = 0
	var ok bool

	var infoRanges []*core.InfoRange = make([]*core.InfoRange, 0)

	atomic.AddUint64(&GlobalStat.QCount, 1)

	index := tc.hash([]byte(query.Header.Host)) % tc.concurrencty

	tc.lockers[index].RLock()
	defer tc.lockers[index].RUnlock()

	if insList, ok = tc.cache[index][query.Header.Host]; !ok { // check if the instance exist
		var errmsg = fmt.Sprintf("instance[%s] in cache group [%d] not exist", query.Header.Host, index)
		glog.Error(errmsg)
		return nil, 0, 0, errors.New(errmsg)
	}
	tlList = insList.timeLevelList

	for e := tlList.Front(); e != nil; e = e.Next() {
		ins := e.Value.(*instance)
		unlock := ins.rRangeLocker()
		var infoRange core.InfoRange = core.InfoRange{}
		infoRange.Header = query.Header
		infoRange.Count = uint32(ins.count)
		infoRange.Step = uint32(ins.step)
		// infoRange.Items = make([]*core.Item, 0)
		span := ins.count * ins.step

		// create key list
		var keyList []string
		if query.KeyList == nil || len(query.KeyList) == 0 {
			keyList = make([]string, 0)
			for key, _ := range ins.items {
				keyList = append(keyList, key)
			}
		} else {
			keyList = query.KeyList
		}

		// range search each key
		bytesBuffer := bytes.NewBuffer([]byte{})
		// write key list size
		binary.Write(bytesBuffer, binary.BigEndian, uint32(len(keyList)))
		if len(keyList) != 0 {
			atomic.AddUint64(&GlobalStat.CacheHitCount, 1)
		}
		// var debugStart = time.Now()
		for _, key := range keyList {
			// write key size
			binary.Write(bytesBuffer, binary.BigEndian, uint32(len(key)))
			// write key
			bytesBuffer.WriteString(key)
			// query data
			tBegin, tEnd, data := ins.queryRange(key, query.TimeBegin, query.TimeEnd)
			if data != nil {
				if tBegin < timeBegin {
					timeBegin = tBegin
				}
				if tEnd > timeEnd {
					timeEnd = tEnd
				}
			} else {
				// 此处假设cache要么是完整数据，要么所有key都有，要么都没有
				// 如果发现部分key丢失，则视同所有key丢失
				glog.Warningf("can't find key[%s] in range(%d-%d)", key, query.TimeBegin, query.TimeEnd)
				binary.Write(bytesBuffer, binary.BigEndian, uint32(0))
				continue
			}
			// write data size
			binary.Write(bytesBuffer, binary.BigEndian, uint32(len(data)))
			// write data
			if data != nil {
				for i, it := range data {
					binary.Write(bytesBuffer, binary.BigEndian, tBegin+uint32(i*span))
					if it != nil {
						binary.Write(bytesBuffer, binary.BigEndian, uint32(len(it)))
						bytesBuffer.Write(it)
					} else {
						binary.Write(bytesBuffer, binary.BigEndian, uint32(0))
					}
					// fmt.Printf("debug query cache: key[%v] timestamp[%v] \n",
					// 	key, tBegin+uint32(i*span))
					// fmt.Printf("debug query cache: key[%v] timestamp[%v] value[%v]\n",
					// 	key, tBegin+uint32(i*span), it)
				}
			}
		}
		// var debugElapsed = time.Since(debugStart)
		// glog.V(3).Infof("Get instance[%s] key[%s], time used[%v]", query.Header.Host, keyList, debugElapsed)
		if bytesBuffer.Len() > 0 {
			infoRange.Data = bytesBuffer.Bytes()
			atomic.AddUint64(&GlobalStat.QSize, uint64(len(infoRange.Data)))
			unlock()
			// fmt.Printf("debug timecache meta: name[%v], timeReserve[%v], lastTime[%v], count[%v], step[%v], maxSize[%v], itemLen[%v]\n",
			// 	ins.name, ins.timeReserve, ins.lastTime, ins.count, ins.step, ins.maxSize, len(ins.items))
			infoRanges = append(infoRanges, &infoRange)
		}
	}

	if timeBegin == util.UINT32_MAX && timeEnd == 0 {
		glog.Warningf("can't all keys in range(%d-%d)", query.TimeBegin, query.TimeEnd)
		return nil, 0, 0, errors.New(fmt.Sprintf("can't find any data in range(%d-%d)", query.TimeBegin, query.TimeEnd))
	}
	return infoRanges, timeBegin, timeEnd, nil
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  Stat
 *  Description:
 * =====================================================================================
 */
func (tc *TimeCache) Stat() core.Stat {
	return GlobalStat
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  newInstance
 *  Description:
 * =====================================================================================
 */
func (tc *TimeCache) newInstance(name string, index uint32, info *core.Info) *instance {
	ins := new(instance)
	ins.init(name, tc.mm[index], info.Timestamp, tc.timeReserve, int(info.Count), int(info.Step))
	tc.cache[index][info.Header.Host].timeLevelList.PushBack(ins)
	glog.V(1).Infof("create instance[%s]", name)
	return ins
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  isSameTimeLevel
 *  Description:
 * =====================================================================================
 */
func isSameTimeLevel(ins *instance, info *core.Info) bool {
	if ins.count != int(info.Count) {
		return false
	}
	if ins.step != int(info.Step) {
		return false
	}
	var step = ins.count * ins.step
	var insTime = ins.lastTime
	var infoTime = info.GetTimestamp()
	if insTime < infoTime {
		if int(infoTime-insTime)%step != 0 {
			return false
		}
	} else {
		if int(insTime-infoTime)%step != 0 {
			return false
		}
	}

	return true
}
