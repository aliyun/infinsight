/*
 * =====================================================================================
 *
 *       Filename:  ds.go
 *
 *    Description:  describe the core data structure of timecache
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
	"fmt"
	"sync"

	"github.com/golang/glog"
)

/*
 * ===  STRUCT  ========================================================================
 *         Name:  instance
 *  Description:
 * =====================================================================================
 */
type instance struct {
	name        string           // name of instance
	timeReserve uint32           // max duration of time remain(unit:second)
	lastTime    uint32           // the last timestamp for testing data continuity, and check timeout
	count       int              // how many data in one block
	step        int              // duration(seconds) of two adjacent data
	maxSize     int              // max size of []dataIndex
	locker      sync.RWMutex     // rw-lock for sync
	mm          *memManager      // memory manager for cache
	items       map[string]*item // item data
}

/*
 * ===  STRUCT  ========================================================================
 *         Name:  item
 *  Description:
 * =====================================================================================
 */
type item struct {
	timestamp uint32      // first timestamp of all item's timeflow
	index     int         // first index of ring buffer([]dataIndex)
	data      []dataIndex // ring buffer for saving data
}

/*
 * ===  STRUCT  ========================================================================
 *         Name:  dataIndex
 *  Description:
 * =====================================================================================
 */
type dataIndex struct {
	// max: 65536 blocks
	blockIndex uint16
	// high 22bit for offset in block(max: 4MB)
	// low  10bit for data size(max: 1KB)
	dataLocate uint32
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  init
 *  Description:  init instance, memManager can shared with other instance
 * =====================================================================================
 */
func (ins *instance) init(name string, mm *memManager, lastTime uint32, timeReserve uint32, count int, step int) {
	glog.Infof("instance.init(): name[%s]|mm[%p]|lastTime[%d]|timeReserve[%d]|count[%d]|step[%d]",
		name, mm, lastTime, timeReserve, count, step)
	ins.name = name
	ins.mm = mm
	ins.timeReserve = timeReserve
	ins.lastTime = lastTime
	ins.count = count
	ins.step = step
	ins.maxSize = int(ins.timeReserve / uint32(ins.count*ins.step))
	if ins.timeReserve%uint32(ins.count*ins.step) != 0 {
		ins.maxSize++
	}
	ins.items = make(map[string]*item)
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  rRangeLocker
 *  Description:  lock by rlock and return runlock func
 * =====================================================================================
 */
func (ins *instance) rRangeLocker() (unlock func()) {
	ins.locker.RLock()
	return func() {
		ins.locker.RUnlock()
	}
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  wRangeLocker
 *  Description:  lock by wlock and return wunlock func
 * =====================================================================================
 */
func (ins *instance) wRangeLocker() (unlock func()) {
	ins.locker.Lock()
	return func() {
		ins.locker.Unlock()
	}
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  pushBack
 *  Description:  push data to back of timeflow(not thread safe)
 * =====================================================================================
 */
func (ins *instance) pushBack(name string, timestamp uint32, data []byte) bool {
	var ok bool
	var it *item
	if len(data) <= 0 {
		glog.Errorf("instance[%s] item[%s] data is null", ins.name, name)
		return false
	} else if len(data) > 1024 {
		glog.Errorf("instance[%s] item[%s] data is too long. size[%d] is out of max[1024]",
			ins.name, name, len(data))
		return false
	}
	if it, ok = ins.items[name]; !ok {
		it = new(item)
		it.timestamp = timestamp
		it.index = 0
		it.data = make([]dataIndex, ins.maxSize)
		ins.items[name] = it
		glog.V(3).Infof("instance[%s] create item[%s]", ins.name, name)
	} else if timestamp < it.timestamp {
		glog.Errorf("instance[%s] item[%s] timestamp[%d] < min timestamp[%d]",
			ins.name, name, timestamp, it.timestamp)
		return false
	}

	var newindex int = ins.doClean(name, timestamp)
	// insert(update) new data
	var di *dataIndex = &it.data[newindex]
	if di.dataLocate != 0 {
		var size uint32 = di.dataLocate & 0x000003ff
		ins.mm.remove(di.blockIndex, size)
	}
	di.blockIndex, di.dataLocate, ok = ins.mm.append(data)
	if !ok {
		glog.Error("mm append error", timestamp, it.timestamp)
		return false
	}
	di.dataLocate <<= 10
	di.dataLocate |= uint32(len(data))

	ins.lastTime = timestamp

	return true
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  queryRange
 *  Description:  query data from a time range
 * =====================================================================================
 */
func (ins *instance) queryRange(name string, timeBegin uint32, timeEnd uint32) (uint32, uint32, [][]byte) {
	var ok bool
	var it *item

	if it, ok = ins.items[name]; !ok {
		var errmsg = fmt.Sprintf("item[%s] not exist", name)
		glog.Errorf(errmsg)
		return 0, 0, nil
	}

	if timeBegin > it.timestamp+ins.timeReserve || timeEnd < it.timestamp {
		var errmsg = fmt.Sprintf("item[%s] time (%d) not in range:(%d-%d)",
			name, timeBegin, it.timestamp, it.timestamp+ins.timeReserve)
		glog.Errorf(errmsg)
		return 0, 0, nil
	}

	var span int = ins.count * ins.step

	// calculate valid timebegin timeend and locate index
	if timeBegin < it.timestamp {
		timeBegin = it.timestamp
	}
	if timeEnd > it.timestamp+ins.timeReserve-1 {
		timeEnd = it.timestamp + ins.timeReserve - 1
	}
	var indexBegin int = (it.index + int(timeBegin-it.timestamp)/span) % ins.maxSize
	var indexEnd int = (it.index + int(timeEnd-it.timestamp)/span) % ins.maxSize
	var lastIndex int = 0

	var result [][]byte = make([][]byte, 0, int(timeEnd-timeBegin)/span)
	var dataCount int = 0

	// read data
	for i := indexBegin; i != indexEnd; i = (i + 1) % ins.maxSize {
		index := it.data[i].blockIndex
		offset := it.data[i].dataLocate >> 10
		size := it.data[i].dataLocate & 0x000003ff

		if size != 0 {
			data := ins.mm.locate(index, int(offset))
			result = append(result, data[:size])
			dataCount++
			lastIndex = i
		} else {
			result = append(result, nil)
		}
	}
	// read last data, can't read from previous for-loop because
	// if indexEnd + 1 == indexBegin, for-loop will quite immediately
	if it.data[indexEnd].dataLocate != 0 {
		index := it.data[indexEnd].blockIndex
		offset := it.data[indexEnd].dataLocate >> 10
		size := it.data[indexEnd].dataLocate & 0x000003ff

		if size != 0 {
			data := ins.mm.locate(index, int(offset))
			result = append(result, data[:size])
			dataCount++
			lastIndex = indexEnd
		} else {
			result = append(result, nil)
		}
	}
	if dataCount == 0 {
		var errmsg = fmt.Sprintf("item[%s] in query range:(%d-%d) is empty", name, timeBegin, timeEnd)
		glog.Errorf(errmsg)
		return 0, 0, nil
	}

	if indexBegin < it.index {
		indexBegin += ins.maxSize
	}
	timeBegin = it.timestamp + uint32(indexBegin-it.index)*uint32(span)
	if lastIndex < it.index {
		lastIndex += ins.maxSize
	}
	timeEnd = it.timestamp + uint32(lastIndex+1-it.index)*uint32(span) - 1
	return timeBegin, timeEnd, result[:lastIndex-indexBegin+1]
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  autoClean
 *  Description:  auto clean with timestamp
 * =====================================================================================
 */
func (ins *instance) autoClean(name string, timestamp uint32) {
	var ok bool
	var it *item
	if it, ok = ins.items[name]; !ok {
		return
	} else {
		if timestamp < it.timestamp+ins.timeReserve {
			return
		}
	}

	var newindex int = ins.doClean(name, timestamp)
	var di *dataIndex = &it.data[newindex]
	if di.dataLocate != 0 {
		var size uint32 = di.dataLocate & 0x000003ff
		ins.mm.remove(di.blockIndex, size)
		di.blockIndex = 0
		di.dataLocate = 0
	}

	if timestamp-it.timestamp > 2*ins.timeReserve {
		delete(ins.items, name)
	}
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  doClean
 *  Description:  clean data by timestamp and return new index
 * =====================================================================================
 */
func (ins *instance) doClean(name string, timestamp uint32) int {
	var it *item = ins.items[name]
	var span int = ins.count * ins.step
	var timediff uint32 = timestamp - it.timestamp
	var indexdiff int = int(timediff) / span
	var newindex int = (it.index + indexdiff) % ins.maxSize

	// clean older data if exist
	if timediff >= ins.timeReserve {
		var iRange = indexdiff - int(ins.timeReserve)/span
		for i := 0; i < iRange; i++ {
			var di *dataIndex = &it.data[(it.index+i)%ins.maxSize]
			if di.dataLocate != 0 {
				var size uint32 = di.dataLocate & 0x000003ff
				ins.mm.remove(di.blockIndex, size)
				di.blockIndex = 0
				di.dataLocate = 0
			}
		}
	}

	// update instance meta
	if timediff >= ins.timeReserve {
		it.timestamp = it.timestamp + uint32(indexdiff)*uint32(span) - ins.timeReserve + uint32(span)
		it.index = (newindex + 1) % ins.maxSize
	}

	return newindex
}
