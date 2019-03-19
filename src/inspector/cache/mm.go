/*
 * =====================================================================================
 *
 *       Filename:  mm.go
 *
 *    Description:  memory manager for timecache
 *
 *        Version:  1.0
 *        Created:  04/15/2018 13:48:32 PM
 *      Copyright:  All rights reserved
 *       Compiler:  go
 *
 *   Organization:  Alibaba-Inc
 *
 * =====================================================================================
 */

package cache

import (
	"container/list"
	"github.com/golang/glog"
	"sync"
)

type memManager struct {
	locker   sync.Mutex   // lock for Thread-safety
	pool     []*dataBlock // mem-pool for alloc and recycle
	recycler list.List    // Record the index of pool which can be alloc (uint16 stack)
	index    uint16       // current index for append data
}

type dataBlock struct {
	lastOffset uint32 // last offset for write data
	usedSize   uint32 // writen size
	data       []byte // max is 4MB
}

const blockMaxSize = 4 * 1024 * 1024 // 4MB

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  init
 *  Description:  init mem manager
 * =====================================================================================
 */
func (mm *memManager) init() {
	glog.Info("mm.init()")
	mm.pool = make([]*dataBlock, 0, 1024)
	db := new(dataBlock)
	db.data = make([]byte, blockMaxSize, blockMaxSize)
	mm.pool = append(mm.pool, db)
	mm.index = 0
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  append
 *  Description:  append data and return begin index
 * =====================================================================================
 */
func (mm *memManager) append(data []byte) (index uint16, offset uint32, ok bool) {
	if index, offset, ok = mm.alloc(len(data)); !ok {
		glog.Error("append error, data len[%d]", len(data))
	}

	copy(mm.pool[index].data[offset:], data)
	ok = true

	return
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  remove
 *  Description:  record size to remove, if block is empty then recycle to pool
 * =====================================================================================
 */
func (mm *memManager) remove(index uint16, size uint32) {
	mm.locker.Lock()
	defer mm.locker.Unlock()
	mm.pool[index].usedSize -= size
	if mm.pool[index].usedSize == 0 {
		mm.pool[index].lastOffset = 0
		mm.recycleBlock(index)
		glog.Infof("mm.remove() with recycle: index[%d]", index)
	}
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  locate
 *  Description:  data location
 * =====================================================================================
 */
func (mm *memManager) locate(index uint16, offset int) []byte {
	return mm.pool[index].data[offset:]
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  alloc
 *  Description:  alloc aconfirmed size
 * =====================================================================================
 */
func (mm *memManager) alloc(size int) (uint16, uint32, bool) {
	var index uint16
	var offset uint32
	var ok bool

	mm.locker.Lock()
	if int(blockMaxSize-mm.pool[mm.index].lastOffset) <= size {
		index, ok = mm.allocBlock()
		if !ok {
			mm.locker.Unlock()
			glog.Error("mm.alloc() error, mm is full")
			return 0, 0, false
		}
		glog.Infof("mm.alloc(%d): index[%d]", size, index)
		mm.index = index
		offset = 0
	} else {
		index = mm.index
		offset = mm.pool[mm.index].lastOffset
	}
	mm.pool[index].usedSize += uint32(size)
	mm.pool[index].lastOffset += uint32(size)
	mm.locker.Unlock()

	return index, offset, true
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  allocBlock
 *  Description:  new block from pool or os
 * =====================================================================================
 */
func (mm *memManager) allocBlock() (index uint16, ok bool) {
	if mm.recycler.Len() > 0 { // use recycled mem-block
		e := mm.recycler.Back()
		if e != nil {
			mm.recycler.Remove(e)
			index = e.Value.(uint16)
			ok = true
			return
		} else {
			panic("mm.allocBlock(): you can never go to here")
		}
		glog.Infof("mm.allocBlock() from recycler: index[%d]", index)
	} else { // allocBlock new mem-block
		newIndex := uint16(len(mm.pool))
		if newIndex == 0xffff {
			glog.Errorf("mm.allocBlock() is not enough, index[%d]", newIndex)
			return 0, false
		}
		db := new(dataBlock)
		db.data = make([]byte, blockMaxSize, blockMaxSize)
		mm.pool = append(mm.pool, db)
		glog.Infof("mm.allocBlock() from os: index[%d]", index)
		return newIndex, true
	}
	return 0, false
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  recycleBlock
 *  Description:  save empty block to pool
 * =====================================================================================
 */
func (mm *memManager) recycleBlock(index uint16) {
	mm.recycler.PushBack(index)
}
