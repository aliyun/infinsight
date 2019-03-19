/*
// =====================================================================================
//
//       Filename:  hash.go
//
//    Description:  提供各种Hash算法
//
//        Version:  1.0
//        Created:  07/16/2018 07:23:21 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package util

import (
	"crypto/md5"
	"encoding/binary"
	"sort"
	"strings"

	"inspector/util/unsafe"
)

const MaxUint64 uint64 = 1<<64 - 1

/*
// ===  FUNCTION  ======================================================================
//         Name:  Md5
//  Description:  128位md5
// =====================================================================================
*/
func Md5(data []byte) [16]byte {
	return md5.Sum(data)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Md5
//  Description:  64位md5
// =====================================================================================
*/
func Md5In64(data []byte) uint64 {
	var md5 = md5.Sum(data)
	var lowMd5 = md5[0:8]
	return binary.LittleEndian.Uint64(lowMd5)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  ConsistentHash
//  Description:  一致性hash
// =====================================================================================
*/
func ConsistentHash(data []byte, nodeCount int) int {
	var sectionLen = MaxUint64 / uint64(nodeCount)
	return int(Md5In64(data) / sectionLen)
}

// hash instance. hash by hid if pid == 0, otherwise by pid
// todo, pid is useless current.
func HashInstance(pid uint32, hid int32, nr int) int {
	pid = 0 // todo

	key := make([]byte, 4)
	if pid == 0 { // normal, hash by hid
		binary.LittleEndian.PutUint32(key, uint32(hid))
	} else {
		binary.LittleEndian.PutUint32(key, pid)
	}
	return ConsistentHash(key, nr)
}

// hash instance by hid
func HashInstanceByHid(hid int32, nr int) int {
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, uint32(hid))

	ans := ConsistentHash(key, nr)

	return ans
}

// calculate md5sum from map list that stores instance map
func CalInstanceListMd5(input []map[string]interface{}, hashKeyLists []string) []byte {
	innerList := make([]string, 0, len(input) + 1)
	for _, innerVal := range input {
		s := new(strings.Builder)
		for _, hashKey := range hashKeyLists {
			if v, ok := innerVal[hashKey]; ok {
				s.WriteString(v.(string))
				s.WriteString("+")
			}
		}
		innerList = append(innerList, s.String())
	}
	sort.Strings(innerList)
	join := strings.Join(innerList, ";")
	ans := Md5(unsafe.String2Bytes(join))
	return ans[:]
}
