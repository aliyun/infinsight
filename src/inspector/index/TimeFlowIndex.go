/*
// =====================================================================================
//
//       Filename:  TimeFlowIndex.go
//
//    Description:  基于递增时间流的索引
//
//        Version:  1.0
//        Created:  07/16/2018 08:27:20 PM
//       Compiler:  g++
//
// =====================================================================================
*/

package index

import "fmt"
import "errors"
import "container/list"

type TFIndexKeyTimeRange struct {
	Key       string
	TimeBegin uint32
	TimeEnd   uint32
}

type TFIndex struct {
	mainKey    string
	indexKey   string
	currentKey string
	timeLink   *list.List
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  NewTimeFlowIndex
//  Description:
// =====================================================================================
*/
func NewTimeFlowIndex(mainKey, indexKey string) *TFIndex {
	var index = new(TFIndex)
	index.mainKey = mainKey
	index.timeLink = new(list.List)
	return index
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  PushBack
//  Description:
// =====================================================================================
*/
func (index *TFIndex) PushBack(key string, timestamp uint32) error {
	var element *list.Element = nil
	if index.currentKey != key {
		element = index.timeLink.Back()
		if element != nil {
			var currentRange = element.Value.(*TFIndexKeyTimeRange)
			currentRange.TimeEnd = timestamp - 1
		}

		var currentRange = &TFIndexKeyTimeRange{TimeBegin: timestamp, Key: key}
		index.timeLink.PushBack(currentRange)
		index.currentKey = key
	}
	element = index.timeLink.Back()
	var currentRange = element.Value.(*TFIndexKeyTimeRange)
	if timestamp > currentRange.TimeEnd {
		currentRange.TimeEnd = timestamp
	} else if timestamp < currentRange.TimeBegin {
		var trCurrent = currentRange
		element = element.Prev()
		currentRange = element.Value.(*TFIndexKeyTimeRange)
		if timestamp < currentRange.TimeBegin {
			return errors.New(fmt.Sprintf("timestamp[%d] is too old than prev timeRange(%d-%d)", timestamp, currentRange.TimeBegin, currentRange.TimeEnd))
		} else {
			trCurrent.TimeBegin = timestamp
			currentRange.TimeEnd = timestamp - 1
		}
	} else {
		return nil
	}
	return nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  CheckIndex
//  Description:
// =====================================================================================
*/
func (index *TFIndex) CheckIndex(timestamp uint32) (string, error) {
	for element := index.timeLink.Front(); element != nil; element = element.Next() {
		var currentRange = element.Value.(*TFIndexKeyTimeRange)
		if currentRange.TimeBegin > timestamp {
			return "", errors.New("index not found")
		}
		if currentRange.TimeEnd < timestamp {
			continue
		}
		return currentRange.Key, nil
	}
	return "", errors.New("index not found")
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  CheckIndexRange
//  Description:
// =====================================================================================
*/
func (index *TFIndex) CheckIndexRange(timeBegin, timeEnd uint32) ([]TFIndexKeyTimeRange, error) {
	var result = make([]TFIndexKeyTimeRange, 0)

	// check out of range
	var element = index.timeLink.Front()
	if element == nil {
		return nil, errors.New("index not exist")
	}
	var currentRange = element.Value.(*TFIndexKeyTimeRange)
	if currentRange.TimeBegin > timeEnd {
		return nil, errors.New(fmt.Sprintf("timeBegin[%d] less than minimum[%d]", timeBegin, currentRange.TimeBegin))
	}

	// find
	for element != nil {
		currentRange = element.Value.(*TFIndexKeyTimeRange)
		if currentRange.TimeEnd < timeBegin {
			element = element.Next()
			continue
		} else {
			result = append(result, TFIndexKeyTimeRange{
				Key:       currentRange.Key,
				TimeBegin: max(currentRange.TimeBegin, timeBegin),
				TimeEnd:   min(currentRange.TimeEnd, timeEnd),
			})
			if currentRange.TimeEnd < timeEnd {
				element = element.Next()
				continue
			} else {
				return result, nil
			}
		}
	}

	// check out of range
	if len(result) == 0 {
		element = index.timeLink.Back()
		currentRange = element.Value.(*TFIndexKeyTimeRange)
		return nil, errors.New(fmt.Sprintf("timeBegin[%d] great than maximum[%d]", timeBegin, currentRange.TimeEnd))
	}

	return result, nil
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  CurrentKey
//  Description:
// =====================================================================================
*/
func (index *TFIndex) CurrentKey() string {
	return index.currentKey
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  CleanPreviousFrom
//  Description:
// =====================================================================================
*/
func (index *TFIndex) CleanPreviousTo(timestamp uint32) {
	var element = index.timeLink.Front()
	for element != nil {
		var currentRange = element.Value.(*TFIndexKeyTimeRange)
		if currentRange.TimeBegin <= timestamp {
			if currentRange.TimeEnd < timestamp {
				var next = element.Next()
				index.timeLink.Remove(element)
				element = next
				continue
			} else {
				currentRange.TimeBegin = timestamp
			}
		}
		element = element.Next()
	}
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  min & max
//  Description:
// =====================================================================================
*/
func min(x, y uint32) uint32 {
	if x < y {
		return x
	} else {
		return y
	}
}

func max(x, y uint32) uint32 {
	if x > y {
		return x
	} else {
		return y
	}
}
