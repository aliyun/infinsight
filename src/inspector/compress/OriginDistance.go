/*
// =====================================================================================
//
//       Filename:  OriginDistance.go
//
//    Description:  compress data base on the distance to origin,
//                  turn a negative number into a positive number.
//                  for example:
//                  -5, -4, -3, -2, -1, nil, 0, 1, 2, 3, 4, 5
//                  10,  8,  6,  4,  2,  0,  1, 3, 5, 7, 9, 11
//
//        Version:  1.0
//        Created:  07/25/2018 05:11:35 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package compress

import (
	"inspector/util"
)

/*
// ===  FUNCTION  ======================================================================
//         Name:  OriginDistanceEncode
//  Description:  zig-zag in fix size
// =====================================================================================
*/
func OriginDistanceEncode(x int64) uint64 {
	if x >= util.InvalidPoint || -(x+1) > util.InvalidPoint {
		return 0
	}

	var ux = uint64(x)
	if x < 0 {
		return (^ux + 1) << 1 // 2, 4, 6, 8, ...
	}
	return ((ux + 1) << 1) - 1 // 1, 3, 5, 7, ...
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  OriginDistanceDecode
//  Description:  zig-zag in fix size
// =====================================================================================
*/
func OriginDistanceDecode(ux uint64) int64 {
	if ux == 0 {
		return util.NullData
	}

	if ux%2 == 0 {
		return int64(^((ux >> 1) - 1))
	} else {
		return int64((ux+1)>>1) - 1
	}
}
