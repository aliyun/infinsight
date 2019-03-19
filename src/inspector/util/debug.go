package util

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"inspector/util/unsafe"
)

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  ShowData
 *  Description:
 * =====================================================================================
 */
func ShowData(data []byte) string {
	var dataBuffer = bytes.NewBuffer([]byte{})
	var n = 0

	// read list count
	var listCount = binary.BigEndian.Uint32(data[n : n+4])
	n += 4

	// read list
	for i := 0; i < int(listCount); i++ {
		// read key size
		var keySize = binary.BigEndian.Uint32(data[n : n+4])
		n += 4

		// read key
		var key = unsafe.Bytes2String(data[n : n+int(keySize)])
		n += int(keySize)
		dataBuffer.WriteString(fmt.Sprintf("%s:{", key))

		// read value count
		var valueCount = binary.BigEndian.Uint32(data[n : n+4])
		n += 4

		// read value
		for j := 0; j < int(valueCount); j++ {
			// read timestamp
			var timestamp = binary.BigEndian.Uint32(data[n : n+4])
			n += 4
			dataBuffer.WriteString(fmt.Sprintf("[%d:", timestamp))

			// read valueSize
			var valueSize = binary.BigEndian.Uint32(data[n : n+4])
			n += 4

			// read value
			var value = data[n : n+int(valueSize)]
			n += int(valueSize)
			dataBuffer.WriteString(fmt.Sprintf("%v]", value))

		}
		dataBuffer.WriteString(fmt.Sprintf("}"))
	}

	return dataBuffer.String()
}
