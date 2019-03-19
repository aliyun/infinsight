/*
// =====================================================================================
//
//       Filename:  net.go
//
//    Description:
//
//        Version:  1.0
//        Created:  08/30/2018 08:26:39 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package util

import (
	"fmt"
	"net"
	"regexp"
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
	TB = 1024 * GB
	PB = 1024 * TB
)

// get all net address that has prefix 'eth', 'bond' or 'en'
func GetAllNetAddr() ([]string, error) {
	ifs, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("get network interface failed. %v", err)
	}

	var ret []string
	var appendByName = func(prefix string) {
		for _, it := range ifs {
			addrs, err := it.Addrs()
			if err != nil {
				continue
			}

			if ok, err := regexp.MatchString("^("+prefix+").*", it.Name); err != nil || !ok {
				continue
			}
			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
					if ipnet.IP.To4() != nil {
						ret = append(ret, ipnet.IP.String())
					}
				}
			}
		}
	}

	appendByName("bond")
	appendByName("eth")
	appendByName("en")

	return ret, nil
}

func ConvertTraffic(traffic uint64) string {
	switch {
	case traffic > PB:
		return fmt.Sprintf("%dPB", traffic/PB)
	case traffic > TB:
		return fmt.Sprintf("%dTB", traffic/TB)
	case traffic > GB:
		return fmt.Sprintf("%dGB", traffic/GB)
	case traffic > MB:
		return fmt.Sprintf("%dMB", traffic/MB)
	case traffic > KB:
		return fmt.Sprintf("%dKB", traffic/KB)
	default:
		return fmt.Sprintf("%dB", traffic)
	}
}