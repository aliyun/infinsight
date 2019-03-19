/*
// =====================================================================================
//
//       Filename:  Service.go
//
//    Description:  用于描述需要监控的服务类型
//
//        Version:  1.0
//        Created:  07/16/2018 07:12:05 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package model

type MetaType int

// =====================================================================================
//       Struct:  Service
//  Description: 表示一类的监控服务，比如MongoDB, Redis
//    Parameter:
//     MetaType: 从哪里拿数据
//     MetaType: 源串
// =====================================================================================
type Service struct {
	Name         string
	MetaType     string
	MetaSource   string
	Frequency    int
	InstanceList []Instance
}
