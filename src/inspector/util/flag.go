/*
// =====================================================================================
//
//       Filename:  flag.go
//
//    Description:  控制信息（参考kill -l）
//
//        Version:  1.0
//        Created:  06/11/2018 07:06:59 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package util

type Flag int

const FLAGTERM Flag = 0   // 用于结束子routine
const FLAGCHLD Flag = 1   // 子routine退出时向父routine报告
const FLAGFINISH Flag = 2 // 子routine一次任务完成时向父routine报告
