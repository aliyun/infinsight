/*
// =====================================================================================
//
//       Filename:  stepInterface.go
//
//    Description:
//
//        Version:  1.0
//        Created:  06/11/2018 05:24:37 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package workflow

/*
// ===  INTERFACE  =====================================================================
//         Name:  StepInterface
//  Description:
// =====================================================================================
*/
type StepInterface interface {
	// step的名字
	Name() string
	// step执行后的错误信息（针对step的异步执行场景）
	Error() error

	// step前置操作（虽然有返回值，但不建议让Before出错）
	// 返回true，继续执行DoStep，返回false，跳过这一步
	Before(input interface{}, params ...interface{}) (bool, error)
	// step操作
	DoStep(input interface{}, params ...interface{}) (interface{}, error)
	// step后置操作（虽然有返回值，但不建议让After出错）
	// 返回true，继续执行下一步，返回false，整个workflow直接结束
	After(input interface{}, params ...interface{}) (bool, error)
}