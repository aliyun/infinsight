/*
// =====================================================================================
//
//       Filename:  stepStatConfig.go
//
//    Description:  对配置文件进行stat，查看是否有新的写入（mtime）是否已经改变
//
//        Version:  1.0
//        Created:  06/12/2018 06:42:56 PM
//       Compiler:  g++
//
// =====================================================================================
*/

package config

import (
	"os"
	"time"

	"github.com/golang/glog"
)

type stepStatConfig struct {
	filename string
	callback func(event WatcheEvent) error
	modTime  time.Time
	err      error
}

func (step *stepStatConfig) Name() string {
	return "stepStatConfig"
}

func (step *stepStatConfig) Error() error {
	return step.err
}

func (step *stepStatConfig) Before(input interface{}, params ...interface{}) (bool, error) {
	return true, nil
}

func (step *stepStatConfig) DoStep(input interface{}, params ...interface{}) (interface{}, error) {
	// stat file
	var fileinfo os.FileInfo
	var err error
	if fileinfo, err = os.Stat(step.filename); err != nil {
		glog.Warningf("stat file[%s] error[%s]", step.filename, err)
		step.err = err
		return nil, err
	}

	// check mtime
	// 有可能在调用ModTime()与callback()时又发生了变化，但是对最终一致性不会有影响
	var tmpTime = fileinfo.ModTime()
	if tmpTime != step.modTime {
		// 这里无论数据是否发生更改，都简单粗暴的认为更改了
		// 目前暂不处理Create和Delete的复杂逻辑，短时用不上
		if err = step.callback(NODECHANGED); err != nil {
			step.err = err
			return nil, err
		}
		step.modTime = tmpTime
	}

	step.err = nil
	return nil, nil
}

func (step *stepStatConfig) After(input interface{}, params ...interface{}) (bool, error) {
	return true, nil
}
