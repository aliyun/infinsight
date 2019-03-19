/*
// =====================================================================================
//
//       Filename:  suggestionHandler.go
//
//    Description:  范围查询处理函数
//
//        Version:  1.0
//        Created:  08/16/2018 05:41:09 PM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package handler

import (
	"encoding/json"
	"fmt"
	"inspector/api_server/configure"
	"inspector/dict_server"
	"inspector/util/unsafe"
	"net/http"

	"github.com/golang/glog"
)

/*
// =====================================================================================
//  data model
// =====================================================================================
*/
type PrometheusSuggestionModel struct {
	Status string   `json:"status"` // success or failure
	Data   []string `json:"data"`
}

/*
// =====================================================================================
//  handler
// =====================================================================================
*/
func (h *ApiHandler) SuggestionHandler(w http.ResponseWriter, r *http.Request) {
	glog.V(1).Infof("[Trace][SuggestionHandler] called: Request[%v] ", r)

	var result = &PrometheusSuggestionModel{}
	result.Status = "success"
	result.Data = make([]string, 0)

	configure.Options.DictServerMap.Range(func(k, v interface{}) bool {
		keyList, err := v.(*dictServer.DictServer).GetKeyList()
		if err != nil {
			glog.Warning("get key list of [%s] error: %s", k, err.Error())
			return false
		}
		for _, it := range keyList {
			result.Data = append(result.Data, it)
		}
		return true
	})

	var resultBytes, _ = json.Marshal(result)
	fmt.Fprintln(w, unsafe.Bytes2String(resultBytes))
}
