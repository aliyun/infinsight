/*
// =====================================================================================
//
//       Filename:  mock.go
//
//    Description:
//
//        Version:  1.0
//        Created:  06/10/2018 10:18:31 AM
//       Compiler:  go1.10.3
//
// =====================================================================================
*/

package handler

import (
	"fmt"
	"net/http"
)

func ShowContext(r *http.Request) {
	fmt.Printf("%v\n", r.Method)
	fmt.Printf("%v\n", r.URL.RequestURI())
	fmt.Printf("%v\n", r.URL.Query())
}

func IndexHandler(w http.ResponseWriter, r *http.Request) {
	// fmt.Println("path: /")
	// ShowContext(r)
	fmt.Fprintln(w, "inspector api is running")
}
