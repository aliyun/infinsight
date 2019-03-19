package connector

import (
	"fmt"
	"inspector/util"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/valyala/fasthttp"
)

const (
	httpStat            = "metric"
	httpTimeout         = 3
	httpMaxIdleConns    = 3
	httpIdleConnTimeout = 10
	httpKeepAlive       = 30
)

type httpConnector struct {
	service string // service name: mongodb, redis, redis_proxy

	// belows are generated inner
	address  string           // url
	cmds     []string         // cmds
	client   *fasthttp.Client // fasthttp client
	request  *fasthttp.Request
	response *fasthttp.Response
}

func NewHttpConnector(service, host string, cmds []string) *httpConnector {
	idx := strings.Index(host, ":")
	if idx == -1 {
		glog.Errorf("read host[%v] error[%v]", host, "no ':' inside")
		return nil
	}

	ip := host[:idx]
	port, err := strconv.Atoi(host[idx+1:])
	if err != nil {
		glog.Errorf("read host[%v] error[%v]", host, "port illegal")
		return nil
	}
	var addr = fmt.Sprintf("http://%s:%d", util.ConvertUnderline2Dot(ip), port)
	for i, it := range cmds {
		cmds[i] = fmt.Sprintf("%s/%s", addr, it)
	}

	connector := &httpConnector{
		service: service,
		address: addr,
		cmds:    cmds,
		client: &fasthttp.Client{
			ReadTimeout: httpTimeout * time.Second,
		},
		request:  fasthttp.AcquireRequest(),
		response: fasthttp.AcquireResponse(),
	}
	return connector
}

func (hc *httpConnector) Get() (interface{}, error) {
	var result [][]byte = make([][]byte, 0)
	for _, cmd := range hc.cmds {
		hc.request.SetRequestURI(cmd)
		err := hc.client.DoTimeout(hc.request, hc.response, httpTimeout*time.Second)
		if err != nil {
			return nil, fmt.Errorf("http get address[%v] failed[%v]", hc.address, err)
		}

		if code := hc.response.StatusCode(); code != fasthttp.StatusOK {
			return nil, fmt.Errorf("http get address[%v] ok but status code[%v] error", hc.address, code)
		}
		result = append(result, hc.response.Body())
	}

	return result, nil
}

func (hc *httpConnector) Close() {
	hc.client = nil
}
