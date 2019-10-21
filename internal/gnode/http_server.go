package gnode

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/wuzhc/gmq/pkg/logs"
)

const (
	RESP_SUCCESS = iota // 响应成功
	RESP_FAILED         // 响应失败
)

type HttpServ struct {
	ctx *Context
}

func NewHttpServ(ctx *Context) *HttpServ {
	return &HttpServ{
		ctx: ctx,
	}
}

func (s *HttpServ) Run() {
	defer s.LogInfo("Http server exit.")

	api := &HttpApi{
		ctx: s.ctx,
	}

	mux := &HttpServMux{}
	mux.handle("/pop", api.Pop)
	mux.handle("/push", api.Push)
	mux.handle("/ack", api.Ack)
	mux.handle("/ping", api.Ping)
	mux.handle("/getTopicStat", api.GetTopicStat)

	addr := s.ctx.Conf.HttpServAddr
	serv := &http.Server{
		Addr:    addr,
		Handler: handlerMux(mux),
	}

	go func() {
		<-s.ctx.Gnode.exitChan
		serv.Shutdown(nil)
	}()

	var err error
	if s.ctx.Conf.HttpServEnableTls {
		certFile := s.ctx.Conf.HttpServCertFile
		keyFile := s.ctx.Conf.HttpServKeyFile
		s.LogInfo(fmt.Sprintf("Http server(%s) is running with tls", addr))
		err = serv.ListenAndServeTLS(certFile, keyFile)
	} else {
		s.LogInfo(fmt.Sprintf("Http server(%s) is running", addr))
		err = serv.ListenAndServe()
	}

	if err != nil {
		s.LogDebug(err)
	}
}

func handlerMux(mux *HttpServMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		c := &HttpServContext{
			w: w,
			r: r,
		}
		handler, exist := mux.m[path]
		if !exist {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("not found"))
			return
		}
		handler(c)
	})
}

type HttpHandlerFunc func(c *HttpServContext)
type HttpServMux struct {
	mu sync.RWMutex
	m  map[string]HttpHandlerFunc
}

func (mux *HttpServMux) handle(pattern string, f HttpHandlerFunc) {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	if pattern == "" {
		panic("http: invalid pattern")
	}
	if f == nil {
		panic("http: nil handler")
	}
	if _, exist := mux.m[pattern]; exist {
		panic("http: multiple registrations for " + pattern)
	}

	if mux.m == nil {
		mux.m = make(map[string]HttpHandlerFunc)
	}
	mux.m[pattern] = f
}

type HttpServContext struct {
	w http.ResponseWriter
	r *http.Request
}

func (c *HttpServContext) JsonData(data interface{}) {
	r := map[string]interface{}{
		"data": data,
		"msg":  "success",
		"code": 0,
	}

	outputJson(c.w, r)
}

func (c *HttpServContext) JsonMsg(code int, msg string) {
	r := map[string]interface{}{
		"data": nil,
		"msg":  msg,
		"code": code,
	}

	outputJson(c.w, r)
}

func (c *HttpServContext) JsonSuccess(msg string) {
	r := map[string]interface{}{
		"data": nil,
		"msg":  msg,
		"code": RESP_SUCCESS,
	}

	outputJson(c.w, r)
}

func (c *HttpServContext) JsonErr(err error) {
	r := map[string]interface{}{
		"data": nil,
		"msg":  err.Error(),
		"code": RESP_FAILED,
	}

	outputJson(c.w, r)
}

func outputJson(w http.ResponseWriter, data map[string]interface{}) {
	v, err := json.Marshal(data)
	if err != nil {
		w.Write([]byte(err.Error()))
	} else {
		w.Header().Set("Content-type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(v)
	}
}

func (c *HttpServContext) Get(key string) string {
	return c.r.URL.Query().Get(key)
}

func (c *HttpServContext) GetDefault(key string, def string) string {
	v := c.r.URL.Query().Get(key)
	if len(v) == 0 {
		return def
	}
	return v
}

func (c *HttpServContext) GetInt(key string) int {
	v := c.r.URL.Query().Get(key)
	iv, _ := strconv.Atoi(v) // ignore error
	return iv
}

func (c *HttpServContext) GetInt64(key string) int64 {
	v := c.r.URL.Query().Get(key)
	iv, _ := strconv.ParseInt(v, 10, 64)
	return iv
}

func (c *HttpServContext) GetDefaultInt(key string, def int) int {
	v := c.r.URL.Query().Get(key)
	iv, err := strconv.Atoi(v)
	if err != nil || iv == 0 {
		return def
	}
	return iv
}

func (c *HttpServContext) Post(key string) string {
	return c.r.PostFormValue(key)
}

func (s *HttpServ) LogError(msg interface{}) {
	s.ctx.Logger.Error(logs.LogCategory("HttpServer"), msg)
}

func (s *HttpServ) LogWarn(msg interface{}) {
	s.ctx.Logger.Warn(logs.LogCategory("HttpServer"), msg)
}

func (s *HttpServ) LogInfo(msg interface{}) {
	s.ctx.Logger.Info(logs.LogCategory("HttpServer"), msg)
}

func (s *HttpServ) LogDebug(msg interface{}) {
	s.ctx.Logger.Debug(logs.LogCategory("HttpServer"), msg)
}
