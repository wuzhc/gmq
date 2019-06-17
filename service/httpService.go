package service

import (
	"fmt"
	"go-mq/mq"
	"html/template"
	"net/http"
	"strconv"
)

type httpService struct {
	dispatcher *mq.Dispatcher
}

func (h *httpService) Index(w http.ResponseWriter, r *http.Request) {
	t, _ := template.ParseFiles("views/index.html")
	fmt.Println(t.Name())
	t.Execute(w, "Hello world")
}

func (h *httpService) AddHandler(w http.ResponseWriter, r *http.Request) {
	k := r.URL.Query().Get("k")

	var v string
	var t int
	for n := 0; n <= 1000; n++ {
		v = strconv.Itoa(n) + k + "weike"
		t = n * 3

		job := &mq.Job{
			Id:    v,
			Topic: "weike",
			Delay: t,
			TTR:   30,
			Body:  "this is a test by wuzhc",
		}
		if err := h.dispatcher.AddToJobPool(job); err != nil {
			fmt.Fprintln(w, err)
		} else {
			fmt.Fprintln(w, "add success")
		}
	}

	for n := 0; n <= 1000; n++ {
		v = strconv.Itoa(n) + k + "xuetang"
		t = n * 3

		job := &mq.Job{
			Id:    v,
			Topic: "xuetang",
			Delay: t,
			TTR:   30,
			Body:  "this is a test by wuzhc",
		}
		if err := h.dispatcher.AddToJobPool(job); err != nil {
			fmt.Fprintln(w, err)
		} else {
			fmt.Fprintln(w, "add success")
		}
	}

	for n := 0; n <= 1000; n++ {
		v = strconv.Itoa(n) + k + "dasai"
		t = 0

		job := &mq.Job{
			Id:    v,
			Topic: "dasai",
			Delay: t,
			TTR:   30,
			Body:  "this is a test by wuzhc",
		}
		if err := h.dispatcher.AddToJobPool(job); err != nil {
			fmt.Fprintln(w, err)
		} else {
			fmt.Fprintln(w, "add success")
		}
	}

}

func (h *httpService) ShowHandler(w http.ResponseWriter, r *http.Request) {
	res := h.dispatcher.GetBuckets()
	fmt.Fprintln(w, res)
}

func (h *httpService) Run() {
	fmt.Println("begin http servie")
	http.Handle("/static/", http.FileServer(http.Dir("static"))) // 启动静态文件服务

	http.HandleFunc("/", h.Index)
	http.HandleFunc("/add", h.AddHandler)
	http.HandleFunc("/show", h.ShowHandler)
	http.ListenAndServe("127.0.0.1:8000", nil)
}
