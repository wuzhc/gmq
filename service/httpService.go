package service

import (
	"encoding/json"
	"fmt"
	"go-mq/mq"
	"go-mq/utils"
	"html/template"
	"net/http"
	"sort"
	"strconv"
)

type httpService struct {
	dispatcher *mq.Dispatcher
}

type Resp struct {
	Data string `json:"data"`
	Msg  string `json:"msg"`
	Code int    `json:"code"`
}

func RespData(data interface{}, w http.ResponseWriter) {
	var resp = make(map[string]interface{})
	resp["data"] = data
	resp["code"] = 0
	resp["msg"] = "success"
	nbyte, err := json.Marshal(resp)
	if err != nil {
		fmt.Fprintln(w, err)
	} else {
		fmt.Fprintln(w, string(nbyte))
	}
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
		t = n * 4

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

	for n := 0; n <= 10000; n++ {
		v = strconv.Itoa(n) + k + "kouzi"
		t = n * 2

		job := &mq.Job{
			Id:    v,
			Topic: "kouzi",
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

	for n := 0; n <= 100000; n++ {
		v = strconv.Itoa(n) + k + "ceping"
		t = n * 2

		job := &mq.Job{
			Id:    v,
			Topic: "ceping",
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

	for n := 0; n <= 100; n++ {
		v = strconv.Itoa(n) + k + "kepu"
		t = n * 2

		job := &mq.Job{
			Id:    v,
			Topic: "kepu",
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

	for n := 0; n <= 300000; n++ {
		v = strconv.Itoa(n) + k + "renrentong"
		t = n

		job := &mq.Job{
			Id:    v,
			Topic: "renrentong",
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

func (h *httpService) GetBucketInfo(w http.ResponseWriter, r *http.Request) {
	type bc struct {
		Id       string `json:"id"`
		JobNum   int    `json:"job_num"`
		NextTime string `json:"next_time"`
	}

	var res []bc
	buckets := h.dispatcher.GetBuckets()
	sort.Sort(mq.ById(buckets))
	for _, b := range buckets {
		v := bc{
			Id:       mq.GetBucketKeyById(b.Id),
			JobNum:   b.JobNum,
			NextTime: utils.FormatTime(b.NextTime),
		}
		res = append(res, v)
	}

	resp, err := json.Marshal(res)
	if err != nil {
		fmt.Fprintln(w, err)
	} else {
		fmt.Fprintf(w, string(resp))
	}
}

func (h *httpService) GetQueueInfo(w http.ResponseWriter, r *http.Request) {
	res, _ := mq.GetReadyQueueStat()
	_, err := json.Marshal(res)
	if err != nil {
		fmt.Fprintln(w, err)
	} else {
		RespData(res, w)
	}
}

func (h *httpService) Run() {
	fmt.Println("begin http servie")
	http.Handle("/static/", http.FileServer(http.Dir("static"))) // 启动静态文件服务

	http.HandleFunc("/", h.Index)
	http.HandleFunc("/add", h.AddHandler)
	http.HandleFunc("/show", h.ShowHandler)
	http.HandleFunc("/getBucketInfo", h.GetBucketInfo)
	http.HandleFunc("/getQueueInfo", h.GetQueueInfo)
	http.ListenAndServe("127.0.0.1:8000", nil)
}
