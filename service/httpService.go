package service

import (
	// 	"encoding/json"
	// 	"fmt"
	"go-mq/mq"
	// 	"go-mq/utils"
	// 	"html/template"
	// 	"net/http"
	// 	"sort"
	// 	"strconv"
)

// var (
// 	ErrJosnMarshal = `{"code":1,"msg":"json marshal failed","data":null}`
// )

type httpService struct {
	dispatcher *mq.Dispatcher
}

func (h *httpService) Run() {}

// func RespData(data interface{}, w http.ResponseWriter) {
// 	var resp = make(map[string]interface{})
// 	resp["data"] = data
// 	resp["code"] = 0
// 	resp["msg"] = "success"
// 	nbyte, err := json.Marshal(resp)

// 	if err != nil {
// 		fmt.Fprintln(w, ErrJosnMarshal)
// 	} else {
// 		fmt.Fprintln(w, string(nbyte))
// 	}
// }

// func RespErr(msg string, w http.ResponseWriter) {
// 	var resp = make(map[string]interface{})
// 	resp["data"] = nil
// 	resp["code"] = 1
// 	resp["msg"] = msg
// 	nbyte, err := json.Marshal(resp)

// 	if err == nil {
// 		fmt.Fprintln(w, ErrJosnMarshal)
// 	} else {
// 		fmt.Fprintln(w, string(nbyte))
// 	}
// }

// func RespSuccess(msg string, w http.ResponseWriter) {
// 	var resp = make(map[string]interface{})
// 	resp["data"] = nil
// 	resp["code"] = 0
// 	resp["msg"] = msg
// 	nbyte, err := json.Marshal(resp)

// 	if err == nil {
// 		fmt.Fprintln(w, ErrJosnMarshal)
// 	} else {
// 		fmt.Fprintln(w, string(nbyte))
// 	}
// }

// func (h *httpService) Index(w http.ResponseWriter, r *http.Request) {
// 	t, _ := template.ParseFiles("views/index.html")
// 	fmt.Println(t.Name())
// 	t.Execute(w, "Hello world")
// }

// func (h *httpService) AddHandler(w http.ResponseWriter, r *http.Request) {
// 	k := r.URL.Query().Get("k")

// 	var v string
// 	var t int
// 	for n := 0; n <= 1000; n++ {
// 		v = strconv.Itoa(n) + k + "weike"
// 		t = n * 3

// 		job := &mq.Job{
// 			Id:    v,
// 			Topic: "weike",
// 			Delay: t,
// 			TTR:   30,
// 			Body:  "this is a test by wuzhc",
// 		}
// 		if err := h.dispatcher.AddToJobPool(job); err != nil {
// 			fmt.Fprintln(w, err)
// 		} else {
// 			fmt.Fprintln(w, "add success")
// 		}
// 	}

// 	for n := 0; n <= 1000; n++ {
// 		v = strconv.Itoa(n) + k + "xuetang"
// 		t = n * 3

// 		job := &mq.Job{
// 			Id:    v,
// 			Topic: "xuetang",
// 			Delay: t,
// 			TTR:   30,
// 			Body:  "this is a test by wuzhc",
// 		}
// 		if err := h.dispatcher.AddToJobPool(job); err != nil {
// 			fmt.Fprintln(w, err)
// 		} else {
// 			fmt.Fprintln(w, "add success")
// 		}
// 	}

// 	for n := 0; n <= 1000; n++ {
// 		v = strconv.Itoa(n) + k + "dasai"
// 		t = n * 4

// 		job := &mq.Job{
// 			Id:    v,
// 			Topic: "dasai",
// 			Delay: t,
// 			TTR:   30,
// 			Body:  "this is a test by wuzhc",
// 		}
// 		if err := h.dispatcher.AddToJobPool(job); err != nil {
// 			fmt.Fprintln(w, err)
// 		} else {
// 			fmt.Fprintln(w, "add success")
// 		}
// 	}

// 	for n := 0; n <= 10000; n++ {
// 		v = strconv.Itoa(n) + k + "kouzi"
// 		t = n * 2

// 		job := &mq.Job{
// 			Id:    v,
// 			Topic: "kouzi",
// 			Delay: t,
// 			TTR:   30,
// 			Body:  "this is a test by wuzhc",
// 		}
// 		if err := h.dispatcher.AddToJobPool(job); err != nil {
// 			fmt.Fprintln(w, err)
// 		} else {
// 			fmt.Fprintln(w, "add success")
// 		}
// 	}

// 	for n := 0; n <= 100000; n++ {
// 		v = strconv.Itoa(n) + k + "ceping"
// 		t = n * 2

// 		job := &mq.Job{
// 			Id:    v,
// 			Topic: "ceping",
// 			Delay: t,
// 			TTR:   30,
// 			Body:  "this is a test by wuzhc",
// 		}
// 		if err := h.dispatcher.AddToJobPool(job); err != nil {
// 			fmt.Fprintln(w, err)
// 		} else {
// 			fmt.Fprintln(w, "add success")
// 		}
// 	}

// 	for n := 0; n <= 100; n++ {
// 		v = strconv.Itoa(n) + k + "kepu"
// 		t = n * 2

// 		job := &mq.Job{
// 			Id:    v,
// 			Topic: "kepu",
// 			Delay: t,
// 			TTR:   30,
// 			Body:  "this is a test by wuzhc",
// 		}
// 		if err := h.dispatcher.AddToJobPool(job); err != nil {
// 			fmt.Fprintln(w, err)
// 		} else {
// 			fmt.Fprintln(w, "add success")
// 		}
// 	}

// 	for n := 0; n <= 300000; n++ {
// 		v = strconv.Itoa(n) + k + "renrentong"
// 		t = n

// 		job := &mq.Job{
// 			Id:    v,
// 			Topic: "renrentong",
// 			Delay: t,
// 			TTR:   30,
// 			Body:  "this is a test by wuzhc",
// 		}
// 		if err := h.dispatcher.AddToJobPool(job); err != nil {
// 			fmt.Fprintln(w, err)
// 		} else {
// 			fmt.Fprintln(w, "add success")
// 		}
// 	}

// }

// // 获取bucket统计信息
// func (h *httpService) GetBucketStat(w http.ResponseWriter, r *http.Request) {
// 	type respBucket struct {
// 		BucketName string `json:"bucket_name"`
// 		JobNum     int    `json:"job_num"`
// 		NextTime   string `json:"next_time"`
// 	}

// 	var res []respBucket
// 	buckets := h.dispatcher.GetBuckets()
// 	sort.Sort(mq.ById(buckets))
// 	for _, b := range buckets {
// 		v := respBucket{
// 			BucketName: mq.GetBucketKeyById(b.Id),
// 			JobNum:     b.JobNum,
// 			NextTime:   utils.FormatTime(b.NextTime),
// 		}
// 		res = append(res, v)
// 	}

// 	RespData(res, w)
// }

// // 获取readyQueue统计信息
// func (h *httpService) GetReadyQueueStat(w http.ResponseWriter, r *http.Request) {
// 	res, err := mq.GetReadyQueueStat()
// 	if err != nil {
// 		RespErr(err.Error(), w)
// 	} else {
// 		RespData(res, w)
// 	}
// }

// func (h *httpService) Run() {
// 	fmt.Println("begin http servie")
// 	http.Handle("/static/", http.FileServer(http.Dir("static"))) // 启动静态文件服务

// 	http.HandleFunc("/", h.Index)
// 	http.HandleFunc("/add", h.AddHandler)
// 	http.HandleFunc("/getBucketStat", h.GetBucketStat)
// 	http.HandleFunc("/getReadyQueueStat", h.GetReadyQueueStat)
// 	http.ListenAndServe("127.0.0.1:8000", nil)
// }
