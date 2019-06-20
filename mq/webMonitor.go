package mq

import (
	"go-mq/utils"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/gomodule/redigo/redis"
)

func init() {
	go RunWebMonitor()
}

func RunWebMonitor() {
	r := gin.Default()
	r.StaticFS("/static", http.Dir("static"))
	r.LoadHTMLGlob("views/*")
	r.GET("/", index)
	r.GET("/login", login)
	r.GET("/home", home)
	r.GET("/bucketList", bucketList)
	r.GET("/bucketJobList", bucketJobList)
	r.GET("/readyQueueList", readyQueueList)
	r.GET("/getReadyQueueStat", getReadyQueueStat)
	r.GET("/getBucketStat", getBucketStat)
	r.GET("/getJobsByBucketKey", getJobsByBucketKey)
	r.GET("/jobDetail", jobDetail)
	r.Run(":8000")
}

// 首页
func index(c *gin.Context) {
	c.HTML(http.StatusOK, "entry.html", gin.H{
		"siteName":      "web监控管理",
		"version":       "v1.0",
		"loginUserName": "wuzhc",
	})
}

// 主页
func home(c *gin.Context) {
	c.HTML(http.StatusOK, "home.html", gin.H{
		"title": "主页",
	})
}

// 登录
func login(c *gin.Context) {
	c.HTML(http.StatusOK, "login.html", gin.H{
		"title": "登录页面",
	})
}

// bucket列表页
func bucketList(c *gin.Context) {
	c.HTML(http.StatusOK, "bucket_list.html", gin.H{
		"title": "bucket列表",
	})
}

// bucket中job列表
func bucketJobList(c *gin.Context) {
	bucketKey := c.Query("bucketKey")
	if len(bucketKey) == 0 {
		c.String(http.StatusBadRequest, "bucketKey参数错误")
		return
	}
	c.HTML(http.StatusOK, "bucket_job_list.html", gin.H{
		"title":     "bucket jobs列表",
		"bucketKey": bucketKey,
	})
}

// ready queue列表页
func readyQueueList(c *gin.Context) {
	c.HTML(http.StatusOK, "readyqueue_list.html", gin.H{
		"title": "readyQueue列表",
	})
}

// 任务job详情
func jobDetail(c *gin.Context) {
	jobId := c.Query("jobId")
	if len(jobId) == 0 {
		c.String(http.StatusBadGateway, "jobId参数错误")
		return
	}
	detail, err := GetJobDetailById(jobId)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	detail["delay"] = utils.SecToTimeString(detail["delay"])
	detail["job_key"] = GetJobKeyById(detail["id"])
	c.HTML(http.StatusOK, "job_detail.html", gin.H{
		"title":  "job详情",
		"detail": detail,
	})
}

// 根据jobId获取job详情
func getJobDetailById(c *gin.Context) {
	jobId := c.Query("jobId")
	if len(jobId) == 0 {
		c.JSON(http.StatusBadRequest, rspErr("jobId参数错误"))
		return
	}
	detail, err := GetJobDetailById(jobId)
	if err != nil {
		c.JSON(http.StatusInternalServerError, rspErr(err))
		return
	}
	c.JSON(http.StatusOK, detail)
}

// 根据bucketKey获取bucket任务列表
func getJobsByBucketKey(c *gin.Context) {
	n := c.DefaultQuery("limit", "20")
	k := c.Query("bucketKey")
	if len(k) == 0 {
		c.JSON(http.StatusBadRequest, rspErr("bucketKey不能为空"))
		return
	}

	conn := redisPool.Get()
	defer conn.Close()

	type jobInfo struct {
		Id        string `json:"id"`
		JobKey    string `json:"job_key"`
		RunTime   string `json:"runtime"`
		TTR       string `json:"ttr"`
		DelayTime string `json:"delay_time"`
		Topic     string `json:"topic"`
		Status    string `json:"status"`
	}
	var res []jobInfo
	records, err := redis.Strings(conn.Do("ZRANGE", k, 0, n, "WITHSCORES"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, rspErr(err))
		return
	}

	var name, time []string
	for i, v := range records {
		if i%2 == 0 {
			name = append(name, v)
		} else {
			time = append(time, v)
		}
	}

	for j, id := range name {
		detail, _ := GetJobDetailById(id)
		res = append(res, jobInfo{
			Id:        id,
			TTR:       detail["TTR"],
			DelayTime: utils.SecToTimeString(detail["delay"]),
			Topic:     detail["topic"],
			Status:    getStatusName(detail["status"]),
			JobKey:    GetJobKeyById(id),
			RunTime:   utils.UnixToFormatTime(time[j]),
		})
	}

	c.JSON(http.StatusOK, rspData(res))
}

func getStatusName(status string) string {
	s, err := strconv.Atoi(status)
	if err != nil {
		return `<span class="layui-badge layui-bg-black">unknown</span>`
	}
	if s == JOB_STATUS_DELAY {
		return `<span class="layui-badge layui-bg-orange">delay</span>`
	}
	if s == JOB_STATUS_DELETE {
		return `<span class="layui-badge layui-bg-green">delete</span>`
	}
	if s == JOB_STATUS_READY {
		return `<span class="layui-badge layui-bg-cyan">ready</span>`
	}
	if s == JOB_STATUS_RESERVED {
		return `<span class="layui-badge layui-bg-blue">reserved</span>`
	}
	return `<span class="layui-badge layui-bg-black">unknown</span>`
}

// 获取readyQueue统计信息
func getReadyQueueStat(c *gin.Context) {
	conn := redisPool.Get()
	defer conn.Close()

	records, err := redis.Strings(conn.Do("KEYS", READY_QUEUE_KEY+"*"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, rspErr(err))
	}

	type queueInfo struct {
		Id        int    `json:"id"`
		QueueName string `json:"queue_name"`
		JobNum    int    `json:"job_num"`
	}
	var res []queueInfo
	for k, r := range records {
		num, err := redis.Int(conn.Do("LLEN", r))
		if err != nil {
			num = 0
		}
		res = append(res, queueInfo{
			Id:        k + 1,
			QueueName: r,
			JobNum:    num,
		})
	}

	c.JSON(http.StatusOK, rspData(res))
}

// 获取bucket统计信息
func getBucketStat(c *gin.Context) {
	conn := redisPool.Get()
	defer conn.Close()

	type bucketInfo struct {
		Id         int    `json:"id"`
		BucketName string `json:"bucket_name"`
		JobNum     int    `json:"job_num"`
		NextTime   string `json:"next_time"`
	}
	var res []bucketInfo
	for k, b := range dispatcher.bucket {
		res = append(res, bucketInfo{
			Id:         k + 1,
			BucketName: GetBucketKeyById(b.Id),
			JobNum:     b.JobNum,
			NextTime:   utils.FormatTime(b.NextTime),
		})
	}

	c.JSON(http.StatusOK, rspData(res))
}

func rspErr(msg interface{}) gin.H {
	var resp = make(gin.H)
	resp["code"] = 1
	resp["msg"] = msg
	resp["data"] = nil
	return resp
}

func rspData(data interface{}) gin.H {
	var resp = make(gin.H)
	resp["code"] = 0
	resp["msg"] = ""
	resp["data"] = data
	return resp
}

func rspSuccess(msg interface{}) gin.H {
	var resp = make(gin.H)
	resp["code"] = 0
	resp["msg"] = msg
	resp["data"] = nil
	return resp
}
