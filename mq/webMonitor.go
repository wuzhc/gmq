package mq

import (
	"net/http"

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
	r.GET("/readyQueueList", readyQueueList)
	r.GET("/getReadyQueueStat", getReadyQueueStat)
	r.GET("/getBucketStat", getBucketStat)
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

// ready列表页
func readyQueueList(c *gin.Context) {
	c.HTML(http.StatusOK, "readyqueue_list.html", gin.H{
		"title": "readyQueue列表",
	})
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
		QueueName string `json:"queue_name"`
		JobNum    int    `json:"job_num"`
	}
	var res []queueInfo
	for _, r := range records {
		num, err := redis.Int(conn.Do("LLEN", r))
		if err != nil {
			num = 0
		}
		res = append(res, queueInfo{
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

	records, err := redis.Strings(conn.Do("KEYS", BUCKET_KEY+"*"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, rspErr(err))
	}

	type bucketInfo struct {
		BucketName string `json:"bucket_name"`
		JobNum     int    `json:"job_num"`
		NextTime   string `json:"next_time"`
	}
	var res []bucketInfo
	for _, r := range records {
		num, err := redis.Int(conn.Do("ZCARD", r))
		if err != nil {
			num = 0
		}
		res = append(res, bucketInfo{
			BucketName: r,
			JobNum:     num,
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
