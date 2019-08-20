package gnode

import (
	"encoding/json"
	"errors"
	"strconv"
)

type HttpApi struct {
	ctx *Context
}

// curl http://127.0.0.1:9504/pop?topic=xxx
// 消费任务
func (h *HttpApi) Pop(c *HttpServContext) {
	topic := c.Get("topic")
	if len(topic) == 0 {
		c.JsonErr(errors.New("topic is empty"))
		return
	}

	queue := GetJobQueueByTopic(topic)
	records, err := Redis.Strings("BRPOP", queue, h.ctx.Conf.RedisPopInterVal)
	if err != nil {
		c.JsonErr(err)
		return
	}

	jobId := records[1]
	if err := SetJobStatus(jobId, JOB_STATUS_RESERVED); err != nil {
		c.JsonErr(err)
		return
	}
	detail, err := GetJobDetailById(jobId)
	if err != nil {
		c.JsonErr(err)
		return
	}

	// TTR表示job执行超时时间(即消费者读取到job到确认删除这段时间)
	// TTR>0时,若执行时间超过TTR,将重新添加到ready_queue,然后再次被消费
	// TTR<=0时,消费者读取到job时,即会删除任务池中的job单元
	TTR, err := strconv.Atoi(detail["TTR"])
	if err != nil {
		c.JsonErr(err)
		return
	}
	if TTR > 0 {
		IncrJobConsumeNum(jobId)
		h.ctx.Dispatcher.addToTTRBucket <- &JobCard{
			id:    detail["id"],
			delay: TTR + 3,
			topic: detail["topic"],
		}
	} else {
		Ack(detail["id"])
	}

	c.JsonData(detail)
	return
}

// curl -d 'data={"id":"xxx_1","body":"this is a job","topic":"game_1","TTR":10,"delay":20}' 'http://127.0.0.1:9504/push'
// 生产任务
func (h *HttpApi) Push(c *HttpServContext) {
	data := c.Post("data")
	if len(data) == 0 {
		c.JsonErr(errors.New("data is empty"))
		return
	}

	job := &Job{}
	if err := json.Unmarshal([]byte(data), job); err != nil {
		c.JsonErr(err)
		return
	}

	if err := h.ctx.Dispatcher.AddToJobPool(job); err != nil {
		c.JsonErr(err)
		return
	}

	c.JsonSuccess("push success")
}

// curl http://127.0.0.1:9504/ack?jobId=xxx
// 确认删除已消费任务
func (h *HttpApi) Ack(c *HttpServContext) {
	jobId := c.Get("jobId")
	if len(jobId) == 0 {
		c.JsonErr(errors.New("jobId is empty"))
		return
	}

	res, err := Ack(jobId)
	if err != nil {
		c.JsonErr(err)
		return
	}

	if res {
		c.JsonSuccess("ack success")
	} else {
		c.JsonErr(errors.New("ack failed"))
	}
}
