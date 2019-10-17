package gnode

import (
	"encoding/json"
	"errors"
	// "time"
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

	t := h.ctx.Dispatcher.GetTopic(topic)
	msgId, msg, err := t.pop()
	if err != nil {
		c.JsonErr(err)
		return
	}

	// if topic.isAutoAck is false, add to waiting queue
	if !t.isAutoAck {
		if err := t.pushDelayMsg(msgId, msg, 60); err != nil {
			c.JsonErr(err)
			return
		}
	}

	c.JsonData(msg)
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

	if _, err := h.ctx.Dispatcher.push(job.Topic, job.Body, job.Delay); err != nil {
		c.JsonErr(err)
		return
	}

	c.JsonSuccess("push success")
}

// curl http://127.0.0.1:9504/ack?jobId=xxx&topic=xxx
// func (h *HttpApi) Ack(c *HttpServContext) {
// 	jobId := c.GetInt64("jobId")
// 	if jobId == 0 {
// 		c.JsonErr(errors.New("jobId is empty"))
// 		return
// 	}
// 	topic := c.Get("topic")
// 	if len(topic) == 0 {
// 		c.JsonErr(errors.New("topic is empty"))
// 		return
// 	}

// 	t := h.ctx.Dispatcher.GetTopic(topic)
// 	j := t.waitAckMQ.PopByJobId(jobId)
// 	if j == nil {
// 		c.JsonErr(errors.New("job is not exist"))
// 	} else {
// 		c.JsonSuccess("success")
// 	}
// }
