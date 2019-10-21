package gnode

import (
	"encoding/json"
	"errors"
	"net/http"
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
		if err := t.pushMsgToBucket(msgId, msg, 60); err != nil {
			c.JsonErr(err)
			return
		}
	}

	data := &Msg{
		Id:    msgId,
		Topic: topic,
		Body:  string(msg),
	}
	c.JsonData(data)
	return
}

// curl -d 'data={"body":"this is a job","topic":"game_1","delay":20}' 'http://127.0.0.1:9504/push'
// 推送消息
func (h *HttpApi) Push(c *HttpServContext) {
	data := c.Post("data")
	if len(data) == 0 {
		c.JsonErr(errors.New("data is empty"))
		return
	}

	msg := &Msg{}
	if err := json.Unmarshal([]byte(data), msg); err != nil {
		c.JsonErr(err)
		return
	}

	if _, err := h.ctx.Dispatcher.push(msg.Topic, []byte(msg.Body), msg.Delay); err != nil {
		c.JsonErr(err)
		return
	}

	c.JsonSuccess("push success")
}

// curl http://127.0.0.1:9504/ack?msgI=xxx&topic=xxx
func (h *HttpApi) Ack(c *HttpServContext) {
	msgId := c.GetInt64("msgId")
	if msgId == 0 {
		c.JsonErr(errors.New("msgId is empty"))
		return
	}
	topic := c.Get("topic")
	if len(topic) == 0 {
		c.JsonErr(errors.New("topic is empty"))
		return
	}

	if err := h.ctx.Dispatcher.ack(topic, uint64(msgId)); err != nil {
		c.JsonErr(err)
	} else {
		c.JsonSuccess("success")
	}
}

// 获取指定topic统计信息
// curl http://127.0.0.1/getTopicStat?topic=xxx
func (h *HttpApi) GetTopicStat(c *HttpServContext) {
	name := c.Get("topic")
	if len(name) == 0 {
		c.JsonErr(errors.New("topic is empty"))
		return
	}

	topic, err := h.ctx.Dispatcher.GetExistTopic(name)
	if err != nil {
		c.JsonErr(err)
		return
	}

	data := struct {
		Name      string `json:"name"`
		PopNum    int64  `json:"pop_num"`
		PushNum   int64  `json:"push_num"`
		BucketNum int    `json:"bucket_num"`
		StartTime string `json:"start_time"`
	}{topic.name, topic.popNum, topic.pushNum, topic.getBucketNum(), topic.startTime.Format("2006-01-02 15:04:05")}
	c.JsonData(data)
}

// 心跳接口
func (h *HttpApi) Ping(c *HttpServContext) {
	c.w.WriteHeader(http.StatusOK)
	c.w.Write([]byte{'O', 'K'})
}
