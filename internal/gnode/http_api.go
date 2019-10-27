package gnode

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
)

type HttpApi struct {
	ctx *Context
}

type topicData struct {
	Name      string `json:"name"`
	PopNum    int64  `json:"pop_num"`
	PushNum   int64  `json:"push_num"`
	BucketNum int    `json:"bucket_num"`
	DeadNum   int    `json:"dead_num"`
	StartTime string `json:"start_time"`
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
	msg, fid, offset, err := t.pop()
	defer func() {
		msg = nil
	}()
	if err != nil {
		c.JsonErr(err)
		return
	}

	// if topic.isAutoAck is false, add to waiting queue
	if !t.isAutoAck {
		t.waitAckMux.Lock()
		t.waitAckMap[msg.Id] = []int{fid, offset}
		t.waitAckMux.Unlock()
	}

	data := RespMsgData{
		Id:    strconv.FormatUint(msg.Id, 10),
		Body:  string(msg.Body),
		Retry: msg.Retry,
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

	msg := RecvMsgData{}
	if err := json.Unmarshal([]byte(data), &msg); err != nil {
		c.JsonErr(err)
		return
	}

	msgId, err := h.ctx.Dispatcher.push(msg.Topic, []byte(msg.Body), msg.Delay)
	if err != nil {
		c.JsonErr(err)
		return
	}

	var rsp = make(map[string]string)
	rsp["msgId"] = strconv.FormatUint(msgId, 10)
	c.JsonData(rsp)
}

// curl http://127.0.0.1:9504/ack?msgId=xxx&topic=xxx
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

// curl http://127.0.0.1:9504/pop?topic=xxx
// 消费任务
func (h *HttpApi) Set(c *HttpServContext) {
	isAutoAck := c.GetInt("isAutoAck")
	topic := c.Get("topic")
	if len(topic) == 0 {
		c.JsonErr(errors.New("topic is empty"))
		return
	}

	t := h.ctx.Dispatcher.GetTopic(topic)
	if err := t.set(isAutoAck); err != nil {
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

	t, err := h.ctx.Dispatcher.GetExistTopic(name)
	if err != nil {
		c.JsonErr(err)
		return
	}

	data := topicData{}
	data.Name = t.name
	data.PopNum = t.popNum
	data.PushNum = t.pushNum
	data.BucketNum = t.getBucketNum()
	data.DeadNum = t.getDeadNum()
	data.StartTime = t.startTime.Format("2006-01-02 15:04:05")
	c.JsonData(data)
}

// 获取所有topic统计信息
// curl http://127.0.0.1/getAllTopicStat
// http://127.0.0.1:9504/getAllTopicStat
func (h *HttpApi) GetAllTopicStat(c *HttpServContext) {
	topics := h.ctx.Dispatcher.GetTopics()

	var topicDatas = make([]topicData, len(topics))
	for i, t := range topics {
		data := topicData{}
		data.Name = t.name
		data.PopNum = t.popNum
		data.PushNum = t.pushNum
		data.BucketNum = t.getBucketNum()
		data.DeadNum = t.getDeadNum()
		data.StartTime = t.startTime.Format("2006-01-02 15:04:05")
		topicDatas[i] = data
	}

	c.JsonData(topicDatas)
}

// 退出topic
// curl http://127.0.0.1/exitTopic?topic=xxx
// http://127.0.0.1:9504/exitTopic?topic=xxx
func (h *HttpApi) ExitTopic(c *HttpServContext) {
	name := c.Get("topic")
	if len(name) == 0 {
		c.JsonErr(errors.New("topic is empty"))
		return
	}

	// topic不存在或没有被客户端请求
	topic, err := h.ctx.Dispatcher.GetExistTopic(name)
	if err != nil {
		c.JsonErr(err)
		return
	}

	topic.exit()
	topic = nil
	delete(h.ctx.Dispatcher.topics, name)
	c.JsonSuccess(fmt.Sprintf("topic.%s has exit.", name))
}

// 设置主题自动确认消息
func (h *HttpApi) SetIsAutoAck(c *HttpServContext) {
	name := c.Get("topic")
	if len(name) == 0 {
		c.JsonErr(errors.New("topic is empty"))
		return
	}

	// topic不存在或没有被客户端请求
	topic, err := h.ctx.Dispatcher.GetExistTopic(name)
	if err != nil {
		c.JsonErr(err)
		return
	}

	var vv int
	v := topic.isAutoAck
	if v {
		vv = 0
	} else {
		vv = 1
	}
	if err := topic.set(vv); err != nil {
		c.JsonErr(err)
		return
	}

	c.JsonSuccess("success")
}

// 心跳接口
func (h *HttpApi) Ping(c *HttpServContext) {
	c.w.WriteHeader(http.StatusOK)
	c.w.Write([]byte{'O', 'K'})
}
