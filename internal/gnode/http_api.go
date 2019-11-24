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
	Name       string `json:"name"`
	PopNum     int64  `json:"pop_num"`
	PushNum    int64  `json:"push_num"`
	QueueNum   int64  `json:"queue_num"`
	DelayNum   int    `json:"delay_num"`
	WaitAckNum int    `json:"wait_ack_num"`
	DeadNum    int    `json:"dead_num"`
	StartTime  string `json:"start_time"`
	IsAutoAck  bool   `json:"is_auto_ack"`
}

// curl http://127.0.0.1:9504/pop?topic=xxx
// 消费任务
func (h *HttpApi) Pop(c *HttpServContext) {
	topic := c.Get("topic")
	if len(topic) == 0 {
		c.JsonErr(errors.New("topic is empty"))
		return
	}
	bindKey := c.Get("bindKey")
	if len(bindKey) == 0 {
		c.JsonErr(errors.New("bindKey is empty"))
		return
	}

	msg, err := h.ctx.Dispatcher.pop(topic, bindKey)
	if err != nil {
		c.JsonErr(err)
		msg = nil
		return
	}

	data := RespMsgData{
		Id:    strconv.FormatUint(msg.Id, 10),
		Body:  string(msg.Body),
		Retry: msg.Retry,
	}

	c.JsonData(data)
	msg = nil
	return
}

// curl http://127.0.0.1:9504/declareQueue?topic=xxx&bindKey=kkk
// 声明队列
func (h *HttpApi) DeclareQueue(c *HttpServContext) {
	topic := c.Get("topic")
	if len(topic) == 0 {
		c.JsonErr(errors.New("topic is empty"))
		return
	}
	bindKey := c.Get("bindKey")
	if len(bindKey) == 0 {
		c.JsonErr(errors.New("bindKey is empty"))
		return
	}

	if err := h.ctx.Dispatcher.declareQueue(topic, bindKey); err != nil {
		c.JsonErr(err)
		return
	}

	c.JsonSuccess("ok")
}

// curl -d 'data={"body":"this is a job","topic":"xxx","delay":20,"route_key":"xxx"}' 'http://127.0.0.1:9504/push'
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

	msgId, err := h.ctx.Dispatcher.push(msg.Topic, msg.RouteKey, []byte(msg.Body), msg.Delay)
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

// curl http://127.0.0.1:9504/config?topic=xxx&isAuthoAck=1&mode=1&msgTTR=30&msgRetry=5
// 配置topic
func (h *HttpApi) Config(c *HttpServContext) {
	topic := c.Get("topic")
	if len(topic) == 0 {
		c.JsonErr(errors.New("topic is empty"))
		return
	}

	configure := &topicConfigure{
		isAutoAck: c.GetInt("isAutoAck"),
		mode:      c.GetInt("mode"),
		msgTTR:    c.GetInt("msgTTR"),
		msgRetry:  c.GetInt("msgRetry"),
	}

	err := h.ctx.Dispatcher.set(topic, configure)
	configure = nil
	if err != nil {
		c.JsonErr(err)
	} else {
		c.JsonSuccess("success")
	}
}

// 获取指定topic统计信息
// curl http://127.0.0.1:9504/getTopicStat?topic=xxx
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
	data.QueueNum = t.queue.num
	data.DelayNum = t.getBucketNum()
	data.DeadNum = t.getDeadNum()
	data.WaitAckNum = len(t.waitAckMap)
	data.IsAutoAck = t.isAutoAck
	data.StartTime = t.startTime.Format("2006-01-02 15:04:05")
	c.JsonData(data)
}

// 获取所有topic统计信息
// curl http://127.0.0.1:9504/getAllTopicStat
// http://127.0.0.1:9504/getAllTopicStat
func (h *HttpApi) GetAllTopicStat(c *HttpServContext) {
	topics := h.ctx.Dispatcher.GetTopics()

	var topicDatas = make([]topicData, len(topics))
	for i, t := range topics {
		data := topicData{}
		data.Name = t.name
		data.PopNum = t.popNum
		data.PushNum = t.pushNum
		data.QueueNum = t.queue.num
		data.WaitAckNum = len(t.waitAckMap)
		data.DelayNum = t.getBucketNum()
		data.DeadNum = t.getDeadNum()
		data.IsAutoAck = t.isAutoAck
		data.StartTime = t.startTime.Format("2006-01-02 15:04:05")
		topicDatas[i] = data
	}

	c.JsonData(topicDatas)
}

// 退出topic
// curl http://127.0.0.1:9504/exitTopic?topic=xxx
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
// curl http://127.0.0.1:9504/setIsAutoAck?topic=xxx
// http://127.0.0.1:9504/setIsAutoAck?topic=xxx
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

	configure := &topicConfigure{
		isAutoAck: vv,
	}

	if err := topic.set(configure); err != nil {
		configure = nil
		c.JsonErr(err)
		return
	}

	configure = nil
	c.JsonSuccess("success")
}

func (h *HttpApi) GetQueuesByTopic(c *HttpServContext) {
	name := c.Get("topic")
	if len(name) == 0 {
		c.JsonErr(errors.New("topic is empty"))
		return
	}

	data := make(map[string]string)
	topic := h.ctx.Dispatcher.GetTopic(name)
	for k, v := range topic.queues {
		data[k] = v.name
	}

	c.JsonData(data)
}

// 心跳接口
func (h *HttpApi) Ping(c *HttpServContext) {
	c.w.WriteHeader(http.StatusOK)
	c.w.Write([]byte{'O', 'K'})
}
