// ./gctl -node_addr="127.0.0.1:9503" -cmd="delcare" --topic="test-topic" --queue="test-queue-1"
// ./gctl -node_addr="127.0.0.1:9503" -cmd="push" -topic="test-topic" --route_key="test-queue-1" --delay=0
// ./gctl -node_addr="127.0.0.1:9503" -cmd="mpush" -topic="ketang" -route_key="homework" -push_num=1000
// ./gctl -node_addr="127.0.0.1:9503" -cmd="pop" -topic="ketang" -bind_key="homework"
// ./gctl -node_addr="127.0.0.1:9503" -cmd="pop_loop" -topic="ketang" -bind_key="homework"
// ./gctl -node_addr="127.0.0.1:9503" -cmd="ack" -topic="ketang" -msg_id="374389276810416130" -bind_key="homework"
// ./gctl -node_addr="127.0.0.1:9503" -cmd="dead" -topic="ketang" -bind_key="homework"
// ./gctl -node_addr="127.0.0.1:9503" -cmd="subscribe" -channel="ketang"
// ./gctl -node_addr="127.0.0.1:9503" -cmd="publish" -channel="ketang" -message="xxx"
package main

import (
	"flag"
	"github.com/wuzhc/gmq/internal/gctl"
	"log"
	//"strconv"
	"time"
)

var (
	cmd           string
	topic         string
	queue         string
	channel       string
	message       string
	nodeAddr      string
	etcdEndpoints string
	pushNum       int
	popNum        int
	delay         int
	msgId         string
	bindKey       string
	routeKey      string
)

func parseFlag() {
	flag.StringVar(&cmd, "cmd", "push", "command name")
	flag.StringVar(&topic, "topic", "golang", "topic name")
	flag.StringVar(&queue, "queue", "", "queue name")
	flag.StringVar(&channel, "channel", "golang", "channel name")
	flag.StringVar(&message, "message", "golang", "message")
	flag.StringVar(&nodeAddr, "node_addr", "127.0.0.1:9503", "node address")
	flag.StringVar(&etcdEndpoints, "etcd_endpoints", "127.0.0.1:2379", "the address of etcd")
	flag.StringVar(&msgId, "msg_id", "", "the id of message.")
	flag.StringVar(&bindKey, "bind_key", "", "bind key")
	flag.StringVar(&routeKey, "route_key", "", "route key")
	flag.IntVar(&delay, "delay", 0, "delay time for message.")
	flag.IntVar(&popNum, "pop_num", 100, "the number of pop, default to 100.")
	flag.Parse()
}

func main() {
	parseFlag()
	rpc := gctl.NewRpcClient(":9503", 1)
	start := time.Now()
	switch cmd {
	case "delcare":
		// 声明队列
	case "push":
		data := &gctl.PushSchema{
			Topic:    topic,
			RouteKey: routeKey,
			Delay:    int32(delay),
			Message:  message,
		}
		messageId, err := rpc.Push(data)
		if err != nil {
			log.Fatalln(err)
		} else {
			log.Printf("recv message.id : %v\n", messageId)
		}
	case "pop":
		data := &gctl.PopSchema{
			Topic: topic,
			Queue: queue,
		}
		err := rpc.Pop(data)
		if err != nil {
			log.Fatalln(err)
		}
	case "mpush":
		// 批量推送消息
	case "dead":
		// 拉取死信消息
	case "ack":
		// 确认已消费
	case "publish":
		data := &gctl.PublishSchema{
			Channel: channel,
			Message: message,
		}
		resp, err := rpc.Publish(data)
		if err != nil {
			log.Fatalln(err)
		} else {
			log.Printf("recv result: %v\n", resp)
		}
	case "subscribe":
		data := &gctl.SubscribeSchema{Channel: channel}
		err := rpc.Subscribe(data)
		if err != nil {
			log.Fatalln(err)
		}
	case "push_by_weight":
		// 多节点下,按节点权重推送消息
	case "push_by_rand":
		// 多节点下,按随机模式推送消息
	case "push_by_avg":
		// 多节点下,按平均模式推送消息
	case "pop_by_weight":
		// 多节点下,按节点权重消费消息
	case "test":
	default:
		log.Fatalf("unknown '%v' command.\n", cmd)
	}

	log.Printf("%v in total.\n", time.Now().Sub(start))
}
