// ./gctl -node_addr="127.0.0.1:9503" -cmd="delcare" -topic="ketang" -bind_key="homework"
// ./gctl -node_addr="127.0.0.1:9503" -cmd="push" -topic="ketang" -route_key="homework" -push_num=1000
// ./gctl -node_addr="127.0.0.1:9503" -cmd="mpush" -topic="ketang" -route_key="homework" -push_num=1000
// ./gctl -node_addr="127.0.0.1:9503" -cmd="pop" -topic="ketang" -bind_key="homework" -pop_num=1000
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
	"strconv"
	"time"
)

var (
	cmd           string
	topic         string
	channel       string
	message       string
	nodeAddr      string
	etcdEndpoints string
	pushNum       int
	popNum        int
	msgId         string
	bindKey       string
	routeKey      string
)

func main() {
	flag.StringVar(&cmd, "cmd", "push", "command name")
	flag.StringVar(&topic, "topic", "golang", "topic name")
	flag.StringVar(&channel, "channel", "golang", "channel name")
	flag.StringVar(&message, "message", "golang", "message")
	flag.StringVar(&nodeAddr, "node_addr", "127.0.0.1:9503", "node address")
	flag.StringVar(&etcdEndpoints, "etcd_endpoints", "127.0.0.1:2379", "the address of etcd")
	flag.StringVar(&msgId, "msg_id", "", "the id of message.")
	flag.StringVar(&bindKey, "bind_key", "", "bind key")
	flag.StringVar(&routeKey, "route_key", "", "route key")
	flag.IntVar(&pushNum, "push_num", 100, "the number of push, default to 100.")
	flag.IntVar(&popNum, "pop_num", 100, "the number of pop, default to 100.")
	flag.Parse()

	client := gctl.NewClient(nodeAddr, 0)
	start := time.Now()
	switch cmd {
	case "delcare":
		// 声明队列
		gctl.Example_DelcareQueue(client, topic, bindKey)
	case "push":
		// 推送消息
		gctl.Example_Produce(client, topic, pushNum, routeKey)
	case "pop":
		// 拉取消息
		gctl.Example_Consume(client, topic, bindKey)
	case "pop_loop":
		// 轮询拉取消息
		gctl.Example_Loop_Consume(client, topic, bindKey)
	case "mpush":
		// 批量推送消息
		gctl.Example_MProduce(client, topic, pushNum, routeKey)
	case "dead":
		// 拉取死信消息
		gctl.Example_Dead(client, topic, bindKey)
	case "ack":
		// 确认已消费
		gctl.Example_Ack(client, topic, msgId, bindKey)
	case "publish":
		// 发布消息
		gctl.Example_Publish(client, channel, message)
	case "subscribe":
		// 订阅消息
		gctl.Example_Subscribe(client, channel)
	case "push_by_weight":
		// 多节点下,按节点权重推送消息
		for i := 0; i < pushNum; i++ {
			c := gctl.GetClientByWeightMode(etcdEndpoints)
			log.Printf("> select node(%v)\n", c.GetAddr())
			gctl.Example_Produce(c, topic, 1, routeKey)
		}
	case "push_by_rand":
		// 多节点下,按随机模式推送消息
		for i := 0; i < pushNum; i++ {
			c := gctl.GetClientByRandomMode(etcdEndpoints)
			log.Printf("> select node(%v)\n", c.GetAddr())
			gctl.Example_Produce(c, topic, 1, routeKey)
		}
	case "push_by_avg":
		// 多节点下,按平均模式推送消息
		for i := 0; i < pushNum; i++ {
			c := gctl.GetClientByAvgMode(etcdEndpoints)
			log.Printf("> select node(%v)\n", c.GetAddr())
			gctl.Example_Produce(c, topic, 1, routeKey)
		}
	case "pop_by_weight":
		// 多节点下,按节点权重消费消息
		for {
			c := gctl.GetClientByWeightMode(etcdEndpoints)
			log.Printf("> select node(%v)\n", c.GetAddr())
			if err := c.Pop(topic, bindKey); err != nil {
				log.Println(err)
			}

			// receive response
			rtype, data := c.Recv()
			if string(data) == "no message" {
				time.Sleep(1 * time.Second)
			}
			log.Printf("rtype:%v, result:%v\n", rtype, string(data))
		}
	case "test":
		for i := 24; i < 300; i++ {
			gctl.Example_Produce(client, "wuzhc_"+strconv.Itoa(i), 10, routeKey)
		}
	default:
		log.Fatalf("unknown '%v' command.\n", cmd)
	}

	log.Printf("%v in total.\n", time.Now().Sub(start))
}
