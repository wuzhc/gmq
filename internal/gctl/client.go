package gctl

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/etcd-io/etcd/clientv3"
	"github.com/wuzhc/gmq/internal/gnode"
)

const (
	RESP_MESSAGE = 101
	RESP_ERROR   = 102
	RESP_RESULT  = 103
)

var (
	ErrTopicEmpty   = errors.New("topic is empty")
	ErrTopicChannel = errors.New("channel is empty")
)

type MsgPkg struct {
	Body     string `json:"body"`
	Topic    string `json:"topic"`
	Delay    int    `json:"delay"`
	RouteKey string `json:"route_key"`
}

type MMsgPkg struct {
	Body  string
	Delay int
}

type Client struct {
	conn   net.Conn
	addr   string
	weight int
}

func NewClient(addr string, weight int) *Client {
	if len(addr) == 0 {
		log.Fatalln("address is empty")
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatalln(err)
	}

	return &Client{
		conn:   conn,
		addr:   addr,
		weight: weight,
	}
}

func (c *Client) Exit() {
	c.conn.Close()
}

// 获取客户端连接的节点地址
func (c *Client) GetAddr() string {
	return c.addr
}

// 消费消息
// pop <topic_name> <bind_key>
func (c *Client) Pop(topic, bindKey string) error {
	if len(topic) == 0 {
		return ErrTopicEmpty
	}

	var params [][]byte
	params = append(params, []byte("pop"))
	params = append(params, []byte(topic))
	params = append(params, []byte(bindKey))
	line := bytes.Join(params, []byte(" "))
	if _, err := c.conn.Write(line); err != nil {
		return err
	}
	if _, err := c.conn.Write([]byte{'\n'}); err != nil {
		return err
	}

	return nil
}

// 消费消息
// dead <topic_name> <bind_key>
func (c *Client) Dead(topic, bindKey string) error {
	if len(topic) == 0 {
		return ErrTopicEmpty
	}

	var params [][]byte
	params = append(params, []byte("dead"))
	params = append(params, []byte(topic))
	params = append(params, []byte(bindKey))
	line := bytes.Join(params, []byte(" "))
	if _, err := c.conn.Write(line); err != nil {
		return err
	}
	if _, err := c.conn.Write([]byte{'\n'}); err != nil {
		return err
	}

	return nil
}

// 死信
func (c *Client) Dead_back(topic string, num int) error {
	if len(topic) == 0 {
		return ErrTopicEmpty
	}

	var params [][]byte
	params = append(params, []byte("dead"))
	params = append(params, []byte(topic))
	params = append(params, []byte(strconv.Itoa(num)))
	line := bytes.Join(params, []byte(" "))
	if _, err := c.conn.Write(line); err != nil {
		return err
	}
	if _, err := c.conn.Write([]byte{'\n'}); err != nil {
		return err
	}

	return nil
}

// declare queue
// queue <topic_name> <bind_key>\n
func (c *Client) Declare(topic, bindKey string) error {
	if len(topic) == 0 {
		return ErrTopicEmpty
	}

	var params [][]byte
	params = append(params, []byte("queue"))
	params = append(params, []byte(topic))
	params = append(params, []byte(bindKey))
	line := bytes.Join(params, []byte(" "))
	if _, err := c.conn.Write(line); err != nil {
		return err
	}
	if _, err := c.conn.Write([]byte{'\n'}); err != nil {
		return err
	}

	return nil
}

// 生产消息
// pub <topic_name> <route_key> <delay-time>
// [ 4-byte size in bytes ][ N-byte binary data ]
func (c *Client) Push(pkg MsgPkg) error {
	if len(pkg.Topic) == 0 {
		return ErrTopicEmpty
	}

	var params [][]byte
	params = append(params, []byte("pub"))
	params = append(params, []byte(pkg.Topic))
	params = append(params, []byte(pkg.RouteKey))
	params = append(params, []byte(strconv.Itoa(pkg.Delay)))
	line := bytes.Join(params, []byte(" "))
	if _, err := c.conn.Write(line); err != nil {
		return err
	}
	if _, err := c.conn.Write([]byte{'\n'}); err != nil {
		return err
	}

	// write msg.body
	bodylen := make([]byte, 4)
	body := pkg.Body
	binary.BigEndian.PutUint32(bodylen, uint32(len(body)))
	if _, err := c.conn.Write(bodylen); err != nil {
		return err
	}
	if _, err := c.conn.Write([]byte(body)); err != nil {
		return err
	}

	return nil
}

// 批量生产消息
// mpub <topic_name> <num>
// <msg.len> <[]byte({"delay":1,"body":"xxx","topic":"xxx","routeKey":"xxx"})>
// <msg.len> <[]byte({"delay":1,"body":"xxx","topic":"xxx","routeKey":"xxx"})>
func (c *Client) Mpush(topic string, msgs []MMsgPkg, routeKey string) error {
	if len(topic) == 0 {
		return ErrTopicEmpty
	}
	lmsg := len(msgs)
	if lmsg == 0 {
		return errors.New("msgs is empty")
	}

	var params [][]byte
	params = append(params, []byte("mpub"))
	params = append(params, []byte(topic))
	params = append(params, []byte(strconv.Itoa(lmsg)))
	line := bytes.Join(params, []byte(" "))
	if _, err := c.conn.Write(line); err != nil {
		return err
	}
	if _, err := c.conn.Write([]byte{'\n'}); err != nil {
		return err
	}

	for i := 0; i < lmsg; i++ {
		pkg := &MsgPkg{
			Body:     msgs[i].Body,
			Delay:    msgs[i].Delay,
			Topic:    topic,
			RouteKey: routeKey,
		}
		nbyte, err := json.Marshal(pkg)
		if err != nil {
			log.Println(err)
			continue
		}

		bodylen := make([]byte, 4)
		binary.BigEndian.PutUint32(bodylen, uint32(len(nbyte)))
		if _, err := c.conn.Write(bodylen); err != nil {
			return err
		}
		if _, err := c.conn.Write(nbyte); err != nil {
			return err
		}
	}

	return nil
}

// 确认已消费消息
// ack <message_id> <topic> <bind_key>\n
func (c *Client) Ack(topic, msgId, bindKey string) error {
	if len(topic) == 0 {
		return ErrTopicEmpty
	}

	var params [][]byte
	params = append(params, []byte("ack"))
	params = append(params, []byte(msgId))
	params = append(params, []byte(topic))
	params = append(params, []byte(bindKey))
	line := bytes.Join(params, []byte(" "))
	if _, err := c.conn.Write(line); err != nil {
		return err
	}
	if _, err := c.conn.Write([]byte{'\n'}); err != nil {
		return err
	}

	return nil
}

// 设置topic配置
// set <topic> <is_auto_ack>
func (c *Client) Set(topic string, isAutoAck int) error {
	if len(topic) == 0 {
		return ErrTopicEmpty
	}

	var params [][]byte
	params = append(params, []byte("set"))
	params = append(params, []byte(topic))
	params = append(params, []byte(strconv.Itoa(isAutoAck)))
	line := bytes.Join(params, []byte(" "))
	if _, err := c.conn.Write(line); err != nil {
		return err
	}
	if _, err := c.conn.Write([]byte{'\n'}); err != nil {
		return err
	}

	return nil
}

// 订阅频道
// subscribe <channel_name> <message>\n
func (c *Client) Subscribe(channel string) error {
	if len(channel) == 0 {
		return ErrTopicChannel
	}

	var params [][]byte
	params = append(params, []byte("subscribe"))
	params = append(params, []byte(channel))
	line := bytes.Join(params, []byte(" "))
	if _, err := c.conn.Write(line); err != nil {
		return err
	}
	if _, err := c.conn.Write([]byte{'\n'}); err != nil {
		return err
	}

	return nil
}

// 发布消息
// publish <channel_name>\n
// <message_len> <message>
func (c *Client) Publish(channel, message string) error {
	if len(channel) == 0 {
		return ErrTopicChannel
	}

	var params [][]byte
	params = append(params, []byte("publish"))
	params = append(params, []byte(channel))
	line := bytes.Join(params, []byte(" "))
	if _, err := c.conn.Write(line); err != nil {
		return err
	}
	if _, err := c.conn.Write([]byte{'\n'}); err != nil {
		return err
	}

	bodylen := make([]byte, 4)
	body := []byte(message)
	binary.BigEndian.PutUint32(bodylen, uint32(len(body)))
	if _, err := c.conn.Write(bodylen); err != nil {
		return err
	}
	if _, err := c.conn.Write([]byte(body)); err != nil {
		return err
	}

	return nil
}

func (c *Client) Recv() (int, []byte) {
	respTypeBuf := make([]byte, 2)
	io.ReadFull(c.conn, respTypeBuf)
	respType := binary.BigEndian.Uint16(respTypeBuf)

	bodyLenBuf := make([]byte, 4)
	io.ReadFull(c.conn, bodyLenBuf)
	bodyLen := binary.BigEndian.Uint32(bodyLenBuf)

	bodyBuf := make([]byte, bodyLen)
	io.ReadFull(c.conn, bodyBuf)

	return int(respType), bodyBuf
}

// 生产消息
func Example_Produce(c *Client, topic string, num int, routeKey string) {
	type bodyCnt struct {
		From    string `json:"from"`
		To      string `json:"to"`
		Content string `json:"content"`
	}

	for i := 0; i < num; i++ {
		msg := MsgPkg{}
		msg.Body = string("hello world")
		msg.Topic = topic
		msg.Delay = 0
		msg.RouteKey = routeKey
		if err := c.Push(msg); err != nil {
			log.Fatalln(err)
		}

		// receive response
		rtype, data := c.Recv()
		if rtype == RESP_RESULT {
			log.Println(string(data))
		}
	}
}

// 声明队列
func Example_DelcareQueue(c *Client, topic, bindKey string) {
	if err := c.Declare(topic, bindKey); err != nil {
		log.Fatalln(err)
	}

	// receive response
	rtype, data := c.Recv()
	log.Println(fmt.Sprintf("rtype:%v, result:%v", rtype, string(data)))
}

// 消费消息
func Example_Consume(c *Client, topic, bindKey string) {
	if err := c.Pop(topic, bindKey); err != nil {
		log.Println(err)
	}

	// receive response
	rtype, data := c.Recv()
	log.Println(fmt.Sprintf("rtype:%v, result:%v", rtype, string(data)))
}

// 确认已消费消息
func Example_Ack(c *Client, topic, msgId, bindKey string) {
	if err := c.Ack(topic, msgId, bindKey); err != nil {
		log.Println(err)
	}

	// receive response
	rtype, data := c.Recv()
	log.Println(fmt.Sprintf("rtype:%v, result:%v", rtype, string(data)))
}

// 设置topic信息,目前只有isAutoAck选项
func Example_Set(c *Client, topic string, isAutoAck int) {
	if err := c.Set(topic, isAutoAck); err != nil {
		log.Println(err)
	}

	// receive response
	rtype, data := c.Recv()
	log.Println(fmt.Sprintf("rtype:%v, result:%v", rtype, string(data)))
}

func Example_Dead(c *Client, topic, bindKey string) {
	if err := c.Dead(topic, bindKey); err != nil {
		log.Println(err)
	}

	// receive response
	rtype, data := c.Recv()
	log.Println(fmt.Sprintf("rtype:%v, result:%v", rtype, string(data)))
}

// 死信
func Example_Dead_back(c *Client, topic string, num int) {
	if err := c.Dead_back(topic, num); err != nil {
		log.Println(err)
	}

	// receive response
	rtype, data := c.Recv()
	log.Println(fmt.Sprintf("rtype:%v, result:%v", rtype, string(data)))
}

// 轮询模式消费消息
// 当server端没有消息后,sleep 3秒后再次请求
func Example_Loop_Consume(c *Client, topic, bindKey string) {
	for {
		if err := c.Pop(topic, bindKey); err != nil {
			if ok, _ := regexp.MatchString("broken pipe", err.Error()); ok {
				log.Fatalln(err)
			}
		}

		// receive response
		rtype, data := c.Recv()
		if rtype == gnode.RESP_MESSAGE {
			log.Println(string(data))
		} else if rtype == gnode.RESP_ERROR {
			log.Fatalln(string(data))
		} else {
			log.Println(string(data))
		}
	}
}

// 批量生产消息
func Example_MProduce(c *Client, topic string, num int, bindKey string) {
	total := num
	var msgs []MMsgPkg
	for i := 0; i < total; i++ {
		msgs = append(msgs, MMsgPkg{"golang_" + strconv.Itoa(i), 0})
	}
	if err := c.Mpush(topic, msgs, bindKey); err != nil {
		log.Fatalln(err)
	}

	// receive response
	rtype, data := c.Recv()
	log.Println(fmt.Sprintf("rtype:%v, result:%v", rtype, string(data)))
}

// 订阅消息
func Example_Subscribe(c *Client, channel string) {
	if err := c.Subscribe(channel); err != nil {
		log.Println(err)
	}

	// receive response
	i := 0
	_ = c.conn.SetReadDeadline(time.Time{})
	log.Println(c.conn.LocalAddr())
	for {
		i++
		if i > 100 {
			break
		}

		// receive response
		rtype, data := c.Recv()
		fmt.Println(fmt.Sprintf("rtype:%v, result:%v", rtype, string(data)))
	}
}

// 发布消息
func Example_Publish(c *Client, channel, message string) {
	if err := c.Publish(channel, message); err != nil {
		log.Println(err)
	}

	// receive response
	rtype, data := c.Recv()
	log.Println(fmt.Sprintf("rtype:%v, result:%v", rtype, string(data)))
}

var clients []*Client

// 初始化客户端,建立和注册中心节点连接
func InitClients(endpoints string) ([]*Client, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("endpoints is empty.")
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("can't new etcd client.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	resp, err := cli.Get(ctx, "/gmq/node", clientv3.WithPrefix())
	cancel()
	if err != nil {
		return nil, err
	}

	var clients []*Client
	node := make(map[string]string)
	for _, ev := range resp.Kvs {
		fmt.Printf("%s => %s\n", ev.Key, ev.Value)
		if err := json.Unmarshal(ev.Value, &node); err != nil {
			return nil, err
		}

		tcpAddr := node["tcp_addr"]
		weight, _ := strconv.Atoi(node["weight"])
		c := NewClient(tcpAddr, weight)
		clients = append(clients, c)
	}

	return clients, nil
}

// 权重模式
func GetClientByWeightMode(endpoints string) *Client {
	if len(clients) == 0 {
		var err error
		clients, err = InitClients(endpoints)
		if err != nil {
			log.Fatalln(err)
		}
	}

	total := 0
	for _, c := range clients {
		total += c.weight
	}

	w := 0
	rand.Seed(time.Now().UnixNano())
	randValue := rand.Intn(total) + 1
	for _, c := range clients {
		prev := w
		w = w + c.weight
		if randValue > prev && randValue <= w {
			return c
		}
	}

	return nil
}

// 随机模式
func GetClientByRandomMode(endpoints string) *Client {
	if len(clients) == 0 {
		var err error
		clients, err = InitClients(endpoints)
		if err != nil {
			log.Fatalln(err)
		}
	}

	rand.Seed(time.Now().UnixNano())
	k := rand.Intn(len(clients))
	return clients[k]
}

// 平均模式
func GetClientByAvgMode(endpoints string) *Client {
	if len(clients) == 0 {
		var err error
		clients, err = InitClients(endpoints)
		if err != nil {
			log.Fatalln(err)
		}
	}

	c := clients[0]
	if len(clients) > 1 {
		// 已处理过的消息客户端重新放在最后
		clients = append(clients[1:], c)
	}

	return c
}
