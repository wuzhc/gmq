package gnode

import (
	"net"
	"os"
	"testing"
	"time"
)

// 测试推送
func TestPush(t *testing.T) {
	gn := runNode()

	// 连接节点
	client, err := NewClient("127.0.0.1:9503", 1)
	if err != nil {
		t.Fatalf("conn failed, %v\n", err)
	}
	if err := client.Push("golang", "0", "hello world"); err != nil {
		t.Fatalf("push message faield, %v\n", err)
	}

	rtype, result := client.Recv()
	if rtype != RESP_RESULT {
		t.Errorf("rtype expect 103, result:%v", rtype)
	}
	if len(result) == 0 {
		t.Errorf("result expect msg.Id, result: is empty")
	}

	close(gn.exitChan)
	gn.wg.Wait()
}

// 测试重写队列文件对确认消息的影响
func TestRewriteForAck(t *testing.T) {
	os.Remove("data/test-topic.meta")
	os.Remove("data/test-topic.queue")
	os.Remove("data/gnode.log")

	gn := runNode()

	topic := gn.ctx.Dispatcher.GetTopic("test-topic")
	topic.isAutoAck = false

	for i := 0; i < 200000; i++ {
		msg := &Msg{gn.ctx.Dispatcher.snowflake.Generate(), 0, 0, 0, []byte("hello world")}
		topic.push(msg)

		var rmsg *Msg
		if i < 200000 {
			rmsg, _ = topic.pop()
		}

		// 确认消费掉部分消息,这样重写文件才会变小
		if i < 199950 {
			topic.ack(rmsg.Id)
		}
	}

	// t.Log(topic.waitAckMap)
	topic.queue.scan()
	t.Logf("wait-num:%v, pop-num:%v, push-num:%v, filesize:%v", len(topic.waitAckMap), topic.popNum, topic.pushNum, topic.queue.filesize)

	t.Logf("before rewrite: write-offset:%v, read-offset:%v, scan-offset:%v, filesize:%v\n", topic.queue.woffset, topic.queue.roffset, topic.queue.soffset, topic.queue.filesize)
	topic.queue.rewrite()
	t.Logf("after rewrite :write-offset:%v, read-offset:%v, scan-offset:%v, filesize:%v\n", topic.queue.woffset, topic.queue.roffset, topic.queue.soffset, topic.queue.filesize)

	// 再次测试重写文件后,能否正确确认消息
	for k, _ := range topic.waitAckMap {
		topic.ack(k)
	}

	topic.queue.scan()
	if len(topic.waitAckMap) > 0 {
		t.Errorf("the lenght of waitAckMap expect:%v, result:%v\n", 0, len(topic.waitAckMap))
	}

	// t.Log(topic.waitAckMap)
	if topic.queue.soffset != topic.queue.roffset {
		t.Errorf("read-offset:%v != scan-offset:%v\n", topic.queue.soffset, topic.queue.roffset)
	}

	close(gn.exitChan)
	gn.wg.Wait()
}

func BenchmarkPush(b *testing.B) {
	conn, err := net.Dial("tcp", "127.0.0.1:9503")
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		// push(conn, []byte("hello world"), "wuzhc", "0")
	}
}

func runNode() *Gnode {
	gn := New()
	go func() {
		gn.Run()
	}()

	// 确保节点启动成功
	time.Sleep(2 * time.Second)
	return gn
}
