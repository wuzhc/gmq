package gnode

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
)

// func TestPop(t *testing.T) {

// }

// func TestPush(t *testing.T) {

// }

func BenchmarkPush(b *testing.B) {
	conn, err := net.Dial("tcp", "127.0.0.1:9503")
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		push(conn, []byte("hello world"), "wuzhc", "0")
	}
}

func push(conn net.Conn, body []byte, topic, delay string) error {
	var params [][]byte
	params = append(params, []byte("pub"))
	params = append(params, []byte(topic))
	params = append(params, []byte(delay))
	line := bytes.Join(params, []byte(" "))
	if _, err := conn.Write(line); err != nil {
		return err
	}
	if _, err := conn.Write([]byte{'\n'}); err != nil {
		return err
	}

	// write msg.body
	bodylen := make([]byte, 4)
	binary.BigEndian.PutUint32(bodylen, uint32(len(body)))
	if _, err := conn.Write(bodylen); err != nil {
		return err
	}
	if _, err := conn.Write(body); err != nil {
		return err
	}

	return nil
}
