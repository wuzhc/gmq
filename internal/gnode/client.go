package gnode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type Client struct {
	conn   net.Conn
	addr   string
	weight int
}

func NewClient(addr string, weight int) (*Client, error) {
	if len(addr) == 0 {
		return nil, fmt.Errorf("address is empty")
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:   conn,
		addr:   addr,
		weight: weight,
	}, nil
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
func (c *Client) Push(topic, delay, content string) error {
	var params [][]byte
	params = append(params, []byte("pub"))
	params = append(params, []byte(topic))
	params = append(params, []byte(delay))
	line := bytes.Join(params, []byte(" "))
	if _, err := c.conn.Write(line); err != nil {
		return err
	}
	if _, err := c.conn.Write([]byte{'\n'}); err != nil {
		return err
	}

	// write msg.body
	bodylen := make([]byte, 4)
	body := []byte(content)
	binary.BigEndian.PutUint32(bodylen, uint32(len(body)))
	if _, err := c.conn.Write(bodylen); err != nil {
		return err
	}
	if _, err := c.conn.Write(body); err != nil {
		return err
	}

	return nil
}
