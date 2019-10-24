package gnode

import (
	"encoding/binary"
	"errors"
)

const (
	MSG_MAX_DELAY      = 259200 // 最大延迟时间
	MSG_MAX_TTR        = 30     // 最大超时时间
	MSG_MAX_RETRY      = 5      // 消息最大重试次数
	MSG_DELAY_INTERVAL = 10     // 延时间隔,第一次60,第二次120,第三次180,最大为MSG_MAX_RETRY次
)

var (
	ErrMessageNotExist = errors.New("no message")
)

type RespMsgData struct {
	Id    uint64 `json:"id"`
	Body  string `json:"body"`
	Retry uint16 `json:"retry_count"`
}

type RecvMsgData struct {
	Body  string `json:"body"`
	Topic string `json:"topic"`
	Delay int    `json:"delay"`
}

// 消息结构
type Msg struct {
	Id     uint64
	Retry  uint16
	Delay  uint32
	Expire int64
	Body   []byte
}

// 消息编码
func Encode(m *Msg) []byte {
	var data = make([]byte, 8+2+4+len(m.Body))
	binary.BigEndian.PutUint64(data[:8], m.Id)
	binary.BigEndian.PutUint16(data[8:10], m.Retry)
	binary.BigEndian.PutUint32(data[10:14], m.Delay)
	copy(data[14:], m.Body)
	return data
}

// 消息解码
func Decode(data []byte) *Msg {
	msg := &Msg{}
	if len(data) < 14 {
		return msg
	}

	msg.Id = binary.BigEndian.Uint64(data[:8])
	msg.Retry = binary.BigEndian.Uint16(data[8:10])
	msg.Delay = binary.BigEndian.Uint32(data[10:14])
	msg.Body = data[14:]
	return msg
}
