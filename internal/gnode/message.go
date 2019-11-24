package gnode

import (
	"encoding/binary"
	"errors"
)

const (
	MSG_STATUS_DEFAULT = iota // 消息默认状态
	MSG_STATUS_WAIT           // 消息被消费,等待客户端确认
	MSG_STATUS_FIN            // 已得到客户端确认,可移除消息
	MSG_STATUS_EXPIRE         // 未得到客户端确认,已超时

	MSG_MAX_DELAY = 259200 // 最大延迟时间
	MSG_MAX_TTR   = 30     // 最大超时时间
	MSG_MAX_RETRY = 5      // 消息最大重试次数
)

var (
	ErrMessageNotExist = errors.New("no message")
)

type RespMsgData struct {
	Id    string `json:"id"`
	Body  string `json:"body"`
	Retry uint16 `json:"retry_count"`
}

type RecvMsgData struct {
	Body     string `json:"body"`
	Topic    string `json:"topic"`
	Delay    int    `json:"delay"`
	RouteKey string `json:"route_key"`
}

// 消息结构
type Msg struct {
	Id       uint64
	Retry    uint16
	Delay    uint32
	Expire   uint64
	Body     []byte
	RouteKey string
}

type MsgIndex struct {
	Fid    int
	Offset int
}

func NewMsgIndex(fid, offset int) *MsgIndex {
	return &MsgIndex{fid, offset}
}

// 消息编码
// expire(8-bytes) + id(8-bytes) + retry(2-bytes) + body(n-bytes)
func Encode(m *Msg) []byte {
	var data = make([]byte, 8+8+2+len(m.Body))
	binary.BigEndian.PutUint64(data[:8], m.Expire)
	binary.BigEndian.PutUint64(data[8:16], m.Id)
	binary.BigEndian.PutUint16(data[16:18], m.Retry)
	copy(data[18:], m.Body)
	return data
}

// 消息解码
// expire(8-bytes) + id(8-bytes) + retry(2-bytes) + body(n-bytes)
func Decode(data []byte) *Msg {
	msg := &Msg{}
	if len(data) < 18 {
		return msg
	}

	msg.Expire = binary.BigEndian.Uint64(data[:8])
	msg.Id = binary.BigEndian.Uint64(data[8:16])
	msg.Retry = binary.BigEndian.Uint16(data[16:18])
	msg.Body = data[18:]
	return msg
}
