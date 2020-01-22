package gnode

import (
	"encoding/binary"
)

// the message struct for commitLog
type CommitLogMsg struct {
	Id              uint64
	Offset          int64
	MsgLen          uint32
	Delay           uint32
	QueueId         uint16
	QueueOffset     int64
	CreateTimestamp int64
	TopicLen        uint32
	Topic           []byte
	BodyLen         uint32
	Body            []byte
	TagLen          uint32
	Tag             []byte
}

func (msg *CommitLogMsg) validate() error {
	if len(msg.Topic) == 0 {
		return NewClientErr(ErrParams, "topic can't empty.")
	}
	if len(msg.Body) == 0 {
		return NewClientErr(ErrParams, "body can't empty.")
	}
	if len(msg.Tag) == 0 {
		return NewClientErr(ErrParams, "tag can't empty.")
	}
	return nil

}

func EncodeCommitLogMsg(msg *CommitLogMsg) []byte {
	var data = make([]byte, 8+8+4+4+2+8+8+4+int(msg.TopicLen)+4+int(msg.BodyLen)+4+int(msg.TagLen))
	binary.BigEndian.PutUint32(data[0:4], msg.MsgLen)
	binary.BigEndian.PutUint64(data[4:12], msg.Id)
	binary.BigEndian.PutUint64(data[12:20], uint64(msg.Offset))
	binary.BigEndian.PutUint32(data[20:24], msg.Delay)
	binary.BigEndian.PutUint16(data[24:26], msg.QueueId)
	binary.BigEndian.PutUint64(data[26:34], uint64(msg.QueueOffset))
	binary.BigEndian.PutUint64(data[34:42], uint64(msg.CreateTimestamp))

	binary.BigEndian.PutUint32(data[42:46], msg.TopicLen)
	topicEndOffset := 46 + int(msg.TopicLen)
	copy(data[46:topicEndOffset], msg.Topic)

	binary.BigEndian.PutUint32(data[topicEndOffset:topicEndOffset+4], msg.BodyLen)
	bodyEndOffset := topicEndOffset + 4 + int(msg.BodyLen)
	copy(data[topicEndOffset+4:bodyEndOffset], msg.Body)

	binary.BigEndian.PutUint32(data[bodyEndOffset:bodyEndOffset+4], msg.TagLen)
	tagEndOffset := bodyEndOffset + 4 + int(msg.TagLen)
	copy(data[bodyEndOffset+4:tagEndOffset], msg.Tag)

	return data
}

func DecodeCommitLogMsg(data []byte) *CommitLogMsg {
	msg := &CommitLogMsg{}
	msg.MsgLen = binary.BigEndian.Uint32(data[0:4])
	msg.Id = binary.BigEndian.Uint64(data[4:12])
	msg.Offset = int64(binary.BigEndian.Uint64(data[12:20]))
	msg.Delay = binary.BigEndian.Uint32(data[20:24])
	msg.QueueId = binary.BigEndian.Uint16(data[24:26])
	msg.QueueOffset = int64(binary.BigEndian.Uint64(data[26:34]))
	msg.CreateTimestamp = int64(binary.BigEndian.Uint64(data[34:42]))

	msg.TopicLen = binary.BigEndian.Uint32(data[42:46])
	msg.Topic = data[46 : 46+msg.TopicLen]

	msg.BodyLen = binary.BigEndian.Uint32(data[46+msg.TopicLen : 46+msg.TopicLen+4])
	bodyStartOffset := 46 + msg.TopicLen + 4
	msg.Body = data[bodyStartOffset : bodyStartOffset+msg.BodyLen]

	msg.TagLen = binary.BigEndian.Uint32(data[bodyStartOffset+msg.BodyLen : bodyStartOffset+msg.BodyLen+4])
	tagStartOffset := bodyStartOffset + msg.BodyLen + 4
	msg.Tag = data[tagStartOffset : tagStartOffset+msg.TagLen]

	return msg
}

func SetCommitLogOffset(data []byte, offset uint64) {
	binary.BigEndian.PutUint64(data[12:20], offset)
}

func SetCommitLogQueueOffset(data []byte, offset uint64) {
	binary.BigEndian.PutUint64(data[26:34], offset)
}

// the message struct for consumeQueue
type ConsumeQueueMsg struct {
	Topic     []byte
	QueueId   int16
	LogOffset int64
	MsgLen    uint32
	Tag       []byte
}

func EncodeConsumeQueueMsg(msg *ConsumeQueueMsg) []byte {
	var data = make([]byte, 8+4+16)
	binary.BigEndian.PutUint64(data[:8], uint64(msg.LogOffset))
	binary.BigEndian.PutUint32(data[8:12], msg.MsgLen)
	copy(data[12:28], msg.Tag)
	return data
}

func DecodeConsumeQueueMsg(data []byte) *ConsumeQueueMsg {
	msg := &ConsumeQueueMsg{
		LogOffset: int64(binary.BigEndian.Uint64(data[0:8])),
		MsgLen:    binary.BigEndian.Uint32(data[8:12]),
		Tag:       data[12:28],
	}
	return msg
}

type PullMsgSchema struct {
	QueueOffset int64
	MsgId       uint64
	MsgBody     []byte
}
