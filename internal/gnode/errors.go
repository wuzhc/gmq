package gnode

var (
	ErrParams       = "E_INVALID_PARAMS"
	ErrDelay        = "E_INVALID_DELAY"
	ErrReadConn     = "E_INVALID_READ"
	ErrPopMsg       = "E_INVALID_POP"
	ErrAckMsg       = "E_INVALID_ACK"
	ErrJson         = "E_INVALID_JSON"
	ErrPushNum      = "E_INVALID_PUSHNUM"
	ErrPush         = "E_INVALID_PUSH"
	ErrDead         = "E_INVALID_DEAD"
	ErrSet          = "E_INVALID_SET"
	ErrDeclare      = "E_INVALID_DECLARE"
	ErrSubscribe    = "E_INVALID_SUBSCRIBE"
	ErrUnkownCmd    = "E_INVALID_CMD"
	ErrTopicEmpty   = "E_INVALID_TOPIC"
	ErrBindKeyEmpty = "E_INVALID_BINDKEY"
	ErrChannelEmpty = "E_INVALID_CHANNEL"
	ErrPublish      = "E_INVALID_PUBLISH"
)

type FatalClientErr struct {
	code string
	desc string
}

func (err *FatalClientErr) Error() string {
	return err.code + " - " + err.desc
}

func NewFatalClientErr(code, desc string) *FatalClientErr {
	return &FatalClientErr{code, desc}
}

type ClientErr struct {
	code string
	desc string
}

func NewClientErr(code, desc string) *ClientErr {
	return &ClientErr{code, desc}
}

func (err *ClientErr) Error() string {
	return err.code + " - " + err.desc
}
