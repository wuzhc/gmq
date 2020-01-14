package gctl

var (
	ErrParams  = "E_INVALID_PARAMS"
	ErrRequest = "E_INVALID_REQUST"
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
