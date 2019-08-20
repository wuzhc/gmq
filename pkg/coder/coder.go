// 编码器,编码,解码
// 支持json,gcode,protocol
package coder

type Coder interface {
	Encode(data interface{}) ([]byte, error)
	Decode(data []byte, v interface{}) error
}

func New(kind string) Coder {
	switch kind {
	case "json":
		return &JsonCoder{}
	case "gob":
		return &GobCoder{}
	}

	return nil
}
