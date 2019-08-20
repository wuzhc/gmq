package coder

import (
	"encoding/json"
)

type JsonCoder struct {
}

func (j *JsonCoder) Encode(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

func (j *JsonCoder) Decode(data []byte, to interface{}) error {
	return json.Unmarshal(data, to)
}
