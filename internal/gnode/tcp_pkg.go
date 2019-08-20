package gnode

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	STATUS_OK = iota
	STATUS_FAILED
)

var (
	ErrUnkownCmd = errors.New("unkown cmd")
)

type TcpPkg struct {
	Version [4]byte
	CmdLen  int16
	DataLen int16
	Cmd     []byte
	Data    []byte
}

func (p *TcpPkg) Pack(w io.Writer) error {
	var err error
	err = binary.Write(w, binary.BigEndian, &p.Version)
	err = binary.Write(w, binary.BigEndian, &p.CmdLen)
	err = binary.Write(w, binary.BigEndian, &p.DataLen)
	err = binary.Write(w, binary.BigEndian, &p.Cmd)
	err = binary.Write(w, binary.BigEndian, &p.Data)
	return err
}

func (p *TcpPkg) UnPack(r io.Reader) error {
	var err error
	err = binary.Read(r, binary.BigEndian, &p.Version)
	err = binary.Read(r, binary.BigEndian, &p.CmdLen)
	err = binary.Read(r, binary.BigEndian, &p.DataLen)
	p.Cmd = make([]byte, p.CmdLen)
	err = binary.Read(r, binary.BigEndian, &p.Cmd)
	p.Data = make([]byte, p.DataLen)
	err = binary.Read(r, binary.BigEndian, &p.Data)
	return err
}

func (p *TcpPkg) String() string {
	return fmt.Sprintf("version:%s,cmdLen:%d,cmd:%s,dataLen:%d,data:%s",
		p.Version, p.CmdLen, p.Cmd, p.DataLen, p.Data)
}

type TcpRespPkg struct {
	Type    int16
	DataLen int16
	Data    []byte
}

func (p *TcpRespPkg) Pack(w io.Writer) error {
	var err error
	err = binary.Write(w, binary.BigEndian, &p.Type)
	err = binary.Write(w, binary.BigEndian, &p.DataLen)
	err = binary.Write(w, binary.BigEndian, &p.Data)
	return err
}
