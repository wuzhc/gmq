package utils

import (
	"gopkg.in/ini.v1"
)

type IniParser struct {
	conf_reader *ini.File // config reader
}

type IniParserError struct {
	error_info string
}

func (e *IniParserError) Error() string { return e.error_info }

func (this *IniParser) Load(config_file_name string) error {
	conf, err := ini.Load(config_file_name)
	if err != nil {
		this.conf_reader = nil
		return err
	}
	this.conf_reader = conf
	return nil
}

func (this *IniParser) GetString(section string, key string) string {
	if this.conf_reader == nil {
		return ""
	}

	s := this.conf_reader.Section(section)
	if s == nil {
		return ""
	}

	return s.Key(key).String()
}

func (this *IniParser) GetInt32(section string, key string) int32 {
	if this.conf_reader == nil {
		return 0
	}

	s := this.conf_reader.Section(section)
	if s == nil {
		return 0
	}

	value_int, _ := s.Key(key).Int()

	return int32(value_int)
}

func (this *IniParser) GetUint32(section string, key string) uint32 {
	if this.conf_reader == nil {
		return 0
	}

	s := this.conf_reader.Section(section)
	if s == nil {
		return 0
	}

	value_int, _ := s.Key(key).Uint()

	return uint32(value_int)
}

func (this *IniParser) GetInt64(section string, key string) int64 {
	if this.conf_reader == nil {
		return 0
	}

	s := this.conf_reader.Section(section)
	if s == nil {
		return 0
	}

	value_int, _ := s.Key(key).Int64()
	return value_int
}

func (this *IniParser) GetUint64(section string, key string) uint64 {
	if this.conf_reader == nil {
		return 0
	}

	s := this.conf_reader.Section(section)
	if s == nil {
		return 0
	}

	value_int, _ := s.Key(key).Uint64()
	return value_int
}

func (this *IniParser) GetFloat32(section string, key string) float32 {
	if this.conf_reader == nil {
		return 0
	}

	s := this.conf_reader.Section(section)
	if s == nil {
		return 0
	}

	value_float, _ := s.Key(key).Float64()
	return float32(value_float)
}

func (this *IniParser) GetFloat64(section string, key string) float64 {
	if this.conf_reader == nil {
		return 0
	}

	s := this.conf_reader.Section(section)
	if s == nil {
		return 0
	}

	value_float, _ := s.Key(key).Float64()
	return value_float
}
