package utils

import (
	"errors"
	"reflect"
	"strconv"
	"time"
)

// time.Time to "2019-06-07 12:00:00"
func FormatTime(t time.Time) string {
	return t.Format("2006-01-02 15:04:05")
}

// unix to "2019-06-07 12:00:00"
func UnixToFormatTime(t interface{}) string {
	v, err := assertValue(t)
	if err != nil {
		return ""
	}

	return time.Unix(v, 0).Format("2006-01-02 15:04:05")
}

// 172992 to "48h3m12s"
func SecToTimeString(t interface{}) string {
	v, err := assertValue(t)
	if err != nil {
		return ""
	}

	return (time.Duration(v) * time.Second).String()
}

func assertValue(t interface{}) (int64, error) {
	var v int64

	ty := reflect.TypeOf(t)
	switch ty.Kind() {
	case reflect.String:
		i, err := strconv.Atoi(t.(string))
		if err != nil {
			return 0, err
		}
		v = int64(i)
	case reflect.Int:
		v = int64(t.(int))
	case reflect.Int8:
		v = int64(t.(int8))
	case reflect.Int32:
		v = int64(t.(int32))
	case reflect.Int64:
		v = t.(int64)
	default:
		return 0, errors.New("Unkown type")
	}

	return v, nil
}
