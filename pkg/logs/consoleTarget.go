package logs

import (
	"fmt"
	"os"
	"time"
)

type consoleTarget struct {
	fd *os.File
}

func (f *consoleTarget) WriteMsg(data logData) {
	prefix := logPrefix(data.level)
	now := time.Now().Format("2006-01-02 15:04:05")

	s := fmt.Sprintf("[%s] [%s] [%s]", prefix, now, data.category)
	s = fmt.Sprintf("%s %s\n", highlight(s, data.level), data.msg)
	_, err := f.fd.WriteString(s)
	if err != nil {
		panic(err)
	}
}

func (f *consoleTarget) Init(config string) error {
	f.fd = os.Stdout
	return nil
}

func init() {
	RegisterTarget(TARGET_CONSOLE, &consoleTarget{})
}

// 高亮显示文本
func highlight(msg string, level int) string {
	switch level {
	case LOG_TRACE:
		return Cyan(msg)
	case LOG_ERROR:
		return Red(msg)
	case LOG_INFO:
		return Blue(msg)
	case LOG_WARN:
		return Yellow(msg)
	case LOG_DEBUG:
		return Green(msg)
	default:
		return msg
	}
}
