// 日志模块
// 采用设计模式:适配器模式
// 参与者:日志调度器(Dispatcher),日志具体处理器(Target,such as fileTarget, consoleTarget)

// 使用:
// logger := logs.NewDispatcher()
// logger.SetTarget(logs.TARGET_FILE, `{"filename":"xxxx.log","level":10,"max_size":500,"rotate":true}`)
// logger.Error("这是一个错误")
// logger.Debug("这是一个调试")
// logger.Info("这是一个信息")
// logger.Warn("这是一个警告")
package logs

import (
	"fmt"
)

const (
	LOG_ERROR = iota
	LOG_WARN
	LOG_INFO
	LOG_TRACE
	LOG_DEBUG
)

const (
	TARGET_FILE    = "file"
	TARGET_CONSOLE = "console"
)

var logPrefixMap = map[int]string{
	LOG_ERROR: "E",
	LOG_WARN:  "W",
	LOG_DEBUG: "D",
	LOG_INFO:  "I",
	LOG_TRACE: "T",
}

type logData struct {
	category string
	msg      string
	level    int
}

// 新建调度器
func NewDispatcher(lvl int) *Dispatcher {
	return &Dispatcher{
		Level: lvl,
	}
}

// 设置日志处理对象(可选有TARGET_FILE, TARGET_CONSOLE)
func (d *Dispatcher) SetTarget(name string, config string) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()

	target, ok := targetMap[name]
	if !ok {
		panic("适配器未注册")
	}

	// 不能重复设置同一个对象
	for _, tg := range d.Targets {
		if name == tg.name {
			panic(fmt.Sprintf("Can not set target %s twice \n", name))
		}
	}

	// 初始化提供者配置
	if err := target.Init(config); err != nil {
		panic("初始化提供者配置失败:" + err.Error())
	}

	d.Targets = append(d.Targets, &logTarget{
		name:   name,
		hander: target,
	})
}

// 错误输出
func (d *Dispatcher) Error(msg ...interface{}) {
	if d.Level < LOG_ERROR {
		return
	}

	logData := wrapLogData(LOG_ERROR, msg...)
	for _, tg := range d.Targets {
		tg.hander.WriteMsg(logData)
	}
}

// 追踪日志
func (d *Dispatcher) Trace(msg ...interface{}) {
	if d.Level < LOG_TRACE {
		return
	}

	logData := wrapLogData(LOG_TRACE, msg...)
	for _, tg := range d.Targets {
		tg.hander.WriteMsg(logData)
	}
}

// 调试输出
func (d *Dispatcher) Debug(msg ...interface{}) {
	if d.Level < LOG_DEBUG {
		return
	}

	logData := wrapLogData(LOG_DEBUG, msg...)
	for _, tg := range d.Targets {
		tg.hander.WriteMsg(logData)
	}
}

// 信息输出
func (d *Dispatcher) Info(msg ...interface{}) {
	if d.Level < LOG_INFO {
		return
	}

	logData := wrapLogData(LOG_INFO, msg...)
	for _, tg := range d.Targets {
		tg.hander.WriteMsg(logData)
	}
}

// 警告输出
func (d *Dispatcher) Warn(msg ...interface{}) {
	if d.Level < LOG_WARN {
		return
	}

	logData := wrapLogData(LOG_WARN, msg...)
	for _, tg := range d.Targets {
		tg.hander.WriteMsg(logData)
	}
}

type LogCategory string

// 格式化输出日志
func wrapLogData(level int, msg ...interface{}) (data logData) {
	data = logData{
		level:    level,
		msg:      "",
		category: "default",
	}

	for _, v := range msg {
		switch v.(type) {
		case LogCategory:
			data.category = fmt.Sprint(v)
		default:
			data.msg += fmt.Sprintf("%v ", v)
		}
	}

	return
}
