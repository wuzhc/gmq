// 日志调度器
// 功能:
// 初始化各个日志处理器
package logs

type logTarget struct {
	name   string
	hander target
}

// 调度器对象
type Dispatcher struct {
	Level   int
	Targets []*logTarget
}

// 处理器对象关系表
var targetMap = make(map[string]target)

// 注册适配器
func RegisterTarget(name string, tg target) {
	if _, ok := targetMap[name]; ok {
		panic("适配器已经注册")
	} else {
		targetMap[name] = tg
	}
}
