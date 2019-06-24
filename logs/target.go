// 适配器
package logs

type target interface {
	WriteMsg(logData)
	Init(string) error
}

// 输出前缀
func logPrefix(level int) string {
	prefix, ok := logPrefixMap[level]
	if !ok {
		prefix = "-"
	}

	return prefix
}
