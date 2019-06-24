// 适配器
package logs

type target interface {
	WriteMsg(logData)
	Init(string) error
}

// 输出前缀
func logPrefix(level int) string {
	if prefix, ok := logPrefixMap[level]; ok {
		return prefix
	}

	return "-"
}
