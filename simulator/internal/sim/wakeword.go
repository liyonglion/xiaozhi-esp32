package sim

import "strings"

// wakeWords 按优先级排列：先匹配「你好小智」再「小智」，避免长词被短词截断。
var wakeWords = []string{"你好小智", "小智"}

// wakeSeparators 是唤醒词之后可省略的分隔符（规范 spec：，,。: 空格；含全角变体）。
const wakeSeparators = "，,。:：、；;！!？? \t"

// ParseWake 判断一行终端输入是否以唤醒词开头。
// 命中时返回剥去唤醒词与后续分隔符后的查询文本（可能为空），woke 为 true；
// 未命中时原样返回（仅剥除首尾空白），woke 为 false。
func ParseWake(line string) (query string, woke bool) {
	line = strings.TrimSpace(line)
	for _, word := range wakeWords {
		if after, ok := strings.CutPrefix(line, word); ok {
			return strings.TrimLeft(after, wakeSeparators), true
		}
	}
	return line, false
}
