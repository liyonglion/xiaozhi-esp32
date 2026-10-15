package sim

import "testing"

// 唤醒词行解析表（术语见仓库根 GLOSSARY.md：唤醒词/查询）。
// 命中「你好小智」或「小智」前缀即唤醒，剥去前缀与随后的分隔符，剩余为查询。
func TestParseWake(t *testing.T) {
	tests := []struct {
		name  string
		line  string
		query string
		woke  bool
	}{
		{"全角逗号分隔", "小智，我想听故事", "我想听故事", true},
		{"无分隔符", "小智我想听故事", "我想听故事", true},
		{"长唤醒词加逗号", "你好小智，讲个笑话", "讲个笑话", true},
		{"长唤醒词无分隔符", "你好小智讲个笑话", "讲个笑话", true},
		{"冒号分隔", "小智: 今天天气怎么样", "今天天气怎么样", true},
		{"全角冒号", "小智：今天天气怎么样", "今天天气怎么样", true},
		{"句号分隔", "小智。再见", "再见", true},
		{"空格分隔", "小智 你好", "你好", true},
		{"连续分隔符", "小智，,： 你好", "你好", true},
		{"纯唤醒词短", "小智", "", true},
		{"纯唤醒词长", "你好小智", "", true},
		{"唤醒词加尾随分隔符", "小智，", "", true},
		{"前后空白被剥除", "  小智，你好  ", "你好", true},
		{"无唤醒词前缀", "今天天气怎么样", "今天天气怎么样", false},
		{"唤醒词在句中不算", "帮我叫一下小智", "帮我叫一下小智", false},
		{"你好开头但非唤醒词", "你好，世界", "你好，世界", false},
		{"exit 不是唤醒词", "exit", "exit", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, woke := ParseWake(tt.line)
			if woke != tt.woke {
				t.Errorf("ParseWake(%q) 唤醒 = %v, 期望 %v", tt.line, woke, tt.woke)
			}
			if query != tt.query {
				t.Errorf("ParseWake(%q) 查询 = %q, 期望 %q", tt.line, query, tt.query)
			}
		})
	}
}
