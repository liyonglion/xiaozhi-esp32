package sim

import (
	"io"
	"strings"
	"testing"
	"time"
)

// —— 引擎级测试（纯逻辑，注入时钟）——

// 定时器回执字段 + 到点判定：未到不触发，到点取最早一条且只取一次。
func TestAlarmEngineAddTimerReceiptAndDue(t *testing.T) {
	e := newAlarmEngine()
	now := time.Date(2026, 10, 18, 14, 0, 0, 0, time.Local)

	receipt, err := e.addTimer(20, "喝水", now)
	if err != nil {
		t.Fatalf("addTimer 不应失败: %v", err)
	}
	if receipt["type"] != "timer" || receipt["message"] != "喝水" {
		t.Errorf("回执类型/留言不符: %v", receipt)
	}
	if got, _ := receipt["in_minutes"].(int); got != 20 {
		t.Errorf("in_minutes = %v, 期望 20", receipt["in_minutes"])
	}
	if !strings.Contains(receipt["ring_at"].(string), "14:20") {
		t.Errorf("ring_at 应为 20 分钟后的钟点, 实际: %v", receipt["ring_at"])
	}

	if a := e.popDue(now.Add(19 * time.Minute)); a != nil {
		t.Fatalf("未到点不应触发, 实际触发: %+v", a)
	}
	a := e.popDue(now.Add(20 * time.Minute))
	if a == nil {
		t.Fatal("到点应触发")
	}
	if !a.isTimer || a.message != "喝水" {
		t.Errorf("触发项不符: %+v", a)
	}
	if again := e.popDue(now.Add(30 * time.Minute)); again != nil {
		t.Errorf("同一条只应触发一次, 又得到: %+v", again)
	}
}

// 闹钟「8点半」今天已过 → 明天同时刻；未过 → 今天。
func TestAlarmEngineAlarmPastTodayFiresTomorrow(t *testing.T) {
	e := newAlarmEngine()
	now := time.Date(2026, 10, 18, 14, 0, 0, 0, time.Local)

	receipt, err := e.addAlarm("08:30", "", now)
	if err != nil {
		t.Fatalf("addAlarm 不应失败: %v", err)
	}
	if receipt["type"] != "alarm" {
		t.Errorf("回执类型应为 alarm: %v", receipt)
	}
	if a := e.popDue(now.Add(18*time.Hour + 29*time.Minute)); a != nil {
		t.Fatalf("明天 08:30 前一分钟不应触发（今天 14:00 + 18h29m）, 实际: %+v", a)
	}
	a := e.popDue(now.Add(18*time.Hour + 30*time.Minute))
	if a == nil {
		t.Fatal("明天 08:30（14:00 + 18h30m）应触发")
	}

	// 未过的钟点：今天 20:00。
	if _, err := e.addAlarm("20:00", "开会", now); err != nil {
		t.Fatalf("addAlarm 不应失败: %v", err)
	}
	if a := e.popDue(now.Add(6 * time.Hour)); a == nil || a.message != "开会" {
		t.Errorf("今天 20:00 应触发带留言的闹钟, 实际: %+v", a)
	}
}

// 非法输入：钟点格式错 / 时长越界 → 错误；cancel 不存在的 id → 错误。
func TestAlarmEngineValidatesInput(t *testing.T) {
	e := newAlarmEngine()
	now := time.Date(2026, 10, 18, 14, 0, 0, 0, time.Local)

	for _, bad := range []string{"25:00", "8点半", "08:30:45", "8:3", ""} {
		if _, err := e.addAlarm(bad, "", now); err == nil {
			t.Errorf("addAlarm(%q) 应返回格式错误", bad)
		}
	}
	if _, err := e.addTimer(0, "", now); err == nil {
		t.Error("addTimer(0) 应返回越界错误")
	}
	if _, err := e.addTimer(1441, "", now); err == nil {
		t.Error("addTimer(1441) 应返回越界错误")
	}
	if _, err := e.cancel(99); err == nil {
		t.Error("cancel 不存在的 id 应返回错误")
	}

	receipt, _ := e.addTimer(5, "", now)
	id := receipt["id"].(int)
	if _, err := e.cancel(id); err != nil {
		t.Fatalf("cancel 已存在的 id 不应失败: %v", err)
	}
	got := e.list(now)
	if got["count"] != 0 {
		t.Errorf("取消后 list 应为空, 实际: %v", got)
	}
}

// list 回执：条目数、类型、留言字段。
func TestAlarmEngineList(t *testing.T) {
	e := newAlarmEngine()
	now := time.Date(2026, 10, 18, 14, 0, 0, 0, time.Local)

	e.addTimer(10, "a", now)
	e.addAlarm("20:00", "", now)
	got := e.list(now)
	if got["count"] != 2 {
		t.Fatalf("count = %v, 期望 2: %v", got["count"], got)
	}
	names := map[string]bool{}
	for _, raw := range got["alarms"].([]any) {
		item := raw.(map[string]any)
		names[item["type"].(string)] = true
	}
	if !names["timer"] || !names["alarm"] {
		t.Errorf("list 应同时含 timer 与 alarm, 实际: %v", got)
	}
}

// —— 会话级测试（fakeServer，注入时钟推进到点）——

// 到点全链路：响铃打印 + 代发查询（listen detect 带闹钟文本）送达 server；
// 任意输入行停铃且该行被消费（不再作为查询发出）；停后不再响。
func TestSessionAlarmFiresSendsQueryAndStopsByInput(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	out := &lockedBuffer{}
	sess := newTestSession(t, fs, out, nil, &fakePlayer{}, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	fs.nextReceived(t) // hello
	now := time.Now()
	if _, err := sess.alarms.addTimer(20, "喝水", now); err != nil {
		t.Fatalf("addTimer 失败: %v", err)
	}
	dueAt := now.Add(20 * time.Minute)

	// 到点：打印响铃 + 代发查询。
	sess.alarmTick(dueAt)
	eventually(t, func() bool {
		got := out.String()
		return strings.Contains(got, "⏰") && strings.Contains(got, "喝水")
	})
	if !sess.alarms.isRinging() {
		t.Fatal("触发后应处于响铃态")
	}
	query := fs.nextReceived(t)
	if query["type"] != "listen" || query["state"] != "detect" {
		t.Fatalf("到点应代发 listen detect 查询, 实际: %v", query)
	}
	if text, _ := query["text"].(string); !strings.Contains(text, "喝水") {
		t.Errorf("代发查询应带留言, 实际: %q", text)
	}

	// 响铃节奏：2 秒后再响一行。
	sess.alarmTick(dueAt.Add(2 * time.Second))
	eventually(t, func() bool { return strings.Contains(out.String(), "响铃中") })

	// 输入行停铃；该行被消费（select 证明没有新 detect 发出）。
	if _, err := io.WriteString(stdin, "停\n"); err != nil {
		t.Fatalf("写入输入失败: %v", err)
	}
	eventually(t, func() bool { return strings.Contains(out.String(), "已停止响铃") })
	select {
	case extra := <-fs.received:
		t.Fatalf("停铃行不应作为查询发出, 却收到: %v", extra)
	default:
	}

	// 停后不再响。
	before := strings.Count(out.String(), "响铃中")
	sess.alarmTick(dueAt.Add(6 * time.Second))
	sess.alarmTick(dueAt.Add(8 * time.Second))
	if after := strings.Count(out.String(), "响铃中"); after != before {
		t.Errorf("停铃后不应再响: %d → %d", before, after)
	}

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入退出失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Session.Run 返回错误: %v", err)
	}
}

// 播报接管：server 下发 tts start（播放态）后，响铃自动停。
func TestSessionAlarmRingStopsWhenTTSTakesOver(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	out := &lockedBuffer{}
	sess := newTestSession(t, fs, out, nil, &fakePlayer{}, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	fs.nextReceived(t) // hello
	now := time.Now()
	sess.alarms.addTimer(10, "", now)
	dueAt := now.Add(10 * time.Minute)

	sess.alarmTick(dueAt) // 触发 + 代发查询
	fs.nextReceived(t)    // 消费代发的 detect
	if !sess.alarms.isRinging() {
		t.Fatal("触发后应处于响铃态")
	}
	fs.send(t, map[string]any{"type": "tts", "state": "start"})
	eventually(t, func() bool { return strings.Count(out.String(), "播放中> \n") >= 1 })

	// 播放态下 tick：停铃（TTS 接管），不再打印响铃行。
	sess.alarmTick(dueAt.Add(2 * time.Second))
	if sess.alarms.isRinging() {
		t.Fatal("播报接管后应停铃")
	}
	before := strings.Count(out.String(), "响铃中")
	sess.alarmTick(dueAt.Add(4 * time.Second))
	sess.alarmTick(dueAt.Add(6 * time.Second))
	if after := strings.Count(out.String(), "响铃中"); after != before {
		t.Errorf("播报接管后不应再响: %d → %d", before, after)
	}

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入退出失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Session.Run 返回错误: %v", err)
	}
}

// —— MCP 工具接线 ——

// 闹钟四件套经 tools/call 真调度：set_timer/list/set_alarm/cancel 全链路，
// 非法参数回 isError:true（对齐固件 std::unexpected 语义）。
func TestSessionMCPAlarmToolsFlow(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	sess := newTestSession(t, fs, &lockedBuffer{}, nil, &fakePlayer{}, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	fs.nextReceived(t) // hello
	fs.send(t, mcpRequest(1, "tools/call", map[string]any{
		"name": "set_timer", "arguments": map[string]any{"minutes": 5, "message": "喝水"}}))
	fs.send(t, mcpRequest(2, "tools/call", map[string]any{
		"name": "set_alarm", "arguments": map[string]any{"time": "08:30"}}))
	fs.send(t, mcpRequest(3, "tools/call", map[string]any{"name": "clock_list", "arguments": map[string]any{}}))
	fs.send(t, mcpRequest(4, "tools/call", map[string]any{
		"name": "clock_cancel", "arguments": map[string]any{"id": 1}}))
	fs.send(t, mcpRequest(5, "tools/call", map[string]any{
		"name": "clock_cancel", "arguments": map[string]any{"id": 99}}))
	fs.send(t, mcpRequest(6, "tools/call", map[string]any{
		"name": "set_alarm", "arguments": map[string]any{"time": "25:99"}}))
	fs.send(t, mcpRequest(7, "tools/call", map[string]any{
		"name": "set_timer", "arguments": map[string]any{"minutes": 1.5}}))

	timer := wantToolCallResult(t, wantMCPReply(t, fs.nextReceived(t), 1, fs.sessionID))
	if !strings.Contains(timer, `"type":"timer"`) || !strings.Contains(timer, "喝水") {
		t.Errorf("set_timer 回执不符: %s", timer)
	}
	alarm := wantToolCallResult(t, wantMCPReply(t, fs.nextReceived(t), 2, fs.sessionID))
	if !strings.Contains(alarm, `"type":"alarm"`) || !strings.Contains(alarm, "08:30") {
		t.Errorf("set_alarm 回执不符: %s", alarm)
	}
	listed := wantToolCallResult(t, wantMCPReply(t, fs.nextReceived(t), 3, fs.sessionID))
	if !strings.Contains(listed, `"count":2`) {
		t.Errorf("list 应含 2 条: %s", listed)
	}
	cancelled := wantToolCallResult(t, wantMCPReply(t, fs.nextReceived(t), 4, fs.sessionID))
	if !strings.Contains(cancelled, `"cancelled":true`) {
		t.Errorf("cancel 回执不符: %s", cancelled)
	}

	// 后三个都是工具级失败：isError:true + 错误文本。
	for i, id := range []float64{5, 6, 7} {
		payload := wantMCPReply(t, fs.nextReceived(t), id, fs.sessionID)
		result, _ := payload["result"].(map[string]any)
		if result == nil {
			t.Fatalf("第 %d 个失败应答缺少 result: %v", i, payload)
		}
		if isErr, _ := result["isError"].(bool); !isErr {
			t.Errorf("第 %d 个失败应答 isError 应为 true: %v", i, payload)
		}
	}

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入退出失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Session.Run 返回错误: %v", err)
	}
}
