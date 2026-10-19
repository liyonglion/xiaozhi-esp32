package sim

import (
	"fmt"
	"sync"
	"time"
)

// 闹钟/定时器（对齐固件 main/alarm_manager.{h,cc} 的 v1 语义，见 GLOSSARY
// 「定时器」「闹钟」「响铃」）：一次性、不持久化；到点响铃（终端打印 + 响铃
// 字符）并代发一条 listen detect 查询，让 server TTS 播报留言；断网时只剩
// 本地响铃。时间参数一律由调用方注入，便于测试推进时钟。

const (
	alarmBeepInterval = 2 * time.Second // 响铃期间每 2 秒重响一次
	alarmRingTimeout  = 3 * time.Minute // 3 分钟无人停则自动停
	alarmMaxMinutes   = 1440            // 定时器上限 24 小时
)

// alarm 一条定时器（isTimer=true，相对时长）或闹钟（绝对钟点）。
type alarm struct {
	id       int
	deadline time.Time
	isTimer  bool
	message  string
}

// receipt 生成给 LLM 的 JSON 回执（字段与固件侧一致）。
func (a alarm) receipt(now time.Time) map[string]any {
	kind := "alarm"
	if a.isTimer {
		kind = "timer"
	}
	r := map[string]any{
		"id":         a.id,
		"type":       kind,
		"ring_at":    a.deadline.Format("01-02 15:04"),
		"in_minutes": int((a.deadline.Sub(now) + time.Minute - 1) / time.Minute),
	}
	if a.message != "" {
		r["message"] = a.message
	}
	return r
}

// alarmEngine 定时器存储、到点判定与响铃记账。goroutine 安全：工具调用在
// readLoop、到点检查在 alarmLoop、停止在 REPL，三处并发访问。
type alarmEngine struct {
	mu        sync.Mutex
	alarms    []alarm
	nextID    int
	ringing   *alarm
	ringStart time.Time
	lastBeep  time.Time
}

func newAlarmEngine() *alarmEngine {
	return &alarmEngine{}
}

// addTimer 设一个 minutes 分钟后的定时器。
func (e *alarmEngine) addTimer(minutes int, message string, now time.Time) (map[string]any, error) {
	if minutes < 1 || minutes > alarmMaxMinutes {
		return nil, fmt.Errorf("timer duration must be between 1 and %d minutes", alarmMaxMinutes)
	}
	return e.add(alarm{deadline: now.Add(time.Duration(minutes) * time.Minute), isTimer: true, message: message}, now), nil
}

// addAlarm 设一个绝对钟点的闹钟；今天已过则明天同时刻。
func (e *alarmEngine) addAlarm(hhmm, message string, now time.Time) (map[string]any, error) {
	parsed, err := time.ParseInLocation("15:04", hhmm, now.Location())
	if err != nil {
		return nil, fmt.Errorf("alarm time must be exactly 24-hour HH:MM, e.g. \"08:30\"")
	}
	deadline := time.Date(now.Year(), now.Month(), now.Day(),
		parsed.Hour(), parsed.Minute(), 0, 0, now.Location())
	if !deadline.After(now) {
		deadline = deadline.Add(24 * time.Hour)
	}
	return e.add(alarm{deadline: deadline, message: message}, now), nil
}

func (e *alarmEngine) add(a alarm, now time.Time) map[string]any {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.nextID++
	a.id = e.nextID
	e.alarms = append(e.alarms, a)
	return a.receipt(now)
}

// cancel 按 id 取消；找不到时返回错误（由工具层转 isError:true）。
func (e *alarmEngine) cancel(id int) (map[string]any, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, a := range e.alarms {
		if a.id == id {
			e.alarms = append(e.alarms[:i], e.alarms[i+1:]...)
			return map[string]any{"id": id, "cancelled": true}, nil
		}
	}
	return nil, fmt.Errorf("no alarm or timer with id %d; use list to check existing ids", id)
}

func (e *alarmEngine) list(now time.Time) map[string]any {
	e.mu.Lock()
	defer e.mu.Unlock()
	items := make([]any, 0, len(e.alarms))
	for _, a := range e.alarms {
		items = append(items, a.receipt(now))
	}
	return map[string]any{"count": len(items), "alarms": items}
}

// popDue 取出到点的最早一条（无则 nil）。
func (e *alarmEngine) popDue(now time.Time) *alarm {
	e.mu.Lock()
	defer e.mu.Unlock()
	earliest := -1
	for i := range e.alarms {
		if earliest == -1 || e.alarms[i].deadline.Before(e.alarms[earliest].deadline) {
			earliest = i
		}
	}
	if earliest == -1 || e.alarms[earliest].deadline.After(now) {
		return nil
	}
	due := e.alarms[earliest]
	e.alarms = append(e.alarms[:earliest], e.alarms[earliest+1:]...)
	return &due
}

// —— 响铃记账 ——

func (e *alarmEngine) isRinging() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ringing != nil
}

func (e *alarmEngine) startRing(a alarm, now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ringing = &a
	e.ringStart = now
	e.lastBeep = now // 首声由 fireAlarm 立即打印，这里只记账
}

func (e *alarmEngine) stopRing() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ringing = nil
}

// stopRingingIfAny 正在响铃则停并返回 true（任意输入行停铃用）。
func (e *alarmEngine) stopRingingIfAny() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	stopped := e.ringing != nil
	e.ringing = nil
	return stopped
}

func (e *alarmEngine) beepDue(now time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ringing == nil || now.Sub(e.lastBeep) < alarmBeepInterval {
		return false
	}
	e.lastBeep = now
	return true
}

func (e *alarmEngine) ringExpired(now time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ringing != nil && now.Sub(e.ringStart) >= alarmRingTimeout
}

// —— Session 侧驱动 ——

// alarmLoop 每秒驱动一次闹钟检查（对齐固件主循环 CLOCK_TICK 的挂载方式）。
func (s *Session) alarmLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if s.isClosing() {
			return
		}
		s.alarmTick(time.Now())
	}
}

func (s *Session) isClosing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closing
}

// alarmTick 到点检查与响铃节奏；now 注入以便测试推进时钟。
func (s *Session) alarmTick(now time.Time) {
	if s.alarms.isRinging() {
		// server 播报已接管（tts start → 播放态）：停铃转说话
		if s.currentState() == StateSpeaking {
			s.alarms.stopRing()
			return
		}
		if s.alarms.ringExpired(now) {
			s.alarms.stopRing()
			return
		}
		if s.alarms.beepDue(now) {
			s.printf("\a⏰ 响铃中（输入任意行停止）\n")
		}
		return
	}
	if a := s.alarms.popDue(now); a != nil {
		s.fireAlarm(*a, now)
	}
}

// fireAlarm 到点触发：先入响铃态（永不丢失），打断当前播放（闹钟优先），
// 再代发查询（失败只剩本地响铃，不重试）。
func (s *Session) fireAlarm(a alarm, now time.Time) {
	kind := "定时器"
	if !a.isTimer {
		kind = "闹钟"
	}
	detail := a.message
	if detail == "" {
		detail = "无留言"
	}
	s.alarms.startRing(a, now)
	s.printf("\a⏰ %s（%d 号）到点：%s — 输入任意行停止响铃\n", kind, a.id, detail)

	// Run 退出路径上不再做任何网络 I/O：响铃已入账（不丢闹钟），
	// 避免在飞的 dial/hello 超时拖延 wg.Wait 最多约 30 秒。
	if s.isClosing() {
		return
	}

	// 播放态到点：走打断路径（对齐 REPL 的 barge-in 次序：先关门再停播再 abort）
	if s.currentState() == StateSpeaking {
		s.setAborting(true)
		s.player.Stop()
		if err := s.sendAbort(); err != nil {
			s.reportSendError("打断（abort）", err)
		}
	}
	if err := s.ensureConnected(); err != nil {
		s.printf("闹钟播报无法送达（重连失败）: %v — 仅本地响铃\n", err)
		return
	}
	if err := s.sendDetect(alarmQueryText(a)); err != nil {
		s.reportSendError("闹钟播报", err)
	}
}

// alarmQueryText 代发查询的文本：server 以 listen detect 文本接收，跳过 ASR
// 交给 LLM，回复经 TTS 下发（见 GLOSSARY「查询」的设备代发语义）。
// 措辞必须是明确的播报指令——实测教训（2026-10-19）：写成「请叫醒我并提醒我」
// 会被 LLM 当成新的设闹钟请求，反而再设一只闹钟。
func alarmQueryText(a alarm) string {
	message := a.message
	if message == "" {
		message = "时间到了，快醒过来啦"
	}
	return "设备闹钟到点，请直接向用户播报下面这句提醒把人叫醒。" +
		"这是播报指令，不是新的用户请求：不要设置新闹钟，不要调用任何工具。" +
		"提醒内容：「" + message + "」"
}
