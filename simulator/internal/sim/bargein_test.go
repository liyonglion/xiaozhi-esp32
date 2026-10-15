package sim

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"
)

// —— sim#4 打断（barge-in）：播放态唤醒词 → 停播 + abort + 按新唤醒处理 ——

// 播放态输入唤醒词（含同行查询）：立即 Stop 本地播放，随后按序发出
// abort（带 reason）→ 新唤醒上报 → 新查询（术语见 GLOSSARY.md「打断」）。
func TestSessionBargeInStopsPlayerAndSendsAbortThenNewWake(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	out := &lockedBuffer{}
	player := &fakePlayer{}
	sess := newTestSession(t, fs, out, nil, player, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	if _, err := io.WriteString(stdin, "小智，讲个长故事\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	fs.nextReceived(t)                                      // hello
	wantDetect(t, fs.nextReceived(t), "你好小智", fs.sessionID) // 唤醒上报
	wantDetect(t, fs.nextReceived(t), "讲个长故事", fs.sessionID) // 查询

	// server 开始长 TTS：起播 + 下发若干 opus 包（同步到确已进入播放态）。
	fs.send(t, map[string]any{"type": "tts", "state": "start"})
	fs.sendBinary(t, []byte{0x01})
	fs.sendBinary(t, []byte{0x02})
	eventually(t, func() bool {
		frames, _, _ := player.snapshot()
		return len(frames) >= 3 // 头 2 页 + 首个音频页
	})

	// 播放态注入唤醒输入（同行携带查询）。
	if _, err := io.WriteString(stdin, "小智，再讲一个\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}

	// 上行顺序：abort → 新唤醒上报 → 新查询。
	abort := fs.nextReceived(t)
	wantString(t, abort, "type", "abort")
	wantString(t, abort, "reason", "wake_word_detected")
	wantString(t, abort, "session_id", fs.sessionID)
	wantDetect(t, fs.nextReceived(t), "你好小智", fs.sessionID)
	wantDetect(t, fs.nextReceived(t), "再讲一个", fs.sessionID)

	// 本地播放立即停止（Stop 而非 Finish）。
	eventually(t, func() bool {
		_, _, stops := player.snapshot()
		return stops == 1
	})

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Session.Run 返回错误: %v", err)
	}
	if got := out.String(); !strings.Contains(got, "播放中> ") {
		t.Errorf("打断前应进入播放态（提示符），实际输出:\n%s", got)
	}
}

// server 回 tts stop 确认打断：不再触发 Finish/Stop（防双重收尾），
// 在途的旧轮音频尾包被丢弃，状态保持待输入（新唤醒已在进行）。
func TestSessionBargeInStopConfirmNoDoubleTeardown(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	out := &lockedBuffer{}
	player := &fakePlayer{}
	sess := newTestSession(t, fs, out, nil, player, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	if _, err := io.WriteString(stdin, "小智，讲个长故事\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	fs.nextReceived(t) // hello
	fs.nextReceived(t) // 唤醒上报
	fs.nextReceived(t) // 查询

	fs.send(t, map[string]any{"type": "tts", "state": "start"})
	fs.sendBinary(t, []byte{0x01})
	eventually(t, func() bool {
		frames, _, _ := player.snapshot()
		return len(frames) >= 3
	})

	if _, err := io.WriteString(stdin, "小智\n"); err != nil { // 打断（无同行查询）
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	fs.nextReceived(t) // abort
	fs.nextReceived(t) // 新唤醒上报

	// 在途尾包 + server 的 tts stop 确认（真实时序：stop 确认总在新一轮 tts start 之前）。
	before, _, stopsBefore := player.snapshot()
	fs.sendBinary(t, []byte{0x09}) // 已被打断轮次的在途尾包
	fs.send(t, map[string]any{"type": "tts", "state": "stop"})
	eventually(t, func() bool {
		return strings.Count(out.String(), "待输入> ") >= 2
	})
	// 等一轮静默期，确认没有迟到的收尾动作。
	time.Sleep(50 * time.Millisecond)

	frames, finishes, stops := player.snapshot()
	if len(frames) != len(before) {
		t.Errorf("打断后的在途尾包应被丢弃：页数 %d → %d", len(before), len(frames))
	}
	if finishes != 0 {
		t.Errorf("tts stop 确认不应再触发 Finish（防双重收尾），实际 %d 次", finishes)
	}
	if stops != stopsBefore {
		t.Errorf("tts stop 确认不应再触发 Stop，实际 %d → %d 次", stopsBefore, stops)
	}
	// 状态保持待输入：打断后已按新唤醒进入待输入态，确认消息不得拉回待唤醒。
	if got := out.String(); strings.Contains(got, "待唤醒> ") && strings.Count(got, "待唤醒> \n") > 1 {
		t.Errorf("tts stop 确认后状态应保持待输入，不应回待唤醒，实际输出:\n%s", got)
	}

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Session.Run 返回错误: %v", err)
	}
}

// 待输入态重复唤醒：只重置状态，不重复发唤醒上报、不发 abort；
// 同行携带的查询仍照常发送。
func TestSessionRepeatWakeWhileListeningOnlyResets(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	out := &lockedBuffer{}
	player := &fakePlayer{}
	sess := newTestSession(t, fs, out, nil, player, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	if _, err := io.WriteString(stdin, "小智\n再讲一个故事\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	fs.nextReceived(t)                                      // hello
	wantDetect(t, fs.nextReceived(t), "你好小智", fs.sessionID) // 唤醒上报
	wantDetect(t, fs.nextReceived(t), "再讲一个故事", fs.sessionID) // 查询
	eventually(t, func() bool { // 已进入待输入态
		return strings.Count(out.String(), "待输入> ") >= 2
	})

	// 待输入态裸重复唤醒：不发唤醒上报、不发 abort、不发任何消息。
	if _, err := io.WriteString(stdin, "小智\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	select {
	case msg := <-fs.received:
		t.Errorf("待输入态重复唤醒不应发送消息, 却收到: %v", msg)
	default:
	}

	// 重复唤醒同行带查询：查询照常发送，但不重复唤醒上报。
	if _, err := io.WriteString(stdin, "小智，换一个吧\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	wantDetect(t, fs.nextReceived(t), "换一个吧", fs.sessionID)
	select {
	case msg := <-fs.received:
		t.Errorf("不应重复发送唤醒上报, 却收到: %v", msg)
	default:
	}

	_, _, stops := player.snapshot()
	if stops != 0 {
		t.Errorf("待输入态重复唤醒不应触发 Stop, 实际 %d 次", stops)
	}

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Session.Run 返回错误: %v", err)
	}
}

// 打断收尾后的新一轮 TTS 正常起播：头页重发（OpusHead 起头）、换 serial、
// 新轮 tts stop 正常 Finish，状态回待唤醒——打断不留残 stream/进程。
func TestSessionPlaybackAfterBargeInRestartsCleanStream(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	out := &lockedBuffer{}
	player := &fakePlayer{}
	sess := newTestSession(t, fs, out, nil, player, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	if _, err := io.WriteString(stdin, "小智，讲个长故事\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	fs.nextReceived(t) // hello
	fs.nextReceived(t) // 唤醒上报
	fs.nextReceived(t) // 查询

	// 旧轮：起播 + 1 个音频包。
	fs.send(t, map[string]any{"type": "tts", "state": "start"})
	fs.sendBinary(t, []byte{0x01})
	eventually(t, func() bool {
		frames, _, _ := player.snapshot()
		return len(frames) >= 3
	})
	round1Pages := countPages(t, player)

	// 打断 + server 确认。
	if _, err := io.WriteString(stdin, "小智，再讲一个\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	fs.nextReceived(t) // abort
	fs.nextReceived(t) // 新唤醒上报
	fs.nextReceived(t) // 新查询
	fs.send(t, map[string]any{"type": "tts", "state": "stop"}) // 打断确认

	// 新一轮 TTS（新唤醒的应答）：起播 + 音频 + 正常收尾。
	fs.send(t, map[string]any{"type": "tts", "state": "start"})
	fs.send(t, map[string]any{"type": "tts", "state": "sentence_start", "text": "好，再讲一个"})
	fs.sendBinary(t, []byte{0x02})
	fs.send(t, map[string]any{"type": "tts", "state": "stop"})
	eventually(t, func() bool {
		_, finishes, _ := player.snapshot()
		return finishes == 1
	})

	// 新轮页序列：紧接旧轮之后，以 OpusHead 头页重新起头，serial 与旧轮不同。
	frames, finishes, stops := player.snapshot()
	var all bytes.Buffer
	for _, f := range frames {
		all.Write(f)
	}
	pages := parseOggPages(t, all.Bytes())
	if len(pages) <= round1Pages {
		t.Fatalf("打断后新轮应继续产页, 共 %d 页（旧轮 %d 页）", len(pages), round1Pages)
	}
	next := pages[round1Pages]
	if !bytes.HasPrefix(next.payload, []byte("OpusHead")) {
		t.Errorf("新轮首页应为 OpusHead 重发, 实际 %q", firstBytes(next.payload))
	}
	if next.seq != 0 {
		t.Errorf("新轮头页 seq = %d, 期望从 0 重计", next.seq)
	}
	if next.serial == pages[0].serial {
		t.Error("新轮 serial 与旧轮相同, 期望换新")
	}
	if finishes != 1 || stops != 1 {
		t.Errorf("Finish = %d（期望 1：新轮收尾）, Stop = %d（期望 1：打断）", finishes, stops)
	}
	got := out.String()
	if !strings.Contains(got, "字幕: 好，再讲一个") {
		t.Errorf("新轮字幕应正常打印, 实际输出:\n%s", got)
	}
	// 状态回正：新轮 stop 后回待唤醒。
	if n := strings.Count(got, "待唤醒> \n"); n < 2 {
		t.Errorf("新轮收尾后应回待唤醒态（至少 2 次）, 实际 %d 次:\n%s", n, got)
	}

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Session.Run 返回错误: %v", err)
	}
}

// countPages 汇总播放器当前收到的 Ogg 页数。
func countPages(t *testing.T, player *fakePlayer) int {
	t.Helper()
	frames, _, _ := player.snapshot()
	var all bytes.Buffer
	for _, f := range frames {
		all.Write(f)
	}
	return len(parseOggPages(t, all.Bytes()))
}

// firstBytes 安全取载荷前 8 字节（不足则全取）。
func firstBytes(b []byte) []byte {
	if len(b) > 8 {
		return b[:8]
	}
	return b
}
