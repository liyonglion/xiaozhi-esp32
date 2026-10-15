package sim

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// sim#6 断线重连：server 主动断开或 WS 中途错误 → 回待唤醒态、REPL 不崩、一行提示（含原因）；
// 下次唤醒用缓存 url/token 重连（完整重走 hello/session_id 握手），不重跑 OTA；重连失败可再试。

// server 主动 close → 提示可见且回待唤醒态 → 再次唤醒用同一 url 重连、hello 重走、
// session_id 换新并回填后续消息；重连握手是全新会话，hello 不携带旧 session_id。
func TestSessionServerCloseThenWakeReconnects(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	out := &lockedBuffer{}
	sess := newTestSession(t, fs, out, nil, &fakePlayer{}, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	if _, err := io.WriteString(stdin, "小智\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	fs.nextReceived(t)                                      // hello #1
	wantDetect(t, fs.nextReceived(t), "你好小智", fs.sessionID) // 唤醒上报

	// server 主动断开（模拟 120s 无活动告别后的 close）。
	fs.closeCurrentConn(t)
	eventually(t, func() bool { return strings.Contains(out.String(), "与服务器的连接已断开") })

	// REPL 存活：再次唤醒触发重连，hello 完整重走且不带旧 session_id。
	if _, err := io.WriteString(stdin, "小智\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	hello2 := fs.nextReceived(t)
	wantString(t, hello2, "type", "hello")
	if _, has := hello2["session_id"]; has {
		t.Errorf("重连的 hello 不应携带 session_id（全新握手）, 实际: %v", hello2)
	}
	// 重连后唤醒上报回填新连接下发的 session_id。
	wantDetect(t, fs.nextReceived(t), "你好小智", fs.sessionIDForConn(2))

	// 用同一 url 重连（两次连接请求 URL 一致，含查询参数）。
	if got := fs.dialCount(t); got != 2 {
		t.Errorf("连接请求次数 = %d, 期望 2（初次 + 重连）", got)
	}
	if u1, u2 := fs.dialURL(t, 0), fs.dialURL(t, 1); u1 != u2 {
		t.Errorf("重连应使用同一 url, 初次 %q ≠ 重连 %q", u1, u2)
	}

	// 断线提示含原因、其后回到待唤醒提示符。
	got := out.String()
	at := strings.Index(got, "与服务器的连接已断开")
	if next := strings.Index(got[at:], "待唤醒> \n"); next < 0 {
		t.Errorf("断线提示后应回到待唤醒提示符, 实际输出:\n%s", got)
	}

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("重连后 exit 应干净退出, 实际: %v", err)
	}
}

// 重连失败（server 拒连）：提示后回待唤醒态，可再次尝试，REPL 全程存活。
func TestSessionReconnectFailureStaysIdleAndRetryable(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	out := &lockedBuffer{}
	sess := newTestSession(t, fs, out, nil, &fakePlayer{}, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	fs.nextReceived(t) // 等初次连接完成（hello）

	// 关停假 server：既有连接被断 + 后续拨号被拒。
	// httptest 的 Close 不动已劫持的 WS 连接，需另行显式断开。
	fs.srv.Close()
	fs.closeCurrentConn(t)
	eventually(t, func() bool { return strings.Contains(out.String(), "与服务器的连接已断开") })

	// 两次唤醒 → 两次重连失败：每次提示后仍处待唤醒态，可再试。
	for i := 0; i < 2; i++ {
		if _, err := io.WriteString(stdin, "小智\n"); err != nil {
			t.Fatalf("写入脚本输入失败: %v", err)
		}
		eventually(t, func() bool {
			return strings.Count(out.String(), "重连失败") >= i+1
		})
	}
	got := out.String()
	at := strings.Index(got, "重连失败")
	if next := strings.Index(got[at:], "待唤醒> \n"); next < 0 {
		t.Errorf("重连失败提示后应回到待唤醒提示符, 实际输出:\n%s", got)
	}

	// REPL 仍存活：exit 干净退出。
	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("重连失败后 exit 应干净退出, 实际: %v", err)
	}
}

// 断线清理须一并清 aborting：该标记只对当前连接的旧轮音频有意义，
// 跨连接存活会把重连后新会话的音频误当在途旧轮丢弃。
func TestSessionHandleDisconnectClearsAborting(t *testing.T) {
	sess := NewSession(Config{URL: "ws://unused"}, strings.NewReader("exit\n"), &lockedBuffer{}, nil, &fakePlayer{})
	sess.mu.Lock()
	sess.aborting = true // 打断已发出、tts stop 确认未到时连接断开
	sess.state = StateSpeaking
	sess.mu.Unlock()

	sess.handleDisconnect(errors.New("connection reset"))

	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.aborting {
		t.Error("断线后 aborting 应被清理，不应跨连接存活")
	}
	if sess.state != StateIdle {
		t.Errorf("断线后状态 = %v, 期望回待唤醒态", sess.state)
	}
	if !sess.connDead {
		t.Error("断线后 connDead 应置位（下次唤醒触发重连）")
	}
}

// OTA 路径的重连：断线后再次唤醒自动重连，但 OTA 检查不重跑（计数保持 1），
// 且缓存的 token 在重连拨号上继续携带。
func TestBootReconnectReusesOTAEndpointWithoutRecheck(t *testing.T) {
	fs := newFakeServer(t)
	ota := newFakeOTA(t, func(count int) (int, string) {
		return http.StatusOK, otaResponseBody(fs.wsURL, "tok-123", false)
	})
	stdinR, stdin := scriptStdin(t)
	out := &lockedBuffer{}
	done := make(chan error, 1)
	go func() {
		done <- Boot(Config{DeviceID: "de:ad:be:ef:ca:fe", ClientID: "0f3a7c2e-9b41-4d8a-a6d5-3e1f2b4c5d6e"},
			OTAConfig{OTAURL: ota.srv.URL, Backoff: time.Millisecond}, stdinR, out, nil, nil)
	}()

	if _, err := io.WriteString(stdin, "小智\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	fs.nextReceived(t)                                      // hello #1（OTA 下发的 url）
	wantDetect(t, fs.nextReceived(t), "你好小智", fs.sessionID) // 唤醒上报
	if got := fs.authorization(t); got != "Bearer tok-123" {
		t.Errorf("初次连接 Authorization = %q, 期望 %q", got, "Bearer tok-123")
	}

	fs.closeCurrentConn(t)
	eventually(t, func() bool { return strings.Contains(out.String(), "与服务器的连接已断开") })

	if _, err := io.WriteString(stdin, "小智\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	fs.nextReceived(t)                                                // hello #2（重连）
	wantDetect(t, fs.nextReceived(t), "你好小智", fs.sessionIDForConn(2)) // 唤醒上报回填新 session_id
	if got := fs.authorization(t); got != "Bearer tok-123" {
		t.Errorf("重连应复用 OTA 下发的缓存 token, Authorization = %q, 期望 %q", got, "Bearer tok-123")
	}

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Boot 返回错误: %v", err)
	}
	if got := ota.count(); got != 1 {
		t.Errorf("重连不应重跑 OTA 检查（期望 1 个请求）, 实收 %d 个", got)
	}
}
