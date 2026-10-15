package sim

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// —— 测试缝合件（本包唯一缝合点：注入式 Session）——

// lockedBuffer 并发安全的输出缓冲，用于断言终端输出。
type lockedBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// fakePlayer 假播放器：记录收到的帧、Finish 与 Stop 次数。
type fakePlayer struct {
	mu       sync.Mutex
	frames   [][]byte
	finishes int
	stops    int
}

func (p *fakePlayer) Play(frame []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.frames = append(p.frames, append([]byte(nil), frame...))
}

func (p *fakePlayer) Finish() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.finishes++
}

func (p *fakePlayer) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stops++
}

func (p *fakePlayer) snapshot() (frames [][]byte, finishes int, stops int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([][]byte(nil), p.frames...), p.finishes, p.stops
}

// fakeServer 进程内假 server：应答 hello、记录收到的文本帧与二进制帧。
type fakeServer struct {
	t         *testing.T
	srv       *httptest.Server
	wsURL     string
	sessionID string

	mu       sync.Mutex
	query    url.Values
	header   http.Header
	conn     *websocket.Conn
	received chan map[string]any
}

func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	fs := &fakeServer{
		t:         t,
		sessionID: "test-session-id",
		received:  make(chan map[string]any, 32),
	}
	upgrader := websocket.Upgrader{}
	fs.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fs.mu.Lock()
		fs.query = r.URL.Query()
		fs.header = r.Header
		fs.mu.Unlock()
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		fs.mu.Lock()
		fs.conn = conn
		fs.mu.Unlock()
		defer conn.Close()
		for {
			msgType, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if msgType != websocket.TextMessage {
				continue
			}
			var msg map[string]any
			if err := json.Unmarshal(data, &msg); err != nil {
				t.Errorf("假 server 收到无法解析的文本帧: %v", err)
				continue
			}
			if msg["type"] == "hello" {
				resp := map[string]any{
					"type":         "hello",
					"version":      1,
					"transport":    "websocket",
					"audio_params": map[string]any{"format": "opus", "sample_rate": 24000, "channels": 1, "frame_duration": 60},
					"session_id":   fs.sessionID,
				}
				writeJSON(t, conn, resp)
			}
			select {
			case fs.received <- msg:
			case <-time.After(2 * time.Second):
				t.Errorf("假 server 消息队列已满，测试未及时消费")
			}
		}
	}))
	t.Cleanup(fs.srv.Close)
	fs.wsURL = "ws" + strings.TrimPrefix(fs.srv.URL, "http")
	return fs
}

// writeJSON 用 Marshal+WriteMessage 下发，模拟真实 server 的线上格式
// （json.dumps 无尾随换行；gorilla 的 WriteJSON 会多带一个 \n）。
func writeJSON(t *testing.T, conn *websocket.Conn, msg map[string]any) {
	t.Helper()
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("假 server 序列化消息失败: %v", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatalf("假 server 下发消息失败: %v", err)
	}
}

// send 从 server 侧下发一条文本帧。
func (fs *fakeServer) send(t *testing.T, msg map[string]any) {
	t.Helper()
	fs.mu.Lock()
	conn := fs.conn
	fs.mu.Unlock()
	if conn == nil {
		t.Fatal("假 server 尚未完成 WebSocket 升级")
	}
	writeJSON(t, conn, msg)
}

// sendBinary 从 server 侧下发一条二进制帧（模拟 opus 包）。
func (fs *fakeServer) sendBinary(t *testing.T, data []byte) {
	t.Helper()
	fs.mu.Lock()
	conn := fs.conn
	fs.mu.Unlock()
	if conn == nil {
		t.Fatal("假 server 尚未完成 WebSocket 升级")
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		t.Fatalf("假 server 下发二进制帧失败: %v", err)
	}
}

// nextReceived 等待下一条 server 收到的文本帧，超时失败。
func (fs *fakeServer) nextReceived(t *testing.T) map[string]any {
	t.Helper()
	select {
	case msg := <-fs.received:
		return msg
	case <-time.After(2 * time.Second):
		t.Fatal("超时：假 server 未收到预期消息")
		return nil
	}
}

func (fs *fakeServer) queryValues(t *testing.T) url.Values {
	t.Helper()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.query == nil {
		t.Fatal("假 server 尚未收到连接请求")
	}
	return fs.query
}

// authorization 返回升级请求携带的 Authorization 头（未连接则失败）。
func (fs *fakeServer) authorization(t *testing.T) string {
	t.Helper()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.header == nil {
		t.Fatal("假 server 尚未收到连接请求")
	}
	return fs.header.Get("Authorization")
}

// scriptStdin 脚本化 stdin：返回读端（喂给 Session）与写端（测试按节奏写入输入行）。
func scriptStdin(t *testing.T) (io.Reader, io.WriteCloser) {
	t.Helper()
	pr, pw := io.Pipe()
	t.Cleanup(func() { pw.Close() })
	return pr, pw
}

func newTestSession(t *testing.T, fs *fakeServer, out io.Writer, verbose io.Writer, player Player, in io.Reader) *Session {
	t.Helper()
	cfg := Config{
		URL:      fs.wsURL,
		DeviceID: "de:ad:be:ef:ca:fe",
		ClientID: "0f3a7c2e-9b41-4d8a-a6d5-3e1f2b4c5d6e",
	}
	return NewSession(cfg, in, out, verbose, player)
}

// eventually 轮轮询等待条件成立，超时失败。
func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("超时：条件未在期限内成立")
}

// —— 断言辅助 ——

func wantString(t *testing.T, msg map[string]any, field, want string) {
	t.Helper()
	if got, _ := msg[field].(string); got != want {
		t.Errorf("消息 %s = %q, 期望 %q（完整消息: %v）", field, got, want, msg)
	}
}

func wantDetect(t *testing.T, msg map[string]any, text, sessionID string) {
	t.Helper()
	wantString(t, msg, "type", "listen")
	wantString(t, msg, "state", "detect")
	wantString(t, msg, "text", text)
	wantString(t, msg, "session_id", sessionID)
}

// —— 测试 ——

// 直连对话主链路：hello 握手结构正确、唤醒上报用规范唤醒词、
// 查询走 listen detect、session_id 回填所有后续 JSON。
func TestSessionHelloHandshakeAndWakeFlow(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	out := &lockedBuffer{}
	player := &fakePlayer{}

	sess := newTestSession(t, fs, out, nil, player, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	// 待唤醒态打「小智，你好」→ 唤醒上报 + 查询 detect。
	if _, err := io.WriteString(stdin, "小智，你好\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}

	hello := fs.nextReceived(t)
	wantString(t, hello, "type", "hello")
	wantString(t, hello, "transport", "websocket")
	if got, _ := hello["version"].(float64); got != 1 {
		t.Errorf("hello version = %v, 期望 1", hello["version"])
	}
	audio, _ := hello["audio_params"].(map[string]any)
	if audio == nil {
		t.Fatalf("hello 缺少 audio_params: %v", hello)
	}
	for field, want := range map[string]any{"format": "opus", "sample_rate": float64(16000), "channels": float64(1), "frame_duration": float64(60)} {
		if got := audio[field]; got != want {
			t.Errorf("hello audio_params.%s = %v, 期望 %v", field, got, want)
		}
	}
	features, _ := hello["features"].(map[string]any)
	if features == nil {
		t.Fatalf("hello 缺少 features: %v", hello)
	}
	if got, _ := features["mcp"].(bool); !got {
		t.Errorf("hello features.mcp = false, 期望 true（声明支持 MCP，server 随即下发 initialize）")
	}
	if got, _ := features["aec"].(bool); got {
		t.Errorf("hello features.aec = true, 期望 false")
	}

	wantDetect(t, fs.nextReceived(t), "你好小智", fs.sessionID)
	wantDetect(t, fs.nextReceived(t), "你好", fs.sessionID)

	// 连接 URL 必须携带 device-id / client-id 查询参数。
	q := fs.queryValues(t)
	if q.Get("device-id") != "de:ad:be:ef:ca:fe" {
		t.Errorf("连接查询参数 device-id = %q, 期望模拟 MAC 形态", q.Get("device-id"))
	}
	if q.Get("client-id") == "" {
		t.Error("连接查询参数 client-id 为空")
	}

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Session.Run 返回错误: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("超时：exit 后 Session 未退出")
	}
}

// 两段式输入：先「小智」唤醒进入待输入态，下一行整行作为查询发送。
func TestSessionTwoStageInput(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	out := &lockedBuffer{}
	sess := newTestSession(t, fs, out, nil, &fakePlayer{}, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	for _, line := range []string{"小智\n", "我想听故事\n", "exit\n"} {
		if _, err := io.WriteString(stdin, line); err != nil {
			t.Fatalf("写入脚本输入失败: %v", err)
		}
	}

	// hello 之后的设备消息：唤醒上报 + 两段式的查询。
	fs.nextReceived(t) // hello
	wantDetect(t, fs.nextReceived(t), "你好小智", fs.sessionID)
	wantDetect(t, fs.nextReceived(t), "我想听故事", fs.sessionID)

	if err := waitDone(t, done); err != nil {
		t.Fatalf("Session.Run 返回错误: %v", err)
	}
}

// 待唤醒态收到无唤醒词前缀的行：给提示而非静默，也不向 server 发任何 listen 消息。
func TestSessionIdleLineWithoutWakeWordGetsHint(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	out := &lockedBuffer{}
	sess := newTestSession(t, fs, out, nil, &fakePlayer{}, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	if _, err := io.WriteString(stdin, "今天天气怎么样\nexit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}

	fs.nextReceived(t) // hello

	if err := waitDone(t, done); err != nil {
		t.Fatalf("Session.Run 返回错误: %v", err)
	}
	// Run 已返回，所有输入处理完毕：不应再收到任何 listen 消息。
	select {
	case msg := <-fs.received:
		t.Errorf("待唤醒态不应发送消息, 却收到: %v", msg)
	default:
	}
	if got := out.String(); !strings.Contains(got, "未检测到唤醒词") {
		t.Errorf("终端输出应包含无唤醒词提示, 实际输出:\n%s", got)
	}
}

// stdin EOF（Ctrl-D）干净退出。
func TestSessionEOFExitsCleanly(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	sess := newTestSession(t, fs, &lockedBuffer{}, nil, &fakePlayer{}, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	if err := stdin.Close(); err != nil {
		t.Fatalf("关闭脚本 stdin 失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("EOF 后 Session.Run 应返回 nil, 实际: %v", err)
	}
}

// waitDone 等待 Session.Run 结束，超时失败。
func waitDone(t *testing.T, done chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("超时：Session 未退出")
		return nil
	}
}

// server 下发一轮完整回复：stt/情绪/字幕带前缀打印，
// tts start/stop 驱动状态提示符（待输入→播放中→待唤醒），opus 帧转发给播放器。
func TestSessionServerReplyDrivesPromptsAndPrinting(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	out := &lockedBuffer{}
	player := &fakePlayer{}
	sess := newTestSession(t, fs, out, nil, player, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	if _, err := io.WriteString(stdin, "小智，你好\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	fs.nextReceived(t)                                      // hello
	wantDetect(t, fs.nextReceived(t), "你好小智", fs.sessionID) // 唤醒上报
	wantDetect(t, fs.nextReceived(t), "你好", fs.sessionID)   // 查询

	opusFrame := []byte{0x00, 0x01, 0x02, 0x03}
	fs.send(t, map[string]any{"type": "stt", "text": "小智，你好"})
	fs.send(t, map[string]any{"type": "tts", "state": "start"})
	fs.send(t, map[string]any{"type": "llm", "text": "😊", "emotion": "happy"})
	fs.send(t, map[string]any{"type": "tts", "state": "sentence_start", "text": "嘿，你好呀"})
	fs.sendBinary(t, opusFrame)
	fs.send(t, map[string]any{"type": "tts", "state": "stop"})

	eventually(t, func() bool {
		got := out.String()
		return strings.Contains(got, "stt: 小智，你好") &&
			strings.Contains(got, "情绪: happy 😊") &&
			strings.Contains(got, "字幕: 嘿，你好呀") &&
			// 两次待唤醒：初始一次 + tts stop 后一次（同步点）。
			strings.Count(got, "待唤醒> \n") >= 2
	})

	got := out.String()
	// 提示符顺序：待唤醒 → 待输入 → 播放中 → 待唤醒。
	at := strings.Index(got, "待唤醒> \n")
	for _, want := range []string{"待输入> \n", "播放中> \n", "待唤醒> \n"} {
		next := strings.Index(got[at+1:], want)
		if next < 0 {
			t.Fatalf("提示符顺序不符，%q 未出现在 %q 之后，实际输出:\n%s", want, got[at:], got)
		}
		at += 1 + next
	}

	// 二进制 opus 帧应以 Ogg 页到达播放器，tts stop 触发 Finish。
	frames, finishes, stops := player.snapshot()
	var stream bytes.Buffer
	for _, f := range frames {
		stream.Write(f)
	}
	pages := parseOggPages(t, stream.Bytes())
	var gotPacket bool
	for _, pg := range pages {
		if bytes.Equal(pg.payload, opusFrame) {
			gotPacket = true
		}
	}
	if !gotPacket {
		t.Errorf("播放器收到的 Ogg 流应含原样 opus 包 %v, 实际 %d 页", opusFrame, len(pages))
	}
	if finishes < 1 {
		t.Error("tts stop 后播放器 Finish 未被调用")
	}
	if stops != 0 {
		t.Errorf("正常收尾不应调用 Stop（打断专用）, 实际 %d 次", stops)
	}

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Session.Run 返回错误: %v", err)
	}
}

// --verbose 打印收发双方的原始 JSON 帧。
func TestSessionVerboseLogsRawFrames(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	verbose := &lockedBuffer{}
	sess := newTestSession(t, fs, &lockedBuffer{}, verbose, &fakePlayer{}, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	if _, err := io.WriteString(stdin, "小智\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	fs.nextReceived(t)                                      // hello
	wantDetect(t, fs.nextReceived(t), "你好小智", fs.sessionID) // 唤醒上报
	fs.send(t, map[string]any{"type": "stt", "text": "小智"})
	eventually(t, func() bool { return strings.Contains(verbose.String(), `"type":"stt"`) })

	// 逐行解析 verbose 输出：方向 + 可解析 JSON。
	sent, received := 0, 0
	for _, line := range strings.Split(strings.TrimSpace(verbose.String()), "\n") {
		var dir string
		var raw string
		switch {
		case strings.HasPrefix(line, "→ "):
			dir, raw = "→", strings.TrimPrefix(line, "→ ")
		case strings.HasPrefix(line, "← "):
			dir, raw = "←", strings.TrimPrefix(line, "← ")
		default:
			t.Errorf("verbose 行缺少方向前缀: %q", line)
			continue
		}
		var msg map[string]any
		if err := json.Unmarshal([]byte(raw), &msg); err != nil {
			t.Errorf("verbose 行不是合法 JSON: %q", line)
			continue
		}
		if dir == "→" {
			sent++
		} else {
			received++
			if msg["type"] == "hello" && msg["session_id"] != fs.sessionID {
				t.Errorf("verbose 下行 hello 的 session_id = %v, 期望 %s", msg["session_id"], fs.sessionID)
			}
		}
	}
	if sent < 2 { // 至少：发出的 hello + 唤醒上报
		t.Errorf("verbose 应记录至少 2 条上行帧, 实际 %d:\n%s", sent, verbose.String())
	}
	if received < 2 { // 至少：hello 应答 + stt
		t.Errorf("verbose 应记录至少 2 条下行帧, 实际 %d:\n%s", received, verbose.String())
	}

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Session.Run 返回错误: %v", err)
	}
}

// 两轮 TTS：注入的播放器收到的是 Ogg Opus 字节流——魔数、前两页
// OpusHead/OpusTags、granuleposition 48kHz 基准每包 +2880、tts stop 收尾、
// 下一轮重发起播（头页重发、serial 更换、granule 重置）。
func TestSessionTTSRoundsRestartPlayerStream(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	player := &fakePlayer{}
	sess := newTestSession(t, fs, &lockedBuffer{}, nil, player, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	if _, err := io.WriteString(stdin, "小智，讲个故事\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	fs.nextReceived(t)                                      // hello
	wantDetect(t, fs.nextReceived(t), "你好小智", fs.sessionID) // 唤醒上报
	wantDetect(t, fs.nextReceived(t), "讲个故事", fs.sessionID) // 查询

	// 第一轮：2 个 opus 包。
	fs.send(t, map[string]any{"type": "tts", "state": "start"})
	fs.sendBinary(t, []byte{0x11})
	fs.sendBinary(t, []byte{0x22})
	fs.send(t, map[string]any{"type": "tts", "state": "stop"})
	// 第二轮：1 个 opus 包。
	fs.send(t, map[string]any{"type": "tts", "state": "start"})
	fs.sendBinary(t, []byte{0x33})
	fs.send(t, map[string]any{"type": "tts", "state": "stop"})

	eventually(t, func() bool {
		_, finishes, _ := player.snapshot()
		return finishes == 2
	})

	frames, finishes, stops := player.snapshot()
	var stream bytes.Buffer
	for _, f := range frames {
		stream.Write(f)
	}
	pages := parseOggPages(t, stream.Bytes())
	if len(pages) != 7 { // 轮 1：头 2 页+音频 2 页；轮 2：头 2 页+音频 1 页
		t.Fatalf("两轮应共 7 页, 实际 %d", len(pages))
	}
	for i, idx := range []int{0, 4} {
		pg := pages[idx]
		if string(pg.magic) != "OggS" {
			t.Errorf("第 %d 轮首帧魔数 = %q, 期望 OggS", i+1, pg.magic)
		}
		if !bytes.HasPrefix(pg.payload, []byte("OpusHead")) {
			t.Errorf("第 %d 轮首页应为 OpusHead, 实际 %q", i+1, pg.payload[:8])
		}
		if !bytes.HasPrefix(pages[idx+1].payload, []byte("OpusTags")) {
			t.Errorf("第 %d 轮第二页应为 OpusTags, 实际 %q", i+1, pages[idx+1].payload[:8])
		}
		if pg.seq != 0 {
			t.Errorf("第 %d 轮头页 seq = %d, 期望每轮从 0 重计", i+1, pg.seq)
		}
	}
	// granuleposition：轮 1 递增 2880/包，轮 2 重置后再递增。
	for i, want := range []uint64{2880, 5760, 2880} {
		audio := []int{2, 3, 6}[i]
		if pages[audio].granule != want {
			t.Errorf("音频页 %d granule = %d, 期望 %d", audio, pages[audio].granule, want)
		}
	}
	if pages[0].serial == pages[4].serial {
		t.Error("两轮流 serial 相同, 期望每轮随机区分")
	}
	if !bytes.Equal(pages[2].payload, []byte{0x11}) || !bytes.Equal(pages[3].payload, []byte{0x22}) || !bytes.Equal(pages[6].payload, []byte{0x33}) {
		t.Error("音频页载荷与 server 下发的包不一致")
	}
	if finishes != 2 || stops != 0 {
		t.Errorf("Finish = %d（期望 2）, Stop = %d（期望 0，打断专用）", finishes, stops)
	}

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Session.Run 返回错误: %v", err)
	}
}
