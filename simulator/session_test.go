package simulator

import (
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

// fakePlayer 假播放器：记录收到的 opus 帧与 Stop 次数。
type fakePlayer struct {
	mu     sync.Mutex
	frames [][]byte
	stops  int
}

func (p *fakePlayer) Play(frame []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.frames = append(p.frames, append([]byte(nil), frame...))
}

func (p *fakePlayer) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stops++
}

func (p *fakePlayer) snapshot() (frames [][]byte, stops int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([][]byte(nil), p.frames...), p.stops
}

// fakeServer 进程内假 server：应答 hello、记录收到的文本帧与二进制帧。
type fakeServer struct {
	t         *testing.T
	srv       *httptest.Server
	wsURL     string
	sessionID string

	mu       sync.Mutex
	query    url.Values
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
		fs.mu.Unlock()
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
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
				if err := conn.WriteJSON(resp); err != nil {
					t.Errorf("假 server 发送 hello 应答失败: %v", err)
				}
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
	if got, _ := features["mcp"].(bool); got {
		t.Errorf("hello features.mcp = true, 本票应为 false（#5 接管）")
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
