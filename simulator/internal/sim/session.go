package sim

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WakeReportText 唤醒上报的规范唤醒词，必须匹配服务端 wakeup_words 配置（见 GLOSSARY.md）。
const WakeReportText = "你好小智"

// Config 模拟器连接配置。
type Config struct {
	URL      string // WebSocket 端点，如 ws://127.0.0.1:8000/xiaozhi/v1/
	DeviceID string // 模拟 MAC 形态设备标识
	ClientID string // 固定 UUID 客户端标识
	Token    string // OTA 下发的访问令牌；非空时连接携带 Authorization: Bearer
}

// State 模拟器 REPL 状态（术语见 GLOSSARY.md：待输入态/播放态）。
type State int

const (
	StateIdle      State = iota // 待唤醒：仅接受唤醒词
	StateListening              // 待输入：任意行作为查询发送
	StateSpeaking               // 播放中（本票为文字版）：server 正在下发 TTS
)

func (st State) prompt() string {
	switch st {
	case StateListening:
		return "待输入> "
	case StateSpeaking:
		return "播放中> "
	default:
		return "待唤醒> "
	}
}

// Player 是 TTS 音频出口。本票默认实现丢弃 opus 帧（#3 接管换 ffplay 流式播放）。
type Player interface {
	Play(frame []byte)
	Stop()
}

// discardPlayer 默认播放器：丢弃所有帧。
type discardPlayer struct{}

func (discardPlayer) Play([]byte) {}
func (discardPlayer) Stop()       {}

// Session 会话核心：持有 WS 连接与 REPL 循环。
// stdin/stdout/verbose/播放器均为注入点，是全项目唯一测试缝合点。
type Session struct {
	cfg     Config
	in      io.Reader
	out     io.Writer
	verbose io.Writer
	player  Player

	mu        sync.Mutex // 保护 out/verbose/state/sessionID 的并发写
	state     State
	sessionID string
	closing   bool

	conn *websocket.Conn
	wg   sync.WaitGroup
}

// NewSession 构造会话。verbose 为 nil 时关闭原始帧日志。
func NewSession(cfg Config, in io.Reader, out io.Writer, verbose io.Writer, player Player) *Session {
	if player == nil {
		player = discardPlayer{}
	}
	return &Session{cfg: cfg, in: in, out: out, verbose: verbose, player: player}
}

// Run 建立连接、完成 hello 握手并进入 REPL，直到 exit/quit 或 stdin EOF。
func (s *Session) Run() error {
	wsURL, err := s.buildURL()
	if err != nil {
		return err
	}
	// token 非空才带 Authorization 头（auth 关闭的部署 token 为空串）。
	var hdr http.Header
	if s.cfg.Token != "" {
		hdr = http.Header{"Authorization": []string{"Bearer " + s.cfg.Token}}
	}
	conn, resp, err := (&websocket.Dialer{HandshakeTimeout: 10 * time.Second}).Dial(wsURL, hdr)
	if err != nil {
		if resp != nil {
			return fmt.Errorf("连接服务器失败（%s，HTTP %d）: %w", wsURL, resp.StatusCode, err)
		}
		return fmt.Errorf("连接服务器失败（%s）: %w — 确认 server 已启动，或用 --url 指定地址", wsURL, err)
	}
	s.conn = conn

	if err := s.handshake(); err != nil {
		conn.Close()
		return err
	}

	s.wg.Add(1)
	go s.readLoop()

	err = s.repl()

	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	conn.Close()
	s.wg.Wait()
	return err
}

// buildURL 在端点上追加 device-id / client-id 查询参数。
func (s *Session) buildURL() (string, error) {
	u, err := url.Parse(s.cfg.URL)
	if err != nil {
		return "", fmt.Errorf("无效的服务器地址 %q: %w", s.cfg.URL, err)
	}
	q := u.Query()
	q.Set("device-id", s.cfg.DeviceID)
	q.Set("client-id", s.cfg.ClientID)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// handshake 发送 hello 并等待应答，取出 session_id 供后续消息回填。
func (s *Session) handshake() error {
	hello := map[string]any{
		"type":      "hello",
		"version":   1,
		"transport": "websocket",
		"audio_params": map[string]any{
			"format":         "opus",
			"sample_rate":    16000,
			"channels":       1,
			"frame_duration": 60,
		},
		"features": map[string]any{"mcp": false, "aec": false},
	}
	if err := s.sendJSON(hello); err != nil {
		return fmt.Errorf("发送 hello 失败: %w", err)
	}

	s.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	msgType, data, err := s.conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("等待 hello 应答失败（10 秒超时）: %w", err)
	}
	s.conn.SetReadDeadline(time.Time{})
	if msgType != websocket.TextMessage {
		return fmt.Errorf("hello 应答不是文本帧（got %d）", msgType)
	}
	s.logFrame("←", data)
	var resp map[string]any
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("解析 hello 应答失败: %w", err)
	}
	if resp["type"] != "hello" {
		return fmt.Errorf("hello 应答 type = %v, 期望 hello", resp["type"])
	}
	sessionID, _ := resp["session_id"].(string)
	if sessionID == "" {
		return fmt.Errorf("hello 应答缺少 session_id: %s", data)
	}
	s.mu.Lock()
	s.sessionID = sessionID
	s.mu.Unlock()
	return nil
}

// repl 读取输入行直到 exit/quit 或 EOF。
func (s *Session) repl() error {
	scanner := bufio.NewScanner(s.in)
	s.showPrompt()
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			s.showPrompt()
			continue
		}
		if line == "exit" || line == "quit" {
			return nil
		}
		s.handleInput(line)
		s.showPrompt()
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("读取输入失败: %w", err)
	}
	return nil // EOF（Ctrl-D）
}

// handleInput 按当前状态处理一行输入。
func (s *Session) handleInput(line string) {
	query, woke := ParseWake(line)
	if woke {
		if err := s.sendDetect(WakeReportText); err != nil {
			s.reportSendError("唤醒上报", err)
			return
		}
		if query != "" {
			if err := s.sendDetect(query); err != nil {
				s.reportSendError("查询", err)
				return
			}
		}
		s.setState(StateListening)
		return
	}
	if s.state == StateIdle {
		s.printf("（提示）未检测到唤醒词：请以「小智」或「你好小智」开头；输入 exit 退出\n")
		return
	}
	if err := s.sendDetect(line); err != nil {
		s.reportSendError("查询", err)
	}
}

// readLoop 消费 server 下发帧：文本帧分发打印，二进制 opus 帧交给播放器。
func (s *Session) readLoop() {
	defer s.wg.Done()
	for {
		msgType, data, err := s.conn.ReadMessage()
		if err != nil {
			s.mu.Lock()
			closing := s.closing
			s.mu.Unlock()
			if !closing {
				s.printf("与服务器的连接已断开: %v（输入 exit 退出）\n", err)
			}
			return
		}
		switch msgType {
		case websocket.BinaryMessage:
			s.player.Play(data)
		case websocket.TextMessage:
			s.logFrame("←", data)
			s.handleServerText(data)
		}
	}
}

// handleServerText 分发 server 文本帧并打印（stt:/情绪:/字幕: 前缀）。
func (s *Session) handleServerText(data []byte) {
	var msg map[string]any
	if err := json.Unmarshal(data, &msg); err != nil {
		s.printf("（忽略）无法解析的消息: %s\n", data)
		return
	}
	switch msg["type"] {
	case "stt":
		text, _ := msg["text"].(string)
		s.printf("stt: %s\n", text)
	case "llm":
		text, _ := msg["text"].(string)
		emotion, _ := msg["emotion"].(string)
		s.printf("情绪: %s %s\n", emotion, text)
	case "tts":
		s.handleTTS(msg)
	case "hello":
		// 握手已在 Run 内处理，忽略重复 hello。
	default:
		s.printf("其他消息: %s\n", data)
	}
}

// handleTTS 处理 tts start/sentence_start/stop，驱动状态提示符。
func (s *Session) handleTTS(msg map[string]any) {
	switch msg["state"] {
	case "start":
		s.setState(StateSpeaking)
		s.showPrompt() // REPL 正阻塞在输入上，立即刷新提示符
	case "sentence_start":
		text, _ := msg["text"].(string)
		s.printf("字幕: %s\n", text)
	case "stop":
		s.player.Stop()
		s.setState(StateIdle)
		s.showPrompt()
	}
}

// setState 更新状态；来自 server 事件的状态变化立即刷新提示符。
func (s *Session) setState(next State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = next
}

// sendDetect 发送 listen detect 消息（唤醒上报与查询共用），回填 session_id。
func (s *Session) sendDetect(text string) error {
	return s.sendJSON(map[string]any{
		"type":  "listen",
		"state": "detect",
		"text":  text,
	})
}

// sendJSON 序列化并写文本帧；握手后的所有消息回填 session_id。
func (s *Session) sendJSON(msg map[string]any) error {
	s.mu.Lock()
	if s.sessionID != "" {
		msg["session_id"] = s.sessionID
	}
	s.mu.Unlock()
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化 %v 消息失败: %w", msg["type"], err)
	}
	if err := s.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("发送 %v 消息失败: %w", msg["type"], err)
	}
	s.logFrame("→", data)
	return nil
}

// —— 输出辅助（统一加锁，避免 REPL 与读循环交错）——

func (s *Session) printf(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Fprintf(s.out, format, args...)
}

// showPrompt 打印当前状态提示符（独立成行）。
func (s *Session) showPrompt() {
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Fprintf(s.out, "%s\n", s.state.prompt())
}

func (s *Session) reportSendError(what string, err error) {
	s.printf("发送%s失败: %v — 连接可能已断开，输入 exit 退出\n", what, err)
}

// logFrame 在 --verbose 下打印原始收发帧。
func (s *Session) logFrame(dir string, data []byte) {
	if s.verbose == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Fprintf(s.verbose, "%s %s\n", dir, data)
}
