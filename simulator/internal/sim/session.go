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

// Player 是 TTS 音频出口。收到的是完整 Ogg Opus 封装字节流
// （由 session 默认装配的 oggPlayer 封装，见 player.go）。
type Player interface {
	Play(frame []byte)
	// Finish 一轮 TTS 结束：收尾（关 stdin、回收进程），让已缓冲音频播完。
	Finish()
	// Stop 立即停（打断：播放态唤醒时杀进程，不等余量播完）。
	Stop()
}

// Session 会话核心：持有 WS 连接与 REPL 循环。
// stdin/stdout/verbose/播放器均为注入点，是全项目唯一测试缝合点。
type Session struct {
	cfg     Config
	in      io.Reader
	out     io.Writer
	verbose io.Writer
	player  Player

	mu        sync.Mutex // 保护 out/verbose/state/sessionID/conn/connDead 的并发写
	state     State
	sessionID string
	closing   bool
	connDead  bool // readLoop 出错后置位：下次唤醒前需用缓存 url/token 重连
	aborting  bool // 打断待确认：丢弃在途旧轮音频，tts stop 确认或新 tts start 清除

	writeMu sync.Mutex // 串行化 WS 写：REPL 与 readLoop（MCP 应答）都可能发帧
	conn    *websocket.Conn
	wg      sync.WaitGroup
}

// NewSession 构造会话。verbose 为 nil 时关闭原始帧日志；
// player 为 nil 时默认装配 ffplay 流式播放器。注入的播放器均被
// Ogg Opus 封装层包装：Play 收到的是完整 Ogg 字节流。
func NewSession(cfg Config, in io.Reader, out io.Writer, verbose io.Writer, player Player) *Session {
	if player == nil {
		player = newFFplayPlayer(out, nil, nil)
	}
	return &Session{cfg: cfg, in: in, out: out, verbose: verbose, player: newOggPlayer(player)}
}

// Run 建立连接、完成 hello 握手并进入 REPL，直到 exit/quit 或 stdin EOF。
// 连接中途断开不退出 REPL：回待唤醒态，下次唤醒自动用缓存配置重连（不重跑 OTA）。
func (s *Session) Run() error {
	conn, err := s.connect()
	if err != nil {
		return err
	}
	s.wg.Add(1)
	go s.readLoop(conn)

	err = s.repl()

	s.mu.Lock()
	s.closing = true
	cur := s.conn
	s.mu.Unlock()
	if cur != nil {
		cur.Close() // 解除 readLoop 阻塞在 ReadMessage 的等待
	}
	s.wg.Wait()
	s.player.Stop() // 读循环已停：清理可能仍在播放的进程（exit/EOF 中途退出的情形）
	return err
}

// connect 用当前配置（url/token 来自启动时的 OTA 结果或 --url 直连）建连并完成 hello 握手。
// 重连走同一入口：hello 是全新会话握手，先清掉旧 session_id。
func (s *Session) connect() (*websocket.Conn, error) {
	wsURL, err := s.buildURL()
	if err != nil {
		return nil, err
	}
	// token 非空才带 Authorization 头（auth 关闭的部署 token 为空串）。
	var hdr http.Header
	if s.cfg.Token != "" {
		hdr = http.Header{"Authorization": []string{"Bearer " + s.cfg.Token}}
	}
	conn, resp, err := (&websocket.Dialer{HandshakeTimeout: 10 * time.Second}).Dial(wsURL, hdr)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("连接服务器失败（%s，HTTP %d）: %w", wsURL, resp.StatusCode, err)
		}
		return nil, fmt.Errorf("连接服务器失败（%s）: %w — 确认 server 已启动，或用 --url 指定地址", wsURL, err)
	}
	s.mu.Lock()
	s.sessionID = ""
	s.conn = conn
	s.mu.Unlock()
	if err := s.handshake(); err != nil {
		conn.Close()
		return nil, err
	}
	s.mu.Lock()
	s.connDead = false
	s.mu.Unlock()
	return conn, nil
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
		"features": map[string]any{"mcp": true, "aec": false},
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

// maxInputBytes 单行输入的长度上限。超出给提示并拒发（选择拒发而非截断：
// 截断后的半句话作为查询发给 server 更容易造成误解）。上限与旧
// bufio.Scanner 的 64KiB 一致——区别是超限不再崩掉整个 REPL。
const maxInputBytes = 64 << 10

// repl 读取输入行直到 exit/quit 或 EOF。
// 用 bufio.Reader 而非 Scanner：Scanner 的 64KiB 行长上限超限时直接报错
// 退出，粘贴超长行会崩 REPL；Reader 无行长上限，超长行在应用层拒发。
func (s *Session) repl() error {
	reader := bufio.NewReader(s.in)
	s.showPrompt()
	for {
		raw, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return fmt.Errorf("读取输入失败: %w", err)
		}
		if line := strings.TrimSpace(raw); line != "" {
			switch {
			case line == "exit" || line == "quit":
				return nil
			case len(line) > maxInputBytes:
				s.printf("（提示）输入行超过 %d KiB 上限，已忽略——请拆分后再发\n", maxInputBytes>>10)
			default:
				s.handleInput(line)
			}
		}
		if err == io.EOF {
			return nil // EOF（Ctrl-D），无换行的末行已在上方处理
		}
		s.showPrompt()
	}
}

// handleInput 按当前状态处理一行输入。
// 播放态的任意输入行（无论是否带唤醒词）都走打断路径：旧 Ogg 流未停时，
// 新旧两轮音频会挤进同一播放器。无唤醒词则只 abort + 查询，不发唤醒上报。
func (s *Session) handleInput(line string) {
	query, woke := ParseWake(line)
	if !woke && s.currentState() != StateSpeaking {
		if s.currentState() == StateIdle {
			s.printf("（提示）未检测到唤醒词：请以「小智」或「你好小智」开头；输入 exit 退出\n")
			return
		}
		if err := s.sendDetect(line); err != nil {
			s.reportSendError("查询", err)
		}
		return
	}
	// 唤醒输入，或播放态的任意输入行：复用唤醒/打断路径。
	if err := s.ensureConnected(); err != nil {
		s.printf("重连失败: %v — 已回待唤醒态，下次唤醒将再试\n", err)
		s.setState(StateIdle)
		return
	}
	was := s.currentState()
	// 打断（barge-in）：先关门再停播（此后到达的在途旧轮音频直接丢弃），
	// 再发 abort 让 server 结束本轮。
	if was == StateSpeaking {
		s.setAborting(true)
		s.player.Stop()
		if err := s.sendAbort(); err != nil {
			s.reportSendError("打断（abort）", err)
			return
		}
	}
	// 唤醒上报仅在输入行带唤醒词时发送；待输入态重复唤醒只重置状态不重复上报。
	if woke && was != StateListening {
		if err := s.sendDetect(WakeReportText); err != nil {
			s.reportSendError("唤醒上报", err)
			return
		}
	}
	if query != "" { // 无唤醒词时 query 即整行（播放态查询路径）
		if err := s.sendDetect(query); err != nil {
			s.reportSendError("查询", err)
			return
		}
	}
	s.setState(StateListening)
}

// sendAbort 发送打断消息；server 收到后回 tts stop 确认（见 handleTTS）。
func (s *Session) sendAbort() error {
	return s.sendJSON(map[string]any{
		"type":   "abort",
		"reason": "wake_word_detected",
	})
}

// ensureConnected 在连接已断时用缓存的 url/token 重连并完整重走 hello 握手
// （OTA 结果仅启动时获取一次，此处不重跑）；连接健康时直接返回。
func (s *Session) ensureConnected() error {
	s.mu.Lock()
	dead := s.connDead
	s.mu.Unlock()
	if !dead {
		return nil
	}
	conn, err := s.connect()
	if err != nil {
		return err
	}
	s.wg.Add(1)
	go s.readLoop(conn)
	return nil
}

// readLoop 消费 server 下发帧：文本帧分发打印，二进制 opus 帧交给播放器。
// 每个连接一个 readLoop；连接出错（server 主动断开/WS 中途错误）回待唤醒态并提示，
// 由下次唤醒触发重连。
func (s *Session) readLoop(conn *websocket.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			s.handleDisconnect(err)
			return
		}
		switch msgType {
		case websocket.BinaryMessage:
			if s.dropInflightOldRound() {
				continue
			}
			s.player.Play(data)
		case websocket.TextMessage:
			s.logFrame("←", data)
			s.handleServerText(data)
		}
	}
}

// handleDisconnect 处理连接断开：退出中的断开静默；否则停掉本地播放、
// 回待唤醒态并给一行含原因的提示。
func (s *Session) handleDisconnect(cause error) {
	s.mu.Lock()
	closing := s.closing
	if !closing {
		s.connDead = true
		s.state = StateIdle
		s.aborting = false // 该标记只对当前连接的在途旧轮音频有意义，不跨连接存活
	}
	s.mu.Unlock()
	if closing {
		return
	}
	s.player.Stop()
	s.printf("与服务器的连接已断开: %v — 已回待唤醒态，下次唤醒将自动重连\n", cause)
	s.showPrompt()
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
	case "mcp":
		s.handleMCP(data) // 传原始字节：MCP id 需原样回传（见 mcp.go）
	case "hello":
		// 握手已在 Run 内处理，忽略重复 hello。
	case "goodbye":
		// WS 协议本无 goodbye（v0.x 旧 opcode 遗留）——防御性兜底：
		// 一行本地化提示，不打原始 JSON，也不实现协议行为。
		s.printf("会话已结束（goodbye）\n")
	default:
		s.printf("其他消息: %s\n", data)
	}
}

// handleTTS 处理 tts start/sentence_start/stop，驱动状态提示符。
func (s *Session) handleTTS(msg map[string]any) {
	switch msg["state"] {
	case "start":
		// 新一轮开始即解除丢弃（真实 server 按序处理：确认 stop 先于新轮 start；
		// ponytail: 乱序 server 的确认 stop 会被误当新轮收尾，掐断新轮播放——
		// 需要支持乱序 server 时再按轮次关联消息）。
		s.setAborting(false)
		s.setState(StateSpeaking)
		s.showPrompt() // REPL 正阻塞在输入上，立即刷新提示符
	case "sentence_start":
		text, _ := msg["text"].(string)
		s.printf("字幕: %s\n", text)
	case "stop":
		if s.consumeAbortConfirm() {
			return // 打断确认：收尾已在打断时用 Stop 完成，不再 Finish（防双重收尾）
		}
		s.player.Finish() // 本轮 TTS 收尾：让余量播完；打断才用 Stop（见 handleInput）
		s.setState(StateIdle)
		s.showPrompt()
	}
}

// setAborting 设置/清除「打断待确认」标记。
func (s *Session) setAborting(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aborting = v
}

// dropInflightOldRound 报告当前二进制帧是否属于已被打断的旧轮（在途尾包），应丢弃。
func (s *Session) dropInflightOldRound() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.aborting
}

// consumeAbortConfirm 消费打断确认：若正处于打断待确认，清标记并返回 true。
func (s *Session) consumeAbortConfirm() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.aborting {
		s.aborting = false
		return true
	}
	return false
}

// setState 更新状态；来自 server 事件的状态变化立即刷新提示符。
func (s *Session) setState(next State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = next
}

// currentState 读取状态（readLoop 与 REPL 并发读写，须持锁）。
func (s *Session) currentState() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// sendDetect 发送 listen detect 消息（唤醒上报与查询共用），回填 session_id。
func (s *Session) sendDetect(text string) error {
	return s.sendJSON(map[string]any{
		"type":  "listen",
		"state": "detect",
		"text":  text,
	})
}

// writeTimeout 单条 WS 写的超时。sendJSON 持 writeMu 写：对端停止读取时
// WriteMessage 会无限阻塞，冻住 REPL 与 MCP 应答；超时走既有发送失败路径。
// 变量而非常量：测试缩窗钉住超时行为。
var writeTimeout = 10 * time.Second

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
	s.writeMu.Lock()
	_ = s.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	err = s.conn.WriteMessage(websocket.TextMessage, data)
	s.writeMu.Unlock()
	if err != nil {
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
	s.printf("发送%s失败: %v — 连接可能已断开，下次唤醒将自动重连\n", what, err)
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
