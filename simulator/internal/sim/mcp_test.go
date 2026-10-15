package sim

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// —— MCP 测试辅助 ——

// mcpRequest 构造 server 下发的 {"type":"mcp","payload":{jsonrpc,id,method,params}}。
func mcpRequest(id float64, method string, params map[string]any) map[string]any {
	payload := map[string]any{"jsonrpc": "2.0", "id": id, "method": method}
	if params != nil {
		payload["params"] = params
	}
	return map[string]any{"type": "mcp", "payload": payload}
}

// wantMCPReply 断言 msg 是对指定 id 的 mcp JSON-RPC 应答（带 session_id），返回 payload。
func wantMCPReply(t *testing.T, msg map[string]any, id float64, sessionID string) map[string]any {
	t.Helper()
	wantString(t, msg, "type", "mcp")
	wantString(t, msg, "session_id", sessionID)
	payload, _ := msg["payload"].(map[string]any)
	if payload == nil {
		t.Fatalf("mcp 应答缺少 payload 对象: %v", msg)
	}
	wantString(t, payload, "jsonrpc", "2.0")
	if got, _ := payload["id"].(float64); got != id {
		t.Errorf("mcp 应答 payload.id = %v, 期望 %v（完整: %v）", payload["id"], id, msg)
	}
	return payload
}

// wantMCPResult 取出应答 payload 的 result 对象，且不应含 error。
func wantMCPResult(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()
	result, _ := payload["result"].(map[string]any)
	if result == nil {
		t.Fatalf("mcp 应答缺少 result 对象: %v", payload)
	}
	if _, hasErr := payload["error"]; hasErr {
		t.Errorf("mcp 应答应无 error, 实际: %v", payload["error"])
	}
	return result
}

// —— 测试 ——

// MCP 握手：server 下发 initialize，模拟器应答 result.serverInfo（标明模拟器）
// 并回显 protocolVersion。
func TestSessionMCPInitialize(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	sess := newTestSession(t, fs, &lockedBuffer{}, nil, &fakePlayer{}, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	fs.nextReceived(t) // hello
	fs.send(t, mcpRequest(1, "initialize", map[string]any{
		"protocolVersion": "2024-11-05",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "xiaozhi-server", "version": "1.0"},
	}))

	payload := wantMCPReply(t, fs.nextReceived(t), 1, fs.sessionID)
	result := wantMCPResult(t, payload)
	wantString(t, result, "protocolVersion", "2024-11-05")
	serverInfo, _ := result["serverInfo"].(map[string]any)
	if serverInfo == nil {
		t.Fatalf("initialize 应答缺少 serverInfo: %v", payload)
	}
	if name, _ := serverInfo["name"].(string); !strings.Contains(name, "xiaozhi-simulator") {
		t.Errorf("serverInfo.name = %q, 应标明 xiaozhi-simulator", name)
	}

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Session.Run 返回错误: %v", err)
	}
}

// tools/list（可带 cursor 分页参数）：应答 result.tools = 两个假工具，
// 各带 name/description/inputSchema（JSON Schema）。
func TestSessionMCPToolsList(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	sess := newTestSession(t, fs, &lockedBuffer{}, nil, &fakePlayer{}, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	fs.nextReceived(t) // hello
	fs.send(t, mcpRequest(2, "tools/list", map[string]any{"cursor": ""}))

	payload := wantMCPReply(t, fs.nextReceived(t), 2, fs.sessionID)
	result := wantMCPResult(t, payload)
	tools, _ := result["tools"].([]any)
	if len(tools) != 2 {
		t.Fatalf("tools/list 应答应有 2 个工具, 实际 %d: %v", len(tools), result)
	}
	for _, raw := range tools {
		tool, _ := raw.(map[string]any)
		if tool == nil {
			t.Fatalf("工具条目不是对象: %v", raw)
		}
		name, _ := tool["name"].(string)
		if desc, _ := tool["description"].(string); desc == "" {
			t.Errorf("工具 %s 缺少 description", name)
		}
		schema, _ := tool["inputSchema"].(map[string]any)
		if schema == nil {
			t.Errorf("工具 %s 缺少 inputSchema（JSON Schema 对象）, 实际: %v", name, tool["inputSchema"])
			continue
		}
		if st, _ := schema["type"].(string); st != "object" {
			t.Errorf("工具 %s inputSchema.type = %q, 期望 object", name, st)
		}
		switch name {
		case "get_device_status", "set_volume":
		default:
			t.Errorf("意外工具名 %q, 期望 get_device_status / set_volume", name)
		}
	}
	// set_volume 的 volume 参数应声明 0-100 整数边界。
	var setVolume map[string]any
	for _, raw := range tools {
		if tool, _ := raw.(map[string]any); tool != nil && tool["name"] == "set_volume" {
			setVolume = tool
		}
	}
	schema, _ := setVolume["inputSchema"].(map[string]any)
	props, _ := schema["properties"].(map[string]any)
	volume, _ := props["volume"].(map[string]any)
	if volume == nil {
		t.Fatalf("set_volume inputSchema 缺少 volume 参数: %v", schema)
	}
	if got, _ := volume["type"].(string); got != "integer" {
		t.Errorf("volume.type = %q, 期望 integer", got)
	}
	if got, _ := volume["minimum"].(float64); got != 0 {
		t.Errorf("volume.minimum = %v, 期望 0", volume["minimum"])
	}
	if got, _ := volume["maximum"].(float64); got != 100 {
		t.Errorf("volume.maximum = %v, 期望 100", volume["maximum"])
	}
	if req, _ := schema["required"].([]any); len(req) != 1 || req[0] != "volume" {
		t.Errorf("inputSchema.required = %v, 期望 [volume]", schema["required"])
	}

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Session.Run 返回错误: %v", err)
	}
}

// tools/call 四段全链路：initialize → tools/list → tools/call(get_device_status)
// → tools/call(set_volume)，应答均为 result.content[0].text + isError:false，id 依次回显。
func TestSessionMPCToolsCallFlow(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	sess := newTestSession(t, fs, &lockedBuffer{}, nil, &fakePlayer{}, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	fs.nextReceived(t) // hello
	fs.send(t, mcpRequest(1, "initialize", map[string]any{"protocolVersion": "2024-11-05"}))
	fs.send(t, mcpRequest(2, "tools/list", nil))
	fs.send(t, mcpRequest(3, "tools/call", map[string]any{"name": "get_device_status", "arguments": map[string]any{}}))
	fs.send(t, mcpRequest(4, "tools/call", map[string]any{"name": "set_volume", "arguments": map[string]any{"volume": 30}}))

	// ① initialize
	wantMCPResult(t, wantMCPReply(t, fs.nextReceived(t), 1, fs.sessionID))
	// ② tools/list
	wantMCPResult(t, wantMCPReply(t, fs.nextReceived(t), 2, fs.sessionID))
	// ③ get_device_status：音量 80 / 电池充电中假数据
	status := wantToolCallResult(t, wantMCPReply(t, fs.nextReceived(t), 3, fs.sessionID))
	if !strings.Contains(status, `"volume":80`) {
		t.Errorf("get_device_status 文本应含音量 80, 实际: %s", status)
	}
	if !strings.Contains(status, "charging") {
		t.Errorf("get_device_status 文本应表明电池充电中, 实际: %s", status)
	}
	// ④ set_volume(30)：回显成功与音量值
	volume := wantToolCallResult(t, wantMCPReply(t, fs.nextReceived(t), 4, fs.sessionID))
	if !strings.Contains(volume, `"volume":30`) {
		t.Errorf("set_volume 文本应回显音量 30, 实际: %s", volume)
	}
	if !strings.Contains(volume, "success") || strings.Contains(volume, `"success":false`) {
		t.Errorf("set_volume 文本应表明成功, 实际: %s", volume)
	}

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Session.Run 返回错误: %v", err)
	}
}

// wantToolCallResult 断言 tools/call 应答形态（content[0].text + isError:false）并返回文本。
func wantToolCallResult(t *testing.T, payload map[string]any) string {
	t.Helper()
	result := wantMCPResult(t, payload)
	if isErr, _ := result["isError"].(bool); isErr {
		t.Errorf("tools/call 应答 isError 应为 false, 完整: %v", payload)
	}
	content, _ := result["content"].([]any)
	if len(content) == 0 {
		t.Fatalf("tools/call 应答缺少 content 数组: %v", payload)
	}
	first, _ := content[0].(map[string]any)
	if first == nil {
		t.Fatalf("content[0] 不是对象: %v", content[0])
	}
	if typ, _ := first["type"].(string); typ != "text" {
		t.Errorf("content[0].type = %q, 期望 text", typ)
	}
	text, _ := first["text"].(string)
	if text == "" {
		t.Errorf("content[0].text 为空, 完整: %v", payload)
	}
	return text
}

// 未知工具名：回 JSON-RPC error（-32601 method not found 形态），带 id 回显，不含 result。
func TestSessionMCPUnknownToolRepliesError(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	sess := newTestSession(t, fs, &lockedBuffer{}, nil, &fakePlayer{}, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	fs.nextReceived(t) // hello
	fs.send(t, mcpRequest(7, "tools/call", map[string]any{"name": "take_photo", "arguments": map[string]any{}}))

	payload := wantMCPReply(t, fs.nextReceived(t), 7, fs.sessionID)
	if _, hasResult := payload["result"]; hasResult {
		t.Errorf("未知工具应答不应含 result, 实际: %v", payload)
	}
	errObj, _ := payload["error"].(map[string]any)
	if errObj == nil {
		t.Fatalf("未知工具应答缺少 error 对象: %v", payload)
	}
	if got, _ := errObj["code"].(float64); got != -32601 {
		t.Errorf("error.code = %v, 期望 -32601（method not found 形态）", errObj["code"])
	}
	if msg, _ := errObj["message"].(string); !strings.Contains(msg, "take_photo") {
		t.Errorf("error.message 应包含未知工具名, 实际: %q", msg)
	}

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Session.Run 返回错误: %v", err)
	}
}

// id 原样回传：JSON-RPC id 用 RawMessage 保留——大整数 id（>2^53）经 float64
// 会丢精度（…993 变 …992），字符串 id 也不得被改写，否则 server 侧对不上号。
func TestSessionMCPPreservesRequestIDVerbatim(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	verbose := &lockedBuffer{}
	sess := newTestSession(t, fs, &lockedBuffer{}, verbose, &fakePlayer{}, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	fs.nextReceived(t) // hello
	// 手工构造原始帧：map[string]any 装不下「会被 float64 丢精度的大整数 id」。
	fs.sendRaw(t, `{"type":"mcp","payload":{"jsonrpc":"2.0","id":9007199254740993,"method":"tools/list"}}`)
	fs.sendRaw(t, `{"type":"mcp","payload":{"jsonrpc":"2.0","id":"call-abc-123","method":"tools/list"}}`)

	// 只认上行（→ ）帧里的 id：下行帧同样含请求 id，不能作为回显证据。
	bigOK, strOK := false, false
	eventually(t, func() bool {
		for _, line := range strings.Split(verbose.String(), "\n") {
			if !strings.HasPrefix(line, "→ ") {
				continue
			}
			if strings.Contains(line, `"id":9007199254740993`) {
				bigOK = true
			}
			if strings.Contains(line, `"id":"call-abc-123"`) {
				strOK = true
			}
		}
		return bigOK && strOK
	})

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Session.Run 返回错误: %v", err)
	}
}

// MCP 期间对话不串线：REPL 在发唤醒+查询的同时，server 交替下发 MCP 请求与
// 对话/音频帧——上行消息互不丢失（顺序跨 goroutine 不定，按集合断言），
// 对话帧照常打印、opus 照常到达播放器（-race 下验证并发 WS 写安全）。
func TestSessionMCPDoesNotBlockDialogue(t *testing.T) {
	fs := newFakeServer(t)
	stdinR, stdin := scriptStdin(t)
	out := &lockedBuffer{}
	player := &fakePlayer{}
	sess := newTestSession(t, fs, out, nil, player, stdinR)
	done := make(chan error, 1)
	go func() { done <- sess.Run() }()

	fs.nextReceived(t) // hello

	if _, err := io.WriteString(stdin, "小智，现在几点\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	opusFrame := []byte{0x09, 0x08, 0x07}
	fs.send(t, mcpRequest(1, "initialize", map[string]any{"protocolVersion": "2024-11-05"}))
	fs.send(t, map[string]any{"type": "stt", "text": "现在几点"})
	fs.send(t, mcpRequest(2, "tools/list", nil))
	fs.send(t, map[string]any{"type": "tts", "state": "start"})
	fs.send(t, mcpRequest(3, "tools/call", map[string]any{"name": "get_device_status", "arguments": map[string]any{}}))
	fs.send(t, map[string]any{"type": "tts", "state": "sentence_start", "text": "现在是三点"})
	fs.sendBinary(t, opusFrame)
	fs.send(t, map[string]any{"type": "tts", "state": "stop"})

	// 设备上行共 5 条：唤醒上报、查询、3 条 MCP 应答。
	var detects, mcpReplies []map[string]any
	for i := 0; i < 5; i++ {
		msg := fs.nextReceived(t)
		if msg["type"] == "listen" {
			detects = append(detects, msg)
		} else {
			mcpReplies = append(mcpReplies, msg)
		}
	}
	detectTexts := map[string]bool{}
	for _, msg := range detects {
		text, _ := msg["text"].(string)
		detectTexts[text] = true
	}
	if !detectTexts["你好小智"] || !detectTexts["现在几点"] {
		t.Errorf("唤醒上报与查询均应送达 server, 实际 detect: %v", detectTexts)
	}
	mcpIDs := map[float64]bool{}
	for _, msg := range mcpReplies {
		wantString(t, msg, "type", "mcp")
		wantString(t, msg, "session_id", fs.sessionID)
		payload, _ := msg["payload"].(map[string]any)
		if payload == nil {
			t.Fatalf("mcp 应答缺少 payload: %v", msg)
		}
		wantString(t, payload, "jsonrpc", "2.0")
		id, _ := payload["id"].(float64)
		mcpIDs[id] = true
	}
	for _, want := range []float64{1, 2, 3} {
		if !mcpIDs[want] {
			t.Errorf("MCP 应答 id=%v 未到达, 实际收到: %v", want, mcpIDs)
		}
	}

	// 对话帧照常处理：stt/字幕打印、opus 到播放器、tts stop 回到待唤醒。
	eventually(t, func() bool {
		got := out.String()
		return strings.Contains(got, "stt: 现在几点") &&
			strings.Contains(got, "字幕: 现在是三点") &&
			strings.Count(got, "待唤醒> \n") >= 2
	})
	frames, _, _ := player.snapshot()
	var oggBytes bytes.Buffer
	for _, f := range frames {
		oggBytes.Write(f)
	}
	pages := parseOggPages(t, oggBytes.Bytes())
	var gotPacket bool
	for _, pg := range pages {
		if bytes.Equal(pg.payload, opusFrame) {
			gotPacket = true
		}
	}
	if !gotPacket {
		t.Errorf("MCP 期间 opus 帧应照常以 Ogg 页到达播放器, 实际 %d 页", len(pages))
	}

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Session.Run 返回错误: %v", err)
	}
}
