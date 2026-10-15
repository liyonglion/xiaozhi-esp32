package sim

import (
	"encoding/json"
	"fmt"
)

// MCP over WebSocket：server 在 hello（features.mcp=true）后异步下发 JSON-RPC 请求，
// 设备以 {"type":"mcp","payload":{jsonrpc,id,result|error}} 应答（协议事实 §6）。
// server 侧 tool_call_timeout 30s：应答在 readLoop 内同步完成，假工具无耗时 I/O。

// rpcRequest MCP JSON-RPC 请求的本地化解析。id 用 json.RawMessage 原样保留：
// 泛型 map 会把数字 id 解成 float64，超过 2^53 的整数回显即丢精度，server 侧对不上号。
type rpcRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params struct {
		ProtocolVersion string         `json:"protocolVersion"`
		Name            string         `json:"name"`
		Arguments       map[string]any `json:"arguments"`
	} `json:"params"`
}

// handleMCP 处理 server 下发的 JSON-RPC 请求并立即应答。
// 入参为原始帧字节（而非已解析的泛型 map），使 id 能原样回传。
func (s *Session) handleMCP(data []byte) {
	var msg struct {
		Payload rpcRequest `json:"payload"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		s.printf("（忽略）无法解析的 mcp 消息: %s\n", data)
		return
	}
	req := msg.Payload
	if len(req.ID) == 0 {
		s.printf("（忽略）无 id 的 mcp 请求（通知）: %v\n", req.Method)
		return
	}
	switch req.Method {
	case "initialize":
		s.sendMCPResult(req.ID, initializeResult(req.Params.ProtocolVersion))
	case "tools/list":
		s.sendMCPResult(req.ID, map[string]any{"tools": fakeTools()})
	case "tools/call":
		s.handleToolsCall(req)
	default:
		s.sendMCPError(req.ID, -32601, fmt.Sprintf("Method not found: %s", req.Method))
	}
}

// handleToolsCall 按工具名分发假工具调用；未知工具回 -32601。
func (s *Session) handleToolsCall(req rpcRequest) {
	switch req.Params.Name {
	case "get_device_status":
		s.sendMCPResult(req.ID, toolCallResult(map[string]any{
			"volume":  80,
			"battery": map[string]any{"level": 100, "state": "charging"},
		}))
	case "set_volume":
		volume, _ := req.Params.Arguments["volume"].(float64)
		s.sendMCPResult(req.ID, toolCallResult(map[string]any{"success": true, "volume": int(volume)}))
	default:
		s.sendMCPError(req.ID, -32601, "Method not found: "+req.Params.Name)
	}
}

// toolCallResult 包装 tools/call 结果为 content[0].text + isError:false。
func toolCallResult(value map[string]any) map[string]any {
	data, err := json.Marshal(value)
	if err != nil { // 字面量 map 不会失败；兜底不静默吞错
		data = []byte(fmt.Sprintf(`{"error":"序列化工具结果失败: %v"}`, err))
	}
	return map[string]any{
		"content": []any{map[string]any{"type": "text", "text": string(data)}},
		"isError": false,
	}
}

// fakeTools 暴露给 server 的两个假工具（get_device_status 假数据 / set_volume 回显成功）。
func fakeTools() []map[string]any {
	return []map[string]any{
		{
			"name":        "get_device_status",
			"description": "获取设备状态（音量、电池等假数据，用于调试）",
			"inputSchema": map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
		{
			"name":        "set_volume",
			"description": "设置音量（0-100，回显设置结果）",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"volume": map[string]any{
						"type":        "integer",
						"minimum":     0,
						"maximum":     100,
						"description": "目标音量",
					},
				},
				"required": []any{"volume"},
			},
		},
	}
}

// initializeResult 回显请求的 protocolVersion，serverInfo 标明模拟器。
func initializeResult(protocolVersion string) map[string]any {
	if protocolVersion == "" {
		protocolVersion = "2024-11-05"
	}
	return map[string]any{
		"protocolVersion": protocolVersion,
		"capabilities":    map[string]any{"tools": map[string]any{}},
		"serverInfo":      map[string]any{"name": "xiaozhi-simulator", "version": "1.0.0"},
	}
}

// sendMCPResult 回 JSON-RPC result 应答（id 为原样保留的 RawMessage）。
func (s *Session) sendMCPResult(id json.RawMessage, result map[string]any) {
	s.sendMCPPayload(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

// sendMCPError 回 JSON-RPC error 应答（未知方法/工具用 -32601）。
func (s *Session) sendMCPError(id json.RawMessage, code int, message string) {
	s.sendMCPPayload(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error":   map[string]any{"code": code, "message": message},
	})
}

// sendMCPPayload 包上 type=mcp 外层并发送（sendJSON 回填 session_id）。
func (s *Session) sendMCPPayload(payload map[string]any) {
	if err := s.sendJSON(map[string]any{"type": "mcp", "payload": payload}); err != nil {
		s.reportSendError("MCP 应答", err)
	}
}
