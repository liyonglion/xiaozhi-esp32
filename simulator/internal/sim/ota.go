package sim

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// SimulatorVersion 模拟器版本：OTA 的 User-Agent 与 body application.version 共用。
const SimulatorVersion = "0.1.0"

// OTAConfig OTA 检查配置（术语见 GLOSSARY.md：启动时换取 WS 地址与令牌的一次性流程）。
// 设备身份（DeviceID/ClientID）单源于 Config，避免两处配置漂移。
type OTAConfig struct {
	OTAURL  string        // OTA 端点，如 http://127.0.0.1:8003/xiaozhi/ota/
	Backoff time.Duration // 重试退避基数；零值用默认 500ms
}

// wsEndpoint OTA 下发的 WebSocket 接入信息。
type wsEndpoint struct {
	URL   string
	Token string
}

// Boot 引导模拟器：--url 直连完全跳过 OTA；否则先做 OTA 检查换取端点（含令牌）再进入会话。
// 启动目标地址统一在此打印，便于测试与终端一致性。
func Boot(cfg Config, ota OTAConfig, in io.Reader, out io.Writer, verbose io.Writer, player Player) error {
	if cfg.URL == "" {
		ep, err := checkOTA(cfg, ota, out)
		if err != nil {
			return err
		}
		cfg.URL, cfg.Token = ep.URL, ep.Token
	}
	fmt.Fprintf(out, "目标 %s\n", cfg.URL)
	return NewSession(cfg, in, out, verbose, player).Run()
}

// otaRetries 初次失败后的重试次数（指数退避：Backoff×1、×2、×4）。
const otaRetries = 3

// checkOTA POST system-info 形态 body 到 OTA 端点，解析 websocket{url,token}。
// 传输层/HTTP 层失败按指数退避重试 otaRetries 次；server 明确答复的不一致（如 mqtt 节）不重试。
func checkOTA(cfg Config, ota OTAConfig, log io.Writer) (*wsEndpoint, error) {
	backoff := ota.Backoff
	if backoff <= 0 {
		backoff = 500 * time.Millisecond
	}
	var lastErr error
	for attempt := 0; attempt <= otaRetries; attempt++ {
		if attempt > 0 {
			wait := backoff << (attempt - 1)
			fmt.Fprintf(log, "OTA 检查失败: %v — %s 后重试（%d/%d）\n", lastErr, wait, attempt, otaRetries)
			time.Sleep(wait)
		}
		ep, err := otaAttempt(cfg, ota.OTAURL)
		if err == nil {
			return ep, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("OTA 检查失败，已重试 %d 次: %w — 确认 --ota-url 地址正确且 server 已启动", otaRetries, lastErr)
}

// otaAttempt 单次 OTA 请求。
func otaAttempt(cfg Config, otaURL string) (*wsEndpoint, error) {
	data, err := json.Marshal(systemInfo(cfg))
	if err != nil {
		return nil, fmt.Errorf("构造 OTA 请求体失败: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, otaURL, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("无效的 OTA 地址 %q: %w", otaURL, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Device-Id", cfg.DeviceID)
	req.Header.Set("Client-Id", cfg.ClientID)
	req.Header.Set("User-Agent", "simulator/"+SimulatorVersion)
	req.Header.Set("Activation-Version", "1")

	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("OTA 请求失败（%s）: %w", otaURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("OTA 端点返回 HTTP %d（%s）", resp.StatusCode, otaURL)
	}

	var parsed struct {
		Websocket struct {
			URL   string `json:"url"`
			Token string `json:"token"`
		} `json:"websocket"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("解析 OTA 响应失败: %w", err)
	}
	if parsed.Websocket.URL == "" {
		return nil, fmt.Errorf("OTA 响应缺少 websocket.url（%s）", otaURL)
	}
	return &wsEndpoint{URL: parsed.Websocket.URL, Token: parsed.Websocket.Token}, nil
}

// systemInfo 构造 OTA body：设备 system-info 形态（server 宽松读取，对照固件 board.cc）。
func systemInfo(cfg Config) map[string]any {
	return map[string]any{
		"version":         2,
		"mac_address":     cfg.DeviceID,
		"uuid":            cfg.ClientID,
		"chip_model_name": "simulator",
		"application":     map[string]any{"name": "simulator", "version": SimulatorVersion},
		"board":           map[string]any{"type": "simulator", "mac": cfg.DeviceID},
	}
}
