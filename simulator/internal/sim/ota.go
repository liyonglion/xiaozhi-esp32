package sim

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// errMQTTMode OTA 响应含 mqtt 节（server 配置了 mqtt_gateway）：模拟器仅支持 WebSocket 模式。
var errMQTTMode = errors.New("不支持 MQTT 模式")

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

// permanentError 确定性失败：重试不会好转（非 5xx/429 的 HTTP 状态、mqtt 节、
// 响应形态错误等）。checkOTA 据此跳过退避重试，立即带明确信息退出。
type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// checkOTA POST system-info 形态 body 到 OTA 端点，解析 websocket{url,token}。
// 仅网络错误与 5xx/429（暂时性失败）按指数退避重试 otaRetries 次；
// 确定性失败（其余 4xx、mqtt 节等）重试只是浪费退避时间，立即退出。
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
		var perm permanentError
		if errors.As(err, &perm) {
			return nil, perm.err
		}
		lastErr = err
	}
	return nil, fmt.Errorf("OTA 检查失败，已重试 %d 次: %w — 确认 --ota-url 地址正确且 server 已启动", otaRetries, lastErr)
}

// otaAttempt 单次 OTA 请求。确定性失败包成 permanentError（见 checkOTA 的重试策略）。
func otaAttempt(cfg Config, otaURL string) (*wsEndpoint, error) {
	data, err := json.Marshal(systemInfo(cfg))
	if err != nil {
		return nil, permanentError{fmt.Errorf("构造 OTA 请求体失败: %w", err)}
	}
	req, err := http.NewRequest(http.MethodPost, otaURL, bytes.NewReader(data))
	if err != nil {
		return nil, permanentError{fmt.Errorf("无效的 OTA 地址 %q: %w", otaURL, err)}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Device-Id", cfg.DeviceID)
	req.Header.Set("Client-Id", cfg.ClientID)
	req.Header.Set("User-Agent", "simulator/"+SimulatorVersion)
	req.Header.Set("Activation-Version", "1")

	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("OTA 请求失败（%s）: %w", otaURL, err) // 网络错误：可重试
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
			// 暂时性失败（服务端故障/限流）：可重试。
			return nil, fmt.Errorf("OTA 端点返回 HTTP %d（%s）", resp.StatusCode, otaURL)
		}
		// 确定性 4xx（404 地址错、401 认证配置错、400 请求形态错……）：重试无益。
		return nil, permanentError{fmt.Errorf(
			"OTA 端点返回 HTTP %d（%s）— 请检查 --ota-url 地址与 server 配置", resp.StatusCode, otaURL)}
	}

	var parsed struct {
		Websocket struct {
			URL   string `json:"url"`
			Token string `json:"token"`
		} `json:"websocket"`
		MQTT json.RawMessage `json:"mqtt"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, permanentError{fmt.Errorf("解析 OTA 响应失败: %w", err)}
	}
	if len(parsed.MQTT) > 0 && string(parsed.MQTT) != "null" {
		return nil, permanentError{fmt.Errorf("OTA 响应含 mqtt 节（server 配置了 mqtt_gateway）: %w", errMQTTMode)}
	}
	if parsed.Websocket.URL == "" {
		return nil, permanentError{fmt.Errorf("OTA 响应缺少 websocket.url（%s）", otaURL)}
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
