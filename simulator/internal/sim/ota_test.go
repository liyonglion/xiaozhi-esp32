package sim

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// —— 假 OTA 端点（对照真 server 的 /xiaozhi/ota/）——

// otaRequest 记录一次 OTA 请求的外部可见信息（handler 返回后 http.Request 不可再读）。
type otaRequest struct {
	Method string
	Path   string
	Header http.Header
	Body   []byte
}

// fakeOTA 进程内假 OTA 端点：按剧本应答，记录收到的请求。
type fakeOTA struct {
	t   *testing.T
	srv *httptest.Server

	mu       sync.Mutex
	requests []otaRequest
}

// newFakeOTA script 按当前是第几次请求返回 (HTTP 状态码, 响应体)。
func newFakeOTA(t *testing.T, script func(count int) (int, string)) *fakeOTA {
	t.Helper()
	fo := &fakeOTA{t: t}
	fo.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("假 OTA 读取请求体失败: %v", err)
		}
		fo.mu.Lock()
		fo.requests = append(fo.requests, otaRequest{
			Method: r.Method,
			Path:   r.URL.Path,
			Header: r.Header.Clone(),
			Body:   body,
		})
		count := len(fo.requests)
		fo.mu.Unlock()
		status, resp := script(count)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprint(w, resp)
	}))
	t.Cleanup(fo.srv.Close)
	return fo
}

func (fo *fakeOTA) count() int {
	fo.mu.Lock()
	defer fo.mu.Unlock()
	return len(fo.requests)
}

func (fo *fakeOTA) request(t *testing.T, i int) otaRequest {
	t.Helper()
	fo.mu.Lock()
	defer fo.mu.Unlock()
	if i >= len(fo.requests) {
		t.Fatalf("假 OTA 尚未收到第 %d 个请求（已收 %d 个）", i+1, len(fo.requests))
	}
	return fo.requests[i]
}

// otaResponseBody 应答真 server 形态：websocket{url,token}（可附 mqtt 节）。
func otaResponseBody(wsURL, token string, withMQTT bool) string {
	body := fmt.Sprintf(`{"server_time":{"epoch":1},"firmware":{"version":"","url":""},"websocket":{"url":%q,"token":%q}}`, wsURL, token)
	if withMQTT {
		body = body[:len(body)-1] + `,"mqtt":{"endpoint":"mqtts://example:8883"}}`
	}
	return body
}

// —— 测试 ——

// OTA→WS 衔接：无 --url 启动先 POST OTA，转连下发的 WS url 且带 Bearer 头。
func TestBootOTAResolvesEndpointThenConnectsWithBearer(t *testing.T) {
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
	fs.nextReceived(t)                                      // hello：已转连 OTA 下发的 url
	wantDetect(t, fs.nextReceived(t), "你好小智", fs.sessionID) // 唤醒上报走通

	if got := fs.authorization(t); got != "Bearer tok-123" {
		t.Errorf("升级请求 Authorization = %q, 期望 %q", got, "Bearer tok-123")
	}
	if q := fs.queryValues(t); q.Get("device-id") != "de:ad:be:ef:ca:fe" {
		t.Errorf("连接查询参数 device-id = %q, 期望来自身份配置", q.Get("device-id"))
	}

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Boot 返回错误: %v", err)
	}
}

// --url 直连是 OTA 检查的逃生口：给出时假 OTA 端点零请求。
func TestBootURLSkipsOTAEntirely(t *testing.T) {
	fs := newFakeServer(t)
	ota := newFakeOTA(t, func(count int) (int, string) {
		return http.StatusOK, otaResponseBody(fs.wsURL, "", false)
	})
	stdinR, stdin := scriptStdin(t)
	done := make(chan error, 1)
	go func() {
		done <- Boot(Config{URL: fs.wsURL, DeviceID: "de:ad:be:ef:ca:fe", ClientID: "0f3a7c2e-9b41-4d8a-a6d5-3e1f2b4c5d6e"},
			OTAConfig{OTAURL: ota.srv.URL, Backoff: time.Millisecond}, stdinR, &lockedBuffer{}, nil, nil)
	}()

	if _, err := io.WriteString(stdin, "小智\nexit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	fs.nextReceived(t)                                      // hello：直连假 server
	wantDetect(t, fs.nextReceived(t), "你好小智", fs.sessionID) // 会话可用
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Boot 返回错误: %v", err)
	}
	if got := ota.count(); got != 0 {
		t.Errorf("--url 直连时假 OTA 端点应收 0 个请求, 实收 %d 个", got)
	}
}

// OTA 请求契约：POST + 必需头（Device-Id/Client-Id/User-Agent/Activation-Version/
// Content-Type）与 system-info 形态 body 的关键字段（server 读得宽松，但头必填）。
func TestBootOTARequestShape(t *testing.T) {
	fs := newFakeServer(t)
	ota := newFakeOTA(t, func(count int) (int, string) {
		return http.StatusOK, otaResponseBody(fs.wsURL, "", false)
	})
	stdinR, stdin := scriptStdin(t)
	done := make(chan error, 1)
	go func() {
		done <- Boot(Config{DeviceID: "aa:bb:cc:dd:ee:ff", ClientID: "11111111-2222-3333-4444-555555555555"},
			OTAConfig{OTAURL: ota.srv.URL, Backoff: time.Millisecond}, stdinR, &lockedBuffer{}, nil, nil)
	}()
	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Boot 返回错误: %v", err)
	}

	req := ota.request(t, 0)
	if req.Method != http.MethodPost {
		t.Errorf("OTA 请求方法 = %q, 期望 POST", req.Method)
	}
	for header, want := range map[string]string{
		"Device-Id":          "aa:bb:cc:dd:ee:ff",
		"Client-Id":          "11111111-2222-3333-4444-555555555555",
		"User-Agent":         "simulator/" + SimulatorVersion,
		"Activation-Version": "1",
		"Content-Type":       "application/json",
	} {
		if got := req.Header.Get(header); got != want {
			t.Errorf("OTA 请求头 %s = %q, 期望 %q", header, got, want)
		}
	}

	var body map[string]any
	if err := json.Unmarshal(req.Body, &body); err != nil {
		t.Fatalf("OTA body 不是合法 JSON: %v（%s）", err, req.Body)
	}
	if got, _ := body["mac_address"].(string); got != "aa:bb:cc:dd:ee:ff" {
		t.Errorf("OTA body mac_address = %q, 期望与 Device-Id 一致", got)
	}
	if got, _ := body["uuid"].(string); got != "11111111-2222-3333-4444-555555555555" {
		t.Errorf("OTA body uuid = %q, 期望与 Client-Id 一致", got)
	}
	app, _ := body["application"].(map[string]any)
	if got, _ := app["version"].(string); app == nil || got != SimulatorVersion {
		t.Errorf("OTA body application.version = %q, 期望 %q", got, SimulatorVersion)
	}
	board, _ := body["board"].(map[string]any)
	if got, _ := board["type"].(string); board == nil || got != "simulator" {
		t.Errorf("OTA body board.type = %q, 期望 simulator", got)
	}
}

// OTA 持续失败：重试 3 次（共 1+3 个请求）后带明确信息退出，终端可见重试进度。
func TestBootOTARetriesThenExitsFriendly(t *testing.T) {
	ota := newFakeOTA(t, func(count int) (int, string) {
		return http.StatusInternalServerError, `{"error":"boom"}`
	})
	stdinR, _ := scriptStdin(t)
	out := &lockedBuffer{}
	done := make(chan error, 1)
	go func() {
		done <- Boot(Config{DeviceID: "de:ad:be:ef:ca:fe", ClientID: "0f3a7c2e-9b41-4d8a-a6d5-3e1f2b4c5d6e"},
			OTAConfig{OTAURL: ota.srv.URL, Backoff: time.Millisecond}, stdinR, out, nil, nil)
	}()

	var err error
	select {
	case err = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("超时：OTA 反复失败后 Boot 未退出")
	}
	if err == nil {
		t.Fatal("OTA 反复失败后 Boot 应返回错误")
	}
	if got := ota.count(); got != 1+3 {
		t.Errorf("失败时应发起 1+3=4 个请求（初次+重试 3 次）, 实收 %d 个", got)
	}
	for _, want := range []string{"OTA", "3 次", "server"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("退出错误应含 %q（指向排查方向）, 实际: %v", want, err)
		}
	}
	if got := out.String(); !strings.Contains(got, "重试") {
		t.Errorf("终端应可见重试进度, 实际输出:\n%s", got)
	}
}

// 响应含 mqtt 节（server 配置了 mqtt_gateway）：明确报不支持 MQTT 模式退出，且不重试。
func TestBootOTARejectsMQTTMode(t *testing.T) {
	fs := newFakeServer(t)
	ota := newFakeOTA(t, func(count int) (int, string) {
		return http.StatusOK, otaResponseBody(fs.wsURL, "", true)
	})
	stdinR, _ := scriptStdin(t)
	done := make(chan error, 1)
	go func() {
		done <- Boot(Config{DeviceID: "de:ad:be:ef:ca:fe", ClientID: "0f3a7c2e-9b41-4d8a-a6d5-3e1f2b4c5d6e"},
			OTAConfig{OTAURL: ota.srv.URL, Backoff: time.Millisecond}, stdinR, &lockedBuffer{}, nil, nil)
	}()

	var err error
	select {
	case err = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("超时：mqtt 节未导致 Boot 退出")
	}
	if err == nil {
		t.Fatal("mqtt 节应使 Boot 返回错误")
	}
	if !strings.Contains(err.Error(), "MQTT") {
		t.Errorf("退出错误应明确报不支持 MQTT 模式, 实际: %v", err)
	}
	if got := ota.count(); got != 1 {
		t.Errorf("mqtt 节是明确答复, 不应重试（期望 1 个请求）, 实收 %d 个", got)
	}
}

// 确定性 4xx（404 地址错、401 认证错等）：立即失败并给明确信息，
// 不做退避重试（重试只会白等 ~3.5s）。
func TestBootOTA4xxFailsFastWithoutRetry(t *testing.T) {
	ota := newFakeOTA(t, func(count int) (int, string) {
		return http.StatusNotFound, `{"error":"not found"}`
	})
	stdinR, _ := scriptStdin(t)
	out := &lockedBuffer{}
	done := make(chan error, 1)
	go func() {
		done <- Boot(Config{DeviceID: "de:ad:be:ef:ca:fe", ClientID: "0f3a7c2e-9b41-4d8a-a6d5-3e1f2b4c5d6e"},
			OTAConfig{OTAURL: ota.srv.URL, Backoff: time.Millisecond}, stdinR, out, nil, nil)
	}()

	var err error
	select {
	case err = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("超时：确定性 4xx 未立即失败")
	}
	if err == nil {
		t.Fatal("4xx 应使 Boot 返回错误")
	}
	if got := ota.count(); got != 1 {
		t.Errorf("确定性 4xx 不应重试（期望 1 个请求）, 实收 %d 个", got)
	}
	for _, want := range []string{"404", "--ota-url"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("退出错误应含 %q（指向排查方向）, 实际: %v", want, err)
		}
	}
	if got := out.String(); strings.Contains(got, "重试") {
		t.Errorf("4xx 不应打印重试进度, 实际输出:\n%s", got)
	}
}

// 429 是 4xx 中的例外：限流是暂时性失败，退避重试后应成功。
func TestBootOTA429RetriesThenSucceeds(t *testing.T) {
	fs := newFakeServer(t)
	ota := newFakeOTA(t, func(count int) (int, string) {
		if count == 1 {
			return http.StatusTooManyRequests, `{"error":"slow down"}`
		}
		return http.StatusOK, otaResponseBody(fs.wsURL, "", false)
	})
	stdinR, stdin := scriptStdin(t)
	done := make(chan error, 1)
	go func() {
		done <- Boot(Config{DeviceID: "de:ad:be:ef:ca:fe", ClientID: "0f3a7c2e-9b41-4d8a-a6d5-3e1f2b4c5d6e"},
			OTAConfig{OTAURL: ota.srv.URL, Backoff: time.Millisecond}, stdinR, &lockedBuffer{}, nil, nil)
	}()

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("429 后重试应成功, 实际: %v", err)
	}
	if got := ota.count(); got != 2 {
		t.Errorf("429 应重试一次后成功（期望 2 个请求）, 实收 %d 个", got)
	}
}

// auth 关闭的部署 token 为空串：连接应省略 Authorization 头。
func TestBootOTAEmptyTokenOmitsAuthorization(t *testing.T) {
	fs := newFakeServer(t)
	ota := newFakeOTA(t, func(count int) (int, string) {
		return http.StatusOK, otaResponseBody(fs.wsURL, "", false)
	})
	stdinR, stdin := scriptStdin(t)
	done := make(chan error, 1)
	go func() {
		done <- Boot(Config{DeviceID: "de:ad:be:ef:ca:fe", ClientID: "0f3a7c2e-9b41-4d8a-a6d5-3e1f2b4c5d6e"},
			OTAConfig{OTAURL: ota.srv.URL, Backoff: time.Millisecond}, stdinR, &lockedBuffer{}, nil, nil)
	}()

	if _, err := io.WriteString(stdin, "exit\n"); err != nil {
		t.Fatalf("写入脚本输入失败: %v", err)
	}
	if err := waitDone(t, done); err != nil {
		t.Fatalf("Boot 返回错误: %v", err)
	}
	if got := fs.authorization(t); got != "" {
		t.Errorf("token 为空时应省略 Authorization 头, 实际 %q", got)
	}
}
