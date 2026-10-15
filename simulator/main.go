// xiaozhi 模拟器：终端版模拟设备，用于调试 xiaozhi-esp32-server。
// 启动先做 OTA 检查换取 WebSocket 地址与令牌（--url 直连可跳过）；术语见仓库根 GLOSSARY.md。
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/liyonglion/xiaozhi-esp32/simulator/internal/sim"
)

// 固定默认身份：跨次运行延续 server 侧会话上下文，可用参数覆盖。
const (
	defaultDeviceID = "de:ad:be:ef:ca:fe"
	defaultClientID = "0f3a7c2e-9b41-4d8a-a6d5-3e1f2b4c5d6e"
	defaultOTAURL   = "http://127.0.0.1:8003/xiaozhi/ota/"
)

func main() {
	wsURL := flag.String("url", "", "WebSocket 端点直连（如 ws://127.0.0.1:8000/xiaozhi/v1/），给出时跳过 OTA 检查")
	otaURL := flag.String("ota-url", defaultOTAURL, "OTA 检查端点")
	deviceID := flag.String("device-id", defaultDeviceID, "设备标识（模拟 MAC 形态）")
	clientID := flag.String("client-id", defaultClientID, "客户端标识（固定 UUID）")
	verbose := flag.Bool("verbose", false, "打印收发原始 JSON 帧")
	flag.Parse()

	var verboseWriter io.Writer
	if *verbose {
		verboseWriter = os.Stdout
	}

	fmt.Printf("模拟器启动 device-id=%s client-id=%s\n", *deviceID, *clientID)
	fmt.Println("打「小智」或「你好小智」唤醒（可同行带查询，如「小智，我想听故事」）；exit / Ctrl-D 退出")

	cfg := sim.Config{URL: *wsURL, DeviceID: *deviceID, ClientID: *clientID}
	ota := sim.OTAConfig{OTAURL: *otaURL, Backoff: 500 * time.Millisecond}
	if err := sim.Boot(cfg, ota, os.Stdin, os.Stdout, verboseWriter, nil); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	fmt.Println("已退出。")
}
