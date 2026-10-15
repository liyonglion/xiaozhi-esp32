// xiaozhi 模拟器：终端版模拟设备，用于调试 xiaozhi-esp32-server。
// 本票（sim#1）为 --url 直连的纯文字对话主链路；术语见仓库根 GLOSSARY.md。
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/liyonglion/xiaozhi-esp32/simulator/internal/sim"
)

// 固定默认身份：跨次运行延续 server 侧会话上下文，可用参数覆盖。
const (
	defaultDeviceID = "de:ad:be:ef:ca:fe"
	defaultClientID = "0f3a7c2e-9b41-4d8a-a6d5-3e1f2b4c5d6e"
)

func main() {
	wsURL := flag.String("url", "", "WebSocket 端点直连（如 ws://127.0.0.1:8000/xiaozhi/v1/）")
	deviceID := flag.String("device-id", defaultDeviceID, "设备标识（模拟 MAC 形态）")
	clientID := flag.String("client-id", defaultClientID, "客户端标识（固定 UUID）")
	verbose := flag.Bool("verbose", false, "打印收发原始 JSON 帧")
	flag.Parse()

	if *wsURL == "" {
		fmt.Fprintln(os.Stderr, "缺少 --url：OTA 检查尚未实现（后续票接管），请先用 --url ws://<ip>:8000/xiaozhi/v1/ 直连")
		os.Exit(2)
	}

	var verboseWriter io.Writer
	if *verbose {
		verboseWriter = os.Stdout
	}

	fmt.Printf("模拟器启动 device-id=%s client-id=%s\n目标 %s\n", *deviceID, *clientID, *wsURL)
	fmt.Println("打「小智」或「你好小智」唤醒（可同行带查询，如「小智，我想听故事」）；exit / Ctrl-D 退出")

	sess := sim.NewSession(sim.Config{URL: *wsURL, DeviceID: *deviceID, ClientID: *clientID},
		os.Stdin, os.Stdout, verboseWriter, nil)
	if err := sess.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "模拟器退出: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("已退出。")
}
