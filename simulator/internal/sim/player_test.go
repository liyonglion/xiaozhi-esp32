package sim

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// errMissingFfplay 模拟 exec.LookPath 找不到 ffplay。
var errMissingFfplay = exec.ErrNotFound

// TestFFplayPlayerMissingFfplayDegrades：ffplay 缺失时打印安装指引并降级丢弃，不崩。
func TestFFplayPlayerMissingFfplayDegrades(t *testing.T) {
	out := &bytes.Buffer{}
	p := newFFplayPlayer(out, func(string) (string, error) { return "", errMissingFfplay }, nil)

	p.Play([]byte{0x01}) // 不应 panic
	p.Play([]byte{0x02})
	p.Finish()
	p.Stop()

	got := out.String()
	if !strings.Contains(got, "ffplay") || !strings.Contains(got, "brew install ffmpeg") {
		t.Errorf("应提示 ffplay 缺失与安装指引, 实际输出: %q", got)
	}
	if n := strings.Count(got, "brew install ffmpeg"); n != 1 {
		t.Errorf("安装指引应只打印一次, 实际 %d 次: %q", n, got)
	}
}

// newCatPlayer 构造注入 cat 子进程的播放器（生产装配路径：oggPlayer 包装）：
// 子进程把 stdin 落盘并在 EOF 后写 drained 标记（验证 stdin 关闭与进程收尾）。
func newCatPlayer(t *testing.T, audioPath, donePath string) Player {
	t.Helper()
	raw := newFFplayPlayer(&bytes.Buffer{},
		func(string) (string, error) { return "/usr/bin/true", nil },
		func(bin string) *exec.Cmd {
			return exec.Command("sh", "-c", `cat >> "$1"; echo drained >> "$2"`, "sh", audioPath, donePath)
		})
	return newOggPlayer(raw)
}

// TestFFplayPlayerFinishClosesStdinAndReaps：Finish 关闭 stdin 让子进程自然退出。
func TestFFplayPlayerFinishClosesStdinAndReaps(t *testing.T) {
	dir := t.TempDir()
	audioPath := filepath.Join(dir, "audio.ogg")
	donePath := filepath.Join(dir, "done")
	p := newCatPlayer(t, audioPath, donePath)

	p.Play([]byte{0x01, 0x02})
	p.Play([]byte{0x03})

	// stdin 关闭 → cat 收到 EOF → 写 drained 标记。
	p.Finish()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if b, err := os.ReadFile(donePath); err == nil && strings.Contains(string(b), "drained") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Finish 后子进程未在 2 秒内因 stdin EOF 退出（%s 未生成）", donePath)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 子进程收到的确是 Ogg 封装流：头两页 + 每包一页。
	b, err := os.ReadFile(audioPath)
	if err != nil {
		t.Fatalf("读取子进程落盘音频失败: %v", err)
	}
	pages := parseOggPages(t, b)
	if len(pages) != 5 { // 头 2 页 + 音频 2 页 + Finish 的 EOS 页
		t.Fatalf("2 个包应产生 5 页（头 2 + 音频 2 + EOS）, 实际 %d", len(pages))
	}
	if !bytes.HasPrefix(pages[0].payload, []byte("OpusHead")) || !bytes.HasPrefix(pages[1].payload, []byte("OpusTags")) {
		t.Errorf("前两页应为 OpusHead/OpusTags, 实际 %q / %q", pages[0].payload[:8], pages[1].payload[:8])
	}
	if !bytes.Equal(pages[2].payload, []byte{0x01, 0x02}) || !bytes.Equal(pages[3].payload, []byte{0x03}) {
		t.Errorf("音频页载荷与喂入的包不一致")
	}

	// Finish 后下一轮 Play 应重新起播（新进程、重发头页）。
	p.Play([]byte{0x04})
	var pages2 []oggPage
	deadline = time.Now().Add(2 * time.Second) // 复用上一轮询的期限变量
	for {
		b2, err := os.ReadFile(audioPath)
		if err != nil {
			t.Fatalf("重读音频失败: %v", err)
		}
		if pages2 = parseOggPagesQuiet(b2); len(pages2) == 8 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("下一轮应重发头页再出音频页（共 8 页）, 实际 %d", len(pages2))
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !bytes.HasPrefix(pages2[5].payload, []byte("OpusHead")) {
		t.Errorf("第二轮第一页应为 OpusHead, 实际 %q", pages2[5].payload[:8])
	}
	p.Stop() // 清理
}

// TestFFplayPlayerStopKillsPromptly：Stop 立即杀进程，不等待其自然结束。
func TestFFplayPlayerStopKillsPromptly(t *testing.T) {
	dir := t.TempDir()
	audioPath := filepath.Join(dir, "audio.ogg")
	var cmds []*exec.Cmd
	var mu sync.Mutex
	p := newFFplayPlayer(&bytes.Buffer{},
		func(string) (string, error) { return "/usr/bin/true", nil },
		func(bin string) *exec.Cmd {
			// 先睡 30 秒再读 stdin：若 Stop 用「等 EOF」而非 kill，这里会挂到测试超时。
			cmd := exec.Command("sh", "-c", `sleep 30; cat > "$1"`, "sh", audioPath)
			mu.Lock()
			cmds = append(cmds, cmd)
			mu.Unlock()
			return cmd
		})

	p.Play([]byte{0x01})
	done := make(chan struct{})
	go func() { p.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop 未立即终止（3 秒内未返回）：应 kill 而非等待")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(cmds) == 0 {
		t.Fatal("Stop 前应有子进程被启动")
	}
	if cmds[0].ProcessState == nil {
		t.Error("Stop 后子进程未被回收（Wait 未完成）")
	}
}

// TestFFplayPlayerStopPreemptsBlockedPlay：Play 阻塞在写满的管道上时，
// Stop 必须仍能立即返回（kill 而非排队等锁）——打断（#6）的契约。
func TestFFplayPlayerStopPreemptsBlockedPlay(t *testing.T) {
	dir := t.TempDir()
	audioPath := filepath.Join(dir, "audio.ogg")
	p := newFFplayPlayer(&bytes.Buffer{},
		func(string) (string, error) { return "/usr/bin/true", nil },
		func(bin string) *exec.Cmd {
			// sleep 30：子进程从不读 stdin，管道写满后 Play 会阻塞。
			return exec.Command("sh", "-c", `sleep 30; cat > "$1"`, "sh", audioPath)
		})

	playDone := make(chan struct{})
	go func() {
		defer close(playDone)
		p.Play(bytes.Repeat([]byte{0x55}, 256*1024)) // 远超 64KB 管道容量
	}()
	// 给 Play 时间进入阻塞写（起进程 + 写满管道）。
	time.Sleep(200 * time.Millisecond)

	stopDone := make(chan struct{})
	go func() { p.Stop(); close(stopDone) }()
	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Play 阻塞在管道写时 Stop 未在 2 秒内返回：Stop 不应排在被阻塞的 Play 之后")
	}
	// kill 后阻塞的 Write 以 EPIPE 返回，Play 也必须退出（不重试、不死等）。
	select {
	case <-playDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop kill 后被阻塞的 Play 未返回")
	}
}

// TestOggPlayerReissuesHeadersPerRound：oggPlayer 每轮重发头页、换 serial，
// Finish/Stop 后下一包从新流开始。
func TestOggPlayerReissuesHeadersPerRound(t *testing.T) {
	sink := &fakePlayer{}
	p := newOggPlayer(sink)

	p.Play([]byte{0x01})
	p.Play([]byte{0x02})
	p.Finish()
	p.Play([]byte{0x03})

	frames, _, _ := sink.snapshot()
	var all bytes.Buffer
	for _, f := range frames {
		all.Write(f)
	}
	pages := parseOggPages(t, all.Bytes())
	if len(pages) != 8 { // 轮 1：头 2 页+音频 2 页+EOS；轮 2：头 2 页+音频 1 页
		t.Fatalf("两轮共应 8 页, 实际 %d", len(pages))
	}
	for i, p := range []int{0, 5} {
		if !bytes.HasPrefix(pages[p].payload, []byte("OpusHead")) {
			t.Errorf("第 %d 轮首个载荷应为 OpusHead, 实际 %q", i+1, pages[p].payload[:8])
		}
		if pages[p].seq != 0 {
			t.Errorf("第 %d 轮头页 seq = %d, 期望从 0 重新计数", i+1, pages[p].seq)
		}
	}
	// Finish 以空 EOS 页收尾该轮（RFC 3533 §6.2），granule 停在最后一个完成的包。
	if pages[4].headerType&0x04 == 0 {
		t.Error("Finish 后应输出带 EOS 标志的收尾页")
	}
	if len(pages[4].payload) != 0 || pages[4].granule != 5760 {
		t.Errorf("EOS 页应为空且 granule=5760, 实际 %d 字节 granule=%d", len(pages[4].payload), pages[4].granule)
	}
	if pages[0].serial == pages[5].serial {
		t.Error("两轮流 serial 相同, 期望每轮随机区分")
	}
	if pages[3].granule != 5760 || pages[7].granule != 2880 {
		t.Errorf("granule 每轮应重置递增: 轮1末页 %d, 轮2首包 %d", pages[3].granule, pages[7].granule)
	}
}
