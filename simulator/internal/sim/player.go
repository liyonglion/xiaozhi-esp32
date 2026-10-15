package sim

import (
	"fmt"
	"io"
	"os/exec"
	"sync"
)

// ffplayArgs 低缓冲实时播放参数：-nodisp 无窗口、-autoexit 播完退出、
// -fflags nobuf -probesize 32 最小化探测缓冲、-loglevel quiet 静音日志、
// 末尾 - 表示从 stdin 读。
var ffplayArgs = []string{"-nodisp", "-autoexit", "-loglevel", "quiet", "-fflags", "nobuf", "-probesize", "32", "-"}

// ffplayPlayer 把 Ogg 字节流喂给 ffplay 子进程 stdin 实时播放。
// 封装由上游 oggPlayer 完成——本类型只管进程生命周期。
// 首个字节到达时惰性起播；Finish 优雅收尾（关 stdin 让余量播完）；
// Stop 立即 kill（打断语义，见 #6）。ffplay 缺失时降级为丢弃并提示一次。
type ffplayPlayer struct {
	out      io.Writer // 提示输出（终端）
	lookPath func(name string) (string, error)
	newCmd   func(bin string) *exec.Cmd

	mu     sync.Mutex
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	warned bool // 缺失提示只打一次
	broken bool // 启动/写入失败后降级
}

// newFFplayPlayer 构造 ffplay 播放器。lookPath/newCmd 为测试注入点，
// 缺省用 exec.LookPath 与真实 ffplay 命令。
func newFFplayPlayer(out io.Writer, lookPath func(string) (string, error), newCmd func(string) *exec.Cmd) *ffplayPlayer {
	if lookPath == nil {
		lookPath = exec.LookPath
	}
	if newCmd == nil {
		newCmd = func(bin string) *exec.Cmd { return exec.Command(bin, ffplayArgs...) }
	}
	return &ffplayPlayer{out: out, lookPath: lookPath, newCmd: newCmd}
}

// start 起播：定位 ffplay、起子进程、建 Ogg 封装器。失败则提示并降级。
func (p *ffplayPlayer) start() {
	bin, err := p.lookPath("ffplay")
	if err != nil {
		p.degrade("未找到 ffplay，无法播放音频：请安装后重试（macOS: brew install ffmpeg）；本次降级为仅显示字幕")
		return
	}
	cmd := p.newCmd(bin)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		p.degrade("创建 ffplay stdin 失败: %v", err)
		return
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close() // 启动失败也要释放管道写端
		p.degrade("启动 ffplay 失败（%s）: %v", bin, err)
		return
	}
	p.cmd = cmd
	p.stdin = stdin
}

// degrade 打印一次提示并停用播放。
func (p *ffplayPlayer) degrade(format string, args ...any) {
	if !p.warned {
		p.warned = true
		fmt.Fprintf(p.out, "（提示）"+format+"\n", args...)
	}
	p.broken = true
}

// Play 写入一段 Ogg 字节：首次调用起播 ffplay。
// 写 stdin 不持锁：ffplay 卡死导致管道写满时，Stop（kill）仍能立即进行，
// 被阻塞的写随后以 EPIPE 返回。
func (p *ffplayPlayer) Play(b []byte) {
	p.mu.Lock()
	if p.broken {
		p.mu.Unlock()
		return
	}
	if p.cmd == nil {
		p.start()
		if p.broken {
			p.mu.Unlock()
			return
		}
	}
	stdin := p.stdin
	p.mu.Unlock()

	if _, err := stdin.Write(b); err != nil {
		p.mu.Lock()
		// 仅当仍是同一进程时按故障处理；Stop/Finish 已换掉进程的 EPIPE 属预期。
		if p.stdin == stdin && !p.broken {
			p.degrade("写入 ffplay 失败（进程可能已退出）: %v", err)
			p.teardown()
		}
		p.mu.Unlock()
	}
}

// Finish 一轮 TTS 结束：关 stdin 让 ffplay 播完余量自行退出，异步回收进程。
// 无看门狗：ffplay 若无视 stdin EOF，进程由会话退出时的 Stop 兜底
// （ponytail: 调试工具，真正挂死再加超时强杀）。Wait 的退出码不区分成功失败。
func (p *ffplayPlayer) Finish() {
	p.mu.Lock()
	if p.cmd == nil {
		p.mu.Unlock()
		return
	}
	// 关闭错误（进程已死时的 EPIPE）不影响收尾语义，静默。
	_ = p.stdin.Close()
	cmd := p.cmd
	p.cmd, p.stdin = nil, nil
	p.mu.Unlock()
	go func() { _ = cmd.Wait() }() // 等待播完可能阻塞数百毫秒，不卡读循环
}

// Stop 立即停止（打断预留，#6）：kill 进程并同步回收。
func (p *ffplayPlayer) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil {
		return
	}
	p.teardown()
}

// teardown kill 并回收当前子进程（调用方持锁）。kill 后关闭 stdin 的
// EPIPE 属预期，静默。
func (p *ffplayPlayer) teardown() {
	_ = p.cmd.Process.Kill()
	_ = p.stdin.Close()
	_ = p.cmd.Wait()
	p.cmd, p.stdin = nil, nil
}

// —— Ogg 封装层 ——
//
// oggPlayer 在任意 Player 前做 Ogg Opus 封装：sink 收到的即完整 Ogg 流
// （每页一次 Play）。Finish/Stop 结束当前流，下一包重发头页。

// oggPlayer 实现 Player：裸 opus 包进，Ogg 页出。
type oggPlayer struct {
	sink Player
	mu   sync.Mutex
	enc  *oggOpusStream
}

// newOggPlayer 包装 sink，为其输出做 Ogg 封装。
func newOggPlayer(sink Player) *oggPlayer {
	return &oggPlayer{sink: sink}
}

// Play 封装一个裸 opus 包并转发给 sink。
// 写 sink 在锁外：sink 阻塞（管道满）时 Stop 不被 Play 拖住。
// ponytail: 打断与首轮写页存在毫秒级交错窗口（旧流尾页可能落入新进程），
// #6 实测若出现跨流脏页再加代际计数。
func (p *oggPlayer) Play(frame []byte) {
	p.mu.Lock()
	if p.enc == nil {
		p.enc = newOggOpusStream(playerWriter{sink: p.sink})
	}
	enc := p.enc
	p.mu.Unlock()
	// 封装是纯内存操作，出错只可能来自 sink；sink 侧自会处理。
	_ = enc.writePacket(frame)
}

// Finish 结束当前流：先补空 EOS 页收尾，再让 sink 收尾，下轮重发头页。
func (p *oggPlayer) Finish() {
	p.mu.Lock()
	enc := p.enc
	p.enc = nil
	p.mu.Unlock()
	if enc != nil {
		_ = enc.finishStream()
	}
	p.sink.Finish()
}

// Stop 立即停止当前流并透传给 sink。
func (p *oggPlayer) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.enc = nil
	p.sink.Stop()
}

// playerWriter 把 io.Writer 写入转成 Player.Play 调用（每页一次）。
type playerWriter struct {
	sink Player
}

func (w playerWriter) Write(b []byte) (int, error) {
	w.sink.Play(b)
	return len(b), nil
}
