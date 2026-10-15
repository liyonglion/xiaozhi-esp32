package sim

import (
	"encoding/binary"
	"io"
	"math/rand"
)

// Ogg Opus 封装（RFC 7845 + RFC 3533）。纯逻辑，无并发保护：
// 每个 oggOpusStream 只在创建它的播放链路上串行使用。
//
// 约定（见 /tmp 协议事实 §5）：server 下行每包 60ms opus，
// granuleposition 按 48kHz 解码基准推进，60ms = 2880 样本。
const samplesPer48kPacket = 2880

// opusInputSampleRate 仅写入 OpusHead 作信息记录；opus 解码恒为 48kHz，
// 播放端不据此假设（server 配置可改采样率）。
const opusInputSampleRate = 24000

// oggOpusStream 把一轮 TTS 的裸 opus 包封装为 Ogg 字节流写入 w。
// 首个包先输出 ID 头页与 comment 头页；每包一页；不写 EOS，
// 流结束由底层 w 关闭（如 ffplay 的 stdin EOF）表达。
type oggOpusStream struct {
	w       io.Writer
	serial  uint32
	seq     uint32
	granule uint64
	started bool
}

// newOggOpusStream 创建一轮 TTS 的封装器；serial 随机以区分流。
func newOggOpusStream(w io.Writer) *oggOpusStream {
	return &oggOpusStream{w: w, serial: rand.Uint32()}
}

// writePacket 封装一个 opus 包并写出一页；首个包前先写头两页。
func (o *oggOpusStream) writePacket(packet []byte) error {
	if !o.started {
		o.started = true
		if err := o.writeHeadPages(); err != nil {
			return err
		}
	}
	o.granule += samplesPer48kPacket
	return o.writePage(0, o.granule, packet)
}

// writeHeadPages 输出 OpusHead 与 OpusTags 两页（RFC 7845 §5.1/§5.2）。
func (o *oggOpusStream) writeHeadPages() error {
	head := make([]byte, 0, 19)
	head = append(head, "OpusHead"...)
	head = append(head,
		1,    // version
		1,    // channel count
		0, 0, // pre-skip
		0, 0, 0, 0, // input sample rate（下面回填）
		0, 0, // output gain
		0, // mapping family
	)
	binary.LittleEndian.PutUint32(head[12:16], opusInputSampleRate)
	if err := o.writePage(0x02, 0, head); err != nil { // BOS
		return err
	}

	tags := make([]byte, 0, 8+4+len("xiaozhi-simulator")+4)
	tags = append(tags, "OpusTags"...)
	var len4 [4]byte
	binary.LittleEndian.PutUint32(len4[:], uint32(len("xiaozhi-simulator")))
	tags = append(tags, len4[:]...)
	tags = append(tags, "xiaozhi-simulator"...)
	binary.LittleEndian.PutUint32(len4[:], 0) // user comment 数
	tags = append(tags, len4[:]...)
	return o.writePage(0, 0, tags)
}

// finishStream 输出空 EOS 页收尾当前流（granule 停在最后一个完成的包）。
func (o *oggOpusStream) finishStream() error {
	return o.writePage(0x04, o.granule, nil)
}

// writePage 按页头+段表+载荷组装一页并计算 CRC（RFC 3533 §3/§4）。
// 长度恰为 255 倍数的包补一个 0 终止 lacing 值（§3.2.1：末段必须 <255）。
func (o *oggOpusStream) writePage(headerType byte, granule uint64, payload []byte) error {
	nSeg := (len(payload) + 254) / 255
	if len(payload) > 0 && len(payload)%255 == 0 {
		nSeg++
	}
	page := make([]byte, 0, 27+nSeg+len(payload))
	page = append(page, "OggS"...)
	page = append(page, 0, headerType)
	var num8 [8]byte
	binary.LittleEndian.PutUint64(num8[:], granule)
	page = append(page, num8[:]...)
	binary.LittleEndian.PutUint32(num8[:4], o.serial)
	page = append(page, num8[:4]...)
	binary.LittleEndian.PutUint32(num8[:4], o.seq)
	page = append(page, num8[:4]...)
	page = append(page, 0, 0, 0, 0) // CRC 占位，最后回填
	page = append(page, byte(nSeg))
	for i := 0; i < nSeg; i++ {
		seg := len(payload) - i*255 // 终止段时为 0
		if seg > 255 {
			seg = 255
		}
		page = append(page, byte(seg))
	}
	page = append(page, payload...)
	binary.LittleEndian.PutUint32(page[22:26], oggCRC(page))
	o.seq++
	_, err := o.w.Write(page)
	return err
}

// —— Ogg CRC32：多项式 0x04c11db7（MSB-first、初值 0、无反转/异或），
// 与标准库 crc32 的 Castagnoli/IEEE 均不同，故自建表（RFC 3533 §4）——

var oggCRCTable [256]uint32

func init() {
	for i := uint32(0); i < 256; i++ {
		r := i << 24
		for bit := 0; bit < 8; bit++ {
			if r&0x80000000 != 0 {
				r = r<<1 ^ 0x04c11db7
			} else {
				r <<= 1
			}
		}
		oggCRCTable[i] = r
	}
}

// oggCRC 计算一页（CRC 字段为零时）的校验和。
func oggCRC(data []byte) uint32 {
	var crc uint32
	for _, b := range data {
		crc = crc<<8 ^ oggCRCTable[byte(crc>>24)^b]
	}
	return crc
}
