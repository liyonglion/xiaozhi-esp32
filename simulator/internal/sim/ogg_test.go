package sim

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

// —— RFC 3533 页解析（测试侧独立实现，不复用生产代码）——

// oggPage 解析出的一页字段。
type oggPage struct {
	magic      string
	version    byte
	headerType byte
	granule    uint64
	serial     uint32
	seq        uint32
	crc        uint32
	segments   int
	lacing     []byte
	payload    []byte
}

// parseOggPagesQuiet 切分页序列；数据不完整/未落齐时返回 nil（轮询用）。
func parseOggPagesQuiet(data []byte) []oggPage {
	var pages []oggPage
	for len(data) > 0 {
		if len(data) < 27 || string(data[:4]) != "OggS" {
			return nil
		}
		nSeg := int(data[26])
		var payloadLen int
		for _, l := range data[27 : 27+nSeg] {
			payloadLen += int(l)
		}
		pageLen := 27 + nSeg + payloadLen
		if pageLen > len(data) {
			return nil
		}
		pages = append(pages, oggPage{
			magic:      string(data[:4]),
			version:    data[4],
			headerType: data[5],
			granule:    binary.LittleEndian.Uint64(data[6:14]),
			serial:     binary.LittleEndian.Uint32(data[14:18]),
			seq:        binary.LittleEndian.Uint32(data[18:22]),
			crc:        binary.LittleEndian.Uint32(data[22:26]),
			segments:   nSeg,
			lacing:     append([]byte(nil), data[27:27+nSeg]...),
			payload:    append([]byte(nil), data[27+nSeg:pageLen]...),
		})
		data = data[pageLen:]
	}
	return pages
}

// parseOggPages 按页头与段表切分字节流为页序列；数据非法时失败。
func parseOggPages(t *testing.T, data []byte) []oggPage {
	t.Helper()
	if pages := parseOggPagesQuiet(data); pages != nil || len(data) == 0 {
		return pages
	}
	t.Fatalf("字节流不是完整 Ogg 页序列（魔数/页长不符）, 共 %d 字节", len(data))
	return nil
}

// 首包输出 ID 头页 + comment 头页 + 音频页（RFC 7845 §3）。
func TestOggStreamFirstPacketEmitsHeadPages(t *testing.T) {
	packet := []byte{0x01, 0x02, 0x03, 0x04}
	var sink bytes.Buffer
	stream := newOggOpusStream(&sink)
	stream.writePacket(packet)

	pages := parseOggPages(t, sink.Bytes())
	if len(pages) != 3 {
		t.Fatalf("首包应产生 3 页（ID+comment+音频）, 实际 %d", len(pages))
	}

	id, tags, audio := pages[0], pages[1], pages[2]

	// ID 头页：BOS、granule 0、OpusHead 布局（RFC 7845 §5.1）。
	if id.headerType != 0x02 {
		t.Errorf("ID 头页 header_type = %#x, 期望 BOS 0x02", id.headerType)
	}
	if id.granule != 0 {
		t.Errorf("ID 头页 granule = %d, 期望 0", id.granule)
	}
	if !bytes.HasPrefix(id.payload, []byte("OpusHead")) {
		t.Fatalf("ID 头页载荷应以 OpusHead 开头, 实际 %q", id.payload)
	}
	if got := id.payload[8]; got != 1 {
		t.Errorf("OpusHead version = %d, 期望 1", got)
	}
	if got := id.payload[9]; got != 1 {
		t.Errorf("OpusHead channels = %d, 期望 1（单声道）", got)
	}
	if got := binary.LittleEndian.Uint16(id.payload[10:12]); got != 0 {
		t.Errorf("OpusHead pre-skip = %d, 期望 0", got)
	}
	if got := binary.LittleEndian.Uint32(id.payload[12:16]); got != 24000 {
		t.Errorf("OpusHead input sample rate = %d, 期望 24000", got)
	}

	// comment 头页：OpusTags + vendor 串（RFC 7845 §5.2）。
	if tags.headerType != 0 {
		t.Errorf("comment 头页 header_type = %#x, 期望 0", tags.headerType)
	}
	if tags.granule != 0 {
		t.Errorf("comment 头页 granule = %d, 期望 0", tags.granule)
	}
	if !bytes.HasPrefix(tags.payload, []byte("OpusTags")) {
		t.Fatalf("comment 头页载荷应以 OpusTags 开头, 实际 %q", tags.payload)
	}
	vendorLen := binary.LittleEndian.Uint32(tags.payload[8:12])
	vendor := string(tags.payload[12 : 12+vendorLen])
	if !strings.Contains(vendor, "xiaozhi") {
		t.Errorf("vendor = %q, 期望含 xiaozhi 标识", vendor)
	}
	if got := binary.LittleEndian.Uint32(tags.payload[12+vendorLen:]); got != 0 {
		t.Errorf("user comment 数 = %d, 期望 0", got)
	}

	// 音频页：载荷即裸 opus 包，granule 48kHz 基准（60ms = 2880）。
	if !bytes.Equal(audio.payload, packet) {
		t.Errorf("音频页载荷 = %v, 期望原样包 %v", audio.payload, packet)
	}
	if audio.granule != 2880 {
		t.Errorf("首音频页 granule = %d, 期望 2880", audio.granule)
	}
	if audio.headerType&0x04 != 0 { // 本流不写 EOS，靠 stdin EOF 结束
		t.Errorf("音频页不应带 EOS 标志")
	}

	// 页序号从 0 连续递增；同一流 serial 一致。
	for i, p := range pages {
		if p.seq != uint32(i) {
			t.Errorf("第 %d 页 seq = %d", i, p.seq)
		}
		if p.version != 0 {
			t.Errorf("第 %d 页 version = %d, 期望 0", i, p.version)
		}
		if p.serial != pages[0].serial {
			t.Errorf("第 %d 页 serial 与首页不一致", i)
		}
	}
}

// 每个 60ms 包一页，granuleposition 以 2880 步进单调递增。
func TestOggStreamGranuleAdvancesPerPacket(t *testing.T) {
	var sink bytes.Buffer
	stream := newOggOpusStream(&sink)
	for i := 0; i < 3; i++ {
		stream.writePacket([]byte{byte(i)})
	}
	pages := parseOggPages(t, sink.Bytes())
	if len(pages) != 5 {
		t.Fatalf("3 包应产生 5 页, 实际 %d", len(pages))
	}
	for i, want := range []uint64{2880, 5760, 8640} {
		if got := pages[2+i].granule; got != want {
			t.Errorf("第 %d 个音频包 granule = %d, 期望 %d", i+1, got, want)
		}
	}
}

// 超过 255 字节的包用多段 lacing 表示（RFC 3533 §3.2.1）。
func TestOggStreamLargePacketLacing(t *testing.T) {
	packet := bytes.Repeat([]byte{0xAB}, 600)
	var sink bytes.Buffer
	stream := newOggOpusStream(&sink)
	stream.writePacket(packet)

	pages := parseOggPages(t, sink.Bytes())
	if len(pages) != 3 {
		t.Fatalf("首包应产生 3 页, 实际 %d", len(pages))
	}
	audio := pages[2]
	want := []byte{255, 255, 90} // 600 = 255+255+90（RFC 3533 §3.2.1）
	if !bytes.Equal(audio.lacing, want) {
		t.Errorf("600 字节包 lacing = %v, 期望 %v", audio.lacing, want)
	}
	if !bytes.Equal(audio.payload, packet) {
		t.Errorf("大包载荷与输入不一致（%d 字节）", len(audio.payload))
	}
}

// 长度恰为 255 倍数的包必须补一个 0 终止 lacing 值（RFC 3533 §3.2.1：
// 包的最后一个 lacing 值必须 <255，否则下一包会被并入本包）。
func TestOggStreamPacketMultipleOf255GetsTerminatingLacing(t *testing.T) {
	for _, size := range []int{255, 510} {
		var sink bytes.Buffer
		stream := newOggOpusStream(&sink)
		stream.writePacket(bytes.Repeat([]byte{0x7E}, size))
		stream.writePacket([]byte{0x01})
		pages := parseOggPages(t, sink.Bytes())
		audio := pages[2]
		want := bytes.Repeat([]byte{255}, size/255)
		want = append(want, 0)
		if !bytes.Equal(audio.lacing, want) {
			t.Errorf("%d 字节包 lacing = %v, 期望 %v（补 0 终止）", size, audio.lacing, want)
		}
		if audio.segments != len(want) || audio.segments > 255 {
			t.Errorf("%d 字节包 segments = %d, 期望 %d 且 ≤255", size, audio.segments, len(want))
		}
	}
}

// CRC 用独立实现的参考向量验证（python3 逐位 MSB-first 0x04c11db7）。
func TestOggCRCReferenceVectors(t *testing.T) {
	cases := []struct {
		data []byte
		want uint32
	}{
		{[]byte("OggS"), 0x5fb0a94f},
		{[]byte("OpusHead\x01\x01\x00\x00"), 0xb797adb5},
		{[]byte("123456789"), 0x89a1897f},
	}
	for _, c := range cases {
		if got := oggCRC(c.data); got != c.want {
			t.Errorf("oggCRC(%q) = %#x, 期望 %#x", c.data, got, c.want)
		}
	}
}
