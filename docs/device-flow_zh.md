# xiaozhi 真实设备全链路：上电 → 注册绑定 → 连接 → 对话（逐字段版）

依据：固件 `main/ota.cc`、`main/boards/common/board.cc`、`main/application.cc`、`main/protocols/websocket_protocol.cc`；服务端 `core/connection.py`、`core/websocket_server.py`、`config/manage_api_client.py`、`core/handle/receiveAudioHandle.py`。以下所有字段均从源码核对。

## 0. 角色与端口速查

| 端口 | 服务 | 说明 |
|---|---|---|
| :8003 | HTTP/OTA | `POST /xiaozhi/ota/` 版本检查；文件模式另有 `/xiaozhi/ota/download/{file}` 固件下载 |
| :8000 | WebSocket | `/xiaozhi/v1/` 对话通道（server 不校验路径，靠 device-id 认设备） |
| :8080 | manager-web | 智控台面板（绑定设备在此操作） |
| manager-api | Java 内部 API | 智控台模式下 xiaozhi-server 的配置源（`/config/agent-models` 等） |

---

## 1. 设备身份：三个标识的来源

| 标识 | 生成 | 持久化 | 用途 |
|---|---|---|---|
| **Device-Id** | WiFi STA MAC（以太网板用 ETH MAC），小写冒号分隔 `aa:bb:cc:dd:ee:ff`（`system_info.cc:35-47`） | 硬件固有 | **设备唯一主体**：OTA 头、WS 头/查询参数、绑定关系的 key |
| **Client-Id** | 首次启动用硬件 RNG 生成规范 UUID v4（`board.cc:14-43`） | NVS `board/uuid`，擦 NVS/重刷才变 | 客户端实例标识，参与 token 签名 |
| **Serial-Number** | 板级可选（部分方案有安全芯片序列号） | 板配置 | 有则 `Activation-Version: 2`，无则 `1`（`ota.cc:70`） |

---

## 2. 上电状态机总览

```
Unknown → Starting → WifiConfiguring → Activating(OTA检查) → [Upgrading] → Idle
Idle ⇄ Connecting ⇄ Listening ⇄ Speaking        （另有 Notifying / AudioTesting / FatalError）
```

每次开机**必做** OTA 检查（`application.cc:365-380` ActivationTask），不存在跳过；失败重试 10 次（10s 起指数退避，`application.cc:443-489`），仍失败进 FatalError。

---

## 3. OTA 版本检查 `POST /xiaozhi/ota/`

### 3.1 请求头（`ota.cc:65-82` SetupHttp）

| Header | 值 | 说明 |
|---|---|---|
| `Device-Id` | MAC | 设备唯一标识（server 侧必填校验） |
| `Client-Id` | UUID | 客户端实例 |
| `Serial-Number` | 板级序列号 | 可选；有则同时令 `Activation-Version: 2` |
| `Activation-Version` | `1` 或 `2` | 激活协议版本 |
| `User-Agent` | `BOARD_NAME/固件版本` | 如 `bread-compact-wifi/1.0.0` |
| `Accept-Language` | `zh_CN` 等 | 语言 |
| `Content-Type` | `application/json` | body 为 JSON |

方法：body 非空用 POST，空则 GET（`ota.cc:104`）。

### 3.2 请求 body（`board.cc:61-140` GetSystemInfoJson，真实字段全集）

```json
{
  "version": 2,                          // system-info 协议版本，固定 2
  "language": "zh_CN",                   // 固件语言
  "flash_size": 4194304,                 // flash 总字节（server 用于固件适配）
  "minimum_free_heap_size": "123456",    // 最小剩余堆（诊断用）
  "mac_address": "aa:bb:cc:dd:ee:ff",    // = Device-Id
  "uuid": "00000000-...",                // = Client-Id
  "chip_model_name": "esp32s3",          // 芯片型号
  "chip_info": {
    "model": 1, "cores": 2,              // esp_chip_info() 原样：型号/核数
    "revision": 0, "features": 0         // 版本号/特性位图
  },
  "application": {
    "name": "xiaozhi",                   // esp_app_desc.project_name
    "version": "1.0.0",                  // 当前固件版本（与响应比较用）
    "compile_time": "2021-01-01T00:00:00Z",
    "idf_version": "5.1-dev",            // 编译所用 ESP-IDF
    "elf_sha256": "ab12…"                // 固件 ELF 的 SHA256 hex（64 字符，定位唯一构建）
  },
  "partition_table": [ { "label": "app", "type": 1, "subtype": 2, "address": 65536, "size": 1048576 } ],
  "ota": { "label": "ota_0" },           // OTA 分区标签
  "board": { "type": "bread-compact-wifi", "mac": "…" }   // 板型与板自定义信息
}
```

server 实际读取宽松：文件模式主要读**头**（Device-Id/Client-Id 必填）；body 中真正被用到的只有 `application.version`、`board.type` 等少数字段（选固件、记录设备型号）。

### 3.3 响应（`ota.cc:129-257` 逐节点消费）

```json
{
  "server_time": { "timestamp": 1760600000000, "timezone_offset": 480 },
  "firmware":    { "version": "1.2.0", "url": "http://ip:8003/xiaozhi/ota/download/xxx.bin", "force": 0 },
  "websocket":   { "url": "ws://ip:8000/xiaozhi/v1/", "token": "<JWT 或空串>", "version": 1 },
  "mqtt":        { "endpoint": "...", "client_id": "...", "username": "...", "password": "...", "publish_topic": "...", "keepalive": 240 }
}
```

| 节点 | 设备侧行为 |
|---|---|
| `server_time.timestamp` | 毫秒级时间戳：`settimeofday()` 校准系统时钟（无 RTC 的设备必需，TLS 也依赖） |
| `server_time.timezone_offset` | **分钟**数：`ts += offset×60×1000` 折算本地时间 |
| `mqtt.*` | 整节逐 key 写入 NVS `mqtt` 命名空间；**出现即选 MQTT 协议** |
| `websocket.url/token/version` | 逐 key 写 NVS `websocket` 命名空间（version 决定二进制帧封装，见 §7.1）；出现且无 mqtt 节则选 WS 协议 |
| `firmware.version/url` | 与当前版本比较，更新才置 `has_new_version_`；随后进入 Upgrading 下载烧写到 OTA 分区，校验通过重启 |
| `firmware.force` | `1` = 无条件升级（跳过版本比较） |

协议选择优先级（`application.cc:531-545`）：`mqtt` 节 > `websocket` 节 > 都没有默认 MQTT。

**token 说明**（server `core/auth.py`）：`server.auth.enabled=false`（默认）时为空串；开启时格式 `{HMAC-SHA256(client_id|device_id|ts)}.{ts}`，默认 30 天有效，白名单设备免验。

---

## 4. 激活流程（旧云端遗留，自建 server 不触发）

OTA 响应若含 `activation` 节（tenclass 云端行为）：

```json
"activation": { "message": "...", "code": "123456", "challenge": "<随机串>", "timeout_ms": 300000 }
```

设备流程（`ota.cc:504-542`）：显示/播报 code 与 message → 用户在网页输入 code → 设备 `POST <ota_url>/activate`，body：

```json
{ "algorithm": "hmac-sha256", "serial_number": "<SN>", "challenge": "<原样回传>", "hmac": "<SN 与 challenge 的 HMAC>" }
```

`202` = 审核中，3s 后重发；`200` = 激活成功，重跑版本检查。自建 xiaozhi-esp32-server **从不下发 activation 节**，此流程不会出现。

---

## 5. WebSocket 连接与 hello 握手

### 5.1 连接

```
GET ws://ip:8000/xiaozhi/v1/            ← 路径不校验，任意 path 均可
    ?device-id=aa:bb:cc:dd:ee:ff        ← 必需（或等价 HTTP 头）
    ?client-id=00000000-...
Header: Authorization: Bearer <token>   ← token 非空才带（auth 关闭的部署为空串）
        Device-Id / Client-Id / User-Agent / Protocol-Version: 1   ← 真设备会带的其余头
```

server 侧（`websocket_server.py:76-110, 210-231`）：无 device-id → 回一句提示文本并关连接；auth 开启才验 token（白名单 MAC 免验）。

### 5.2 hello（连上后设备立即发，等应答最长 **10s**，`websocket_protocol.cc:176-190`）

设备 → server：

```json
{
  "type": "hello",
  "version": 1,                          // = NVS websocket/version，默认 1；决定二进制帧封装（§7.1）
  "transport": "websocket",
  "audio_params": {
    "format": "opus",                     // 上行编码
    "sample_rate": 16000,                 // 上行采样率
    "channels": 1,
    "frame_duration": 60                  // 每帧毫秒数
  },
  "features": {
    "mcp": true,                          // 声明支持 MCP → server 随即异步下发 initialize
    "aec": false                          // 服务端回声消除（true 时 server 做 AEC，设备转 realtime 模式）
  }
}
```

server → 设备（应答，`helloHandle.py:42-65`）：

```json
{
  "type": "hello",
  "version": 1,
  "transport": "websocket",
  "audio_params": { "format": "opus", "sample_rate": 24000, "channels": 1, "frame_duration": 60 },
  "session_id": "<uuid4>"
}
```

- **session_id**：本轮连接会话标识，设备后续所有 JSON 必须回填此值
- `audio_params.sample_rate`：**下行** TTS 采样率（server 自己的配置，不受请求影响，默认 24000），设备按需重采样
- server 消费的请求字段只有 `audio_params`（format 特殊值 `pcm` 时下行改裸 PCM）与 `features`（mcp/aec）；version/transport/token 不校验

---

## 6. 绑定（你说的"设备校验码"）——仅智控台模式

### 6.1 两种部署模式

| 模式 | 判定 | 行为 |
|---|---|---|
| 文件模式 | `read_config_from_api: false`（默认） | **无注册/绑定概念**，任意 device-id 直连即用 |
| 智控台模式 | `read_config_from_api: true` | 设备须绑定到某账号（agent）才进对话 |

### 6.2 智控台模式下的内部流程（`connection.py:800-849`）

WS 连接建立后，server 后台异步调 manager-api：

```
POST {manager-api}/config/agent-models
{ "macAddress": "aa:bb:cc:dd:ee:ff",     ← Device-Id
  "clientId":  "00000000-...",           ← Client-Id
  "selectedModule": { ... } }            ← server 当前选中模块串（LLM/TTS/ASR 等）
```

业务码（`manage_api_client.py:107-113`）：

| code | 含义 | server 行为 |
|---|---|---|
| `0` | 已绑定 | 下发差异化配置（该 agent 配的 LLM/TTS/音色/提示词等），`need_bind=false`，进正常流程 |
| `10041` | 设备不存在 | `need_bind=true`、`bind_code=None` |
| `10042` | 设备存在但未绑定 | `need_bind=true`、`bind_code=msg`（**6 位数字验证码**） |

另调 `POST /config/correct-words {macAddress}` 取替换词（已绑定才有意义）。

### 6.3 未绑定期间连接内发生什么（`connection.py:337-366`、`receiveAudioHandle.py:144-179`）

- 设备发来的一切消息**全部丢弃**
- 每 **60 秒**（`bind_prompt_interval`）播一轮绑定提示：
  1. `{"type":"stt","text":"请登录控制面板，输入945284，绑定设备。"}`（文本先行）
  2. 提示音 `config/assets/bind_code.wav` 的 opus 包（伪装成 TTS 音频轮次）
  3. 逐位播放 `config/assets/bind_code/{d}.wav`（数字 0-9 六个包）
  4. `tts stop` 收尾
- 设备端表现：收到非 hello 的首帧 stt + 一轮 TTS 音频
- `bind_code=None`（10041 设备不存在）时的提示词不同：「没有找到该设备的版本信息，请正确配置 OTA地址，然后重新编译固件。」

### 6.4 绑定完成与生效

用户在 manager-web 面板输入 6 位码 → manager-api 把 device-id 挂到该账号 agent 下。**正在连接中的会话不会自动解锁**（`bind_completed_event` 只在取配置成功时 set）→ 设备**重连**（断开重拨或下次唤醒重连）后，`/config/agent-models` 返回 code=0，进正常对话。绑定关系按 device-id 永久保存。

---

## 7. 对话主循环

### 7.1 二进制音频帧封装（由 hello `version` 决定，`protocol.h:19-33`）

| version | 封装 |
|---|---|
| 1（默认） | 二进制帧 = **裸 opus 包**，无任何前缀 |
| 2 | 16 字节大端头：`u16 version(2) \| u16 type(0=opus,1=json) \| u32 reserved \| u32 timestamp_ms \| u32 payload_size` |
| 3 | 4 字节头：`u8 type(0=opus) \| u8 reserved \| u16 payload_size` |

上行：16kHz/单声道/60ms/帧（960 样本），VBR。下行：采样率 = server 配置（默认 24kHz）/60ms/帧。

### 7.2 消息全表（文本帧 JSON，按 `type` 分发）

**设备 → server：**

| 消息 | 结构 | 时机 |
|---|---|---|
| 唤醒上报 | `{"session_id","type":"listen","state":"detect","text":"你好小智"}` | 唤醒词命中后（此前可先发缓存的唤醒词 opus 包，`CONFIG_SEND_WAKE_WORD_DATA` 默认 y） |
| 开始拾音 | `{"session_id","type":"listen","state":"start","mode":"auto\|manual\|realtime"}` | 进入 Listening；auto=server VAD 断句、manual=客户端说停才停、realtime=持续流 |
| 停止拾音 | `{"session_id","type":"listen","state":"stop"}` | manual 模式说完 / 按钮触发 |
| 文本输入 | `{"session_id","type":"listen","state":"detect","text":"<任意文本>"}` | 跳过 ASR 直进对话（server `listenMessageHandler.py`：命中 `wakeup_words` 配置只回问候，否则 text 直接喂 LLM） |
| 打断 | `{"session_id","type":"abort"[,"reason":"wake_word_detected"]}` | 播放中再唤醒 |
| MCP 应答 | `{"session_id","type":"mcp","payload":{JSON-RPC 2.0}}` | 回应 server 的 initialize/tools/list/tools/call |

**server → 设备：**

| 消息 | 结构 | 时机 |
|---|---|---|
| stt | `{"session_id","type":"stt","text":"..."}` | 识别**最终结果**（无中间结果，每轮一条） |
| tts 开始 | `{"session_id","type":"tts","state":"start"}` | 本轮 TTS 启动（随 stt 同发） |
| 字幕 | `{"session_id","type":"tts","state":"sentence_start","text":"..."}` | 每句开始（无 sentence_end） |
| tts 结束 | `{"session_id","type":"tts","state":"stop"}` | 本轮播完；**收到 abort 后也回它作确认** |
| 情绪 | `{"session_id","type":"llm","text":"🙂","emotion":"happy"}` | 每轮一次 |
| MCP 请求 | `{"session_id","type":"mcp","payload":{jsonrpc,id,method,params}}` | initialize(id=1, capabilities.vision 含 url+token) → tools/list(id=2) → tools/call(id=N) |
| IoT | `{"type":"iot","commands":[{name,method,parameters}]}` | LLM 意图触发设备控制 |
| 系统 | `{"session_id","type":"system","command":"reboot"}` | 唯一命令 |
| 告警 | `{"session_id","type":"alert","status","message","emotion"}` | 三字段均须字符串 |
| 通知 | `{"type":"notify","audio_url","subtitles":[{start_ms,text}]}` | 设备自行 HTTP 拉流播报 |
| 音频 | 二进制帧（§7.1） | tts start 与 stop 之间，一句多个 60ms 包 |

### 7.3 一次完整语音轮（auto 模式）

```
[唤醒词命中] → (发唤醒词opus缓存包) → listen detect(唤醒词)
            → listen start(mode=auto) → 开麦，持续发 16k opus 帧
[server Silero VAD：阈值0.5/0.3、5帧滑窗取3、静默≥200ms 判说完]
            → ASR → stt(最终文本) + tts start → LLM → llm(情绪)
            → 逐句 tts sentence_start + 24k opus 包
[设备收 tts start 即关麦进 Speaking] → tts stop → auto 回 Listening 连续对话
[播放中再唤醒] → abort(reason) → server 回 tts stop 确认 → 新一轮
```

### 7.4 MCP over WebSocket

- 触发：hello `features.mcp=true` → server 异步下发 `initialize`（`protocolVersion:"2024-11-05"`，capabilities 含 `vision:{url,token}` 供设备上传图片识别）
- 设备回 `result.serverInfo`；随后 `tools/list`（可能带 cursor 分页）→ 回工具数组（name/description/inputSchema）
- 运行中 server 按需 `tools/call`（params.name/arguments）→ 设备回 `result.content[0].text + isError`；错误回 `payload.error{code,message}`
- server 侧超时 `tool_call_timeout`（默认 30s）

---

## 8. 保活与断开

| 机制 | 值 | 说明 |
|---|---|---|
| WS 协议层 ping | **无** | 双方都不发（`ping_interval=None`） |
| 应用层 ping | 默认关 | `{"type":"ping"}` 仅 `enable_websocket_ping: true` 才回 pong |
| 活跃判定 | 任意消息 | 收发任何消息（含音频）都刷新 `last_activity_time` |
| 静默断开 | **120s** 无语音（`close_connection_no_voice_time`） | server 先用 end_prompt 自问自答播一句告别语，播完 `tts stop` 后主动 `close()` |
| 硬超时 | 120+60s | 无任何活动直接关 |
| 设备侧超时 | 120s 无下行 | `IsTimeout()` 判通道失效 |
| 重连 | 下次唤醒触发 | 设备用 NVS 缓存的 url/token 重拨，完整重走 hello（新 session_id），**不重跑 OTA** |
| MQTT 模式 | keepalive 240s（PINGREQ） | 断开后 60s 自动重连 |

---

## 9. 固件升级（firmware 节触发）

`firmware.version` 比当前新（或 `force:1`）→ 设备进 Upgrading：HTTP 流式下载 `firmware.url` → 逐块写 OTA 分区（`esp_ota_write`）→ 校验镜像 → `esp_ota_set_boot_partition` → 重启。版本号在成功运行后由 `MarkCurrentVersionValid()` 落盘（防变砖回滚）。

---

## 10. 一图总结

```
上电 → 连WiFi → POST OTA(系统信息JSON) ─┬→ server_time: 校时
                                        ├→ websocket{url,token} → 存NVS
                                        ├→ mqtt{...} → 存NVS(优先)
                                        └→ firmware{version,url,force} → 升级流程
     → WS connect(url + ?device-id&client-id + Bearer token)
     → hello(声明能力) ↔ hello(拿 session_id、下行音频参数)
     → [智控台] manager-api 查绑定 ─┬→ 未绑定: 丢消息+每60s播6位码 → 面板绑定 → 重连
     │                              └→ 已绑定: 下发差异化配置(LLM/TTS/提示词)
     → 对话循环: listen/opus上行 → VAD断句 → ASR/LLM/TTS → stt/llm/字幕/opus下行
     → 打断: abort → tts stop 确认
     → 静默120s: 告别语+close → 设备下次唤醒重连(不重跑OTA)
```
