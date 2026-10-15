# xiaozhi-esp32

ESP32 语音助手固件仓库。本词汇表覆盖固件、服务端（xiaozhi-esp32-server）与模拟器（simulator/）共用的领域术语。

## Language

**模拟器 (Simulator)**:
运行在开发机上的 Go 终端程序，模拟一台 xiaozhi 设备与服务端的完整对话，用于调试服务端。位于 `simulator/`。
_Avoid_: 测试客户端、mock 设备

**唤醒词 (Wake Word)**:
触发设备进入待输入态的口令。终端输入「小智」或「你好小智」均视为唤醒；上报服务端时一律用规范形「你好小智」，因其必须匹配服务端 `wakeup_words` 配置。
_Avoid_: 触发词、热词

**唤醒上报 (Wake Report)**:
唤醒后设备发给服务端的 listen detect 消息，文本为规范唤醒词。服务端据此回问候语，而不是把唤醒词当成提问交给 LLM。
_Avoid_: 唤醒通知

**待输入态 (Listening)**:
唤醒后等待用户输入查询的状态，对应真设备的开麦收音。
_Avoid_: 说话态、录音态

**播放态 (Speaking)**:
正在播放服务端下发的 TTS 回复；此状态下再打唤醒词即触发打断。
_Avoid_: 播报态

**打断 (Barge-in)**:
播放态中再次唤醒：本地停止播放并向服务端发 abort，服务端回 tts stop 结束本轮。
_Avoid_: 插话、截断

**查询 (Query)**:
唤醒后用户输入的文本，以 listen detect 的 text 字段发送；服务端跳过 ASR 直接进入对话。
_Avoid_: 提问、语句

**会话 (Session)**:
由服务端 hello 应答下发的 session_id 标识的连接内对话上下文。
_Avoid_: 连接、对话

**OTA 检查 (OTA Check)**:
程序启动时向 OTA 端点上报设备信息、换取 WebSocket 地址与令牌的一次性流程；`--url` 直连是它的逃生口。
_Avoid_: OTA 升级（本仓库语境中 OTA 检查不升级固件）
