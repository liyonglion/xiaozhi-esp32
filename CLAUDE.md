# xiaozhi-esp32 本地环境

## 构建环境（ESP-IDF v6.1，EIM 安装）

- 激活：`source /Users/lion/.espressif/tools/activate_idf_v6.1.sh`
  - 该脚本自带 venv，不要再用 `export.sh`
  - IDF_PATH: `/Users/lion/.espressif/v6.1/esp-idf`
  - Python venv: `/Users/lion/.espressif/tools/python/v6.1/venv`（内置 pyserial，读串口直接用这个 venv 的 python，系统 python3 没有 pyserial 且 PEP 668 禁止 pip 装包）
- 项目要求 IDF ≥ 6.0.1（推荐 6.1），不支持 5.x

## 硬件与烧录

- 目标芯片：`esp32s3`（已 set-target，sdkconfig 就绪）
- 板型：`CONFIG_BOARD_TYPE_BREAD_COMPACT_WIFI=y`（面包板，NoAudioCodec 软件 音量缩放）
- 串口：`/dev/cu.usbserial-A5069RR4`
- 常用命令：
  ```
  idf.py -p /dev/cu.usbserial-A5069RR4 build
  idf.py -p /dev/cu.usbserial-A5069RR4 flash monitor
  ```
- 构建产物在 `build/`（xiaozhi.elf、generated_assets.bin 等）

## 服务器

- 自建 xiaozhi 服务器：14.103.201.147（OTA 端口 8003、TTS 适配器、assets 有坑，详见全局记忆 xiaozhi-selfhosted-deployment.md）

## Agent skills

### Issue tracker

Issues 记录在本 fork（liyonglion/xiaozhi-esp32）的 GitHub Issues，用 gh CLI 读写。见 `docs/agents/issue-tracker.md`。

### Triage labels

保持默认五个标签（needs-triage / needs-info / ready-for-agent / ready-for-human / wontfix）。见 `docs/agents/triage-labels.md`。

### Domain docs

单上下文：根目录 `GLOSSARY.md` + `docs/adr/`。见 `docs/agents/domain.md`。
