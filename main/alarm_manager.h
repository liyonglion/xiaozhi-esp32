#ifndef _ALARM_MANAGER_H_
#define _ALARM_MANAGER_H_

#include <cJSON.h>
#include <ctime>
#include <expected>
#include <optional>
#include <string>
#include <vector>

// 闹钟/定时器的存储与纯逻辑（到点判定、响铃节奏）。
// 响铃的副作用（提示音、打断播报、代发查询）在 Application 的
// AlarmTick/FireAlarm 里；本类不做任何 IO。
//
// 线程约定：全部方法只在主线程调用——MCP 工具回调经 Application::Schedule
// 落到主任务，到点检查挂在主循环 CLOCK_TICK 分支，天然同线程，无锁。
//
// v1 边界（见 GLOSSARY「定时器」「闹钟」）：只做一次性（无每天/工作日重复
// 规则），不持久化（重启即丢，NVS 持久化留给 v2）。
class AlarmManager {
public:
    static AlarmManager& GetInstance() {
        static AlarmManager instance;
        return instance;
    }

    struct Alarm {
        int id = 0;
        time_t deadline = 0;   // 本地时间（系统时钟由 OTA server_time 校准）
        bool is_timer = false; // true=定时器（相对时长）；false=闹钟（绝对钟点）
        std::string message;   // 可选留言，到点经服务器 TTS 播报
    };

    // 以下三个供 MCP 工具调用：成功返回给 LLM 的 JSON 回执，失败返回错误文本
    std::expected<std::string, std::string> AddTimer(int minutes, const std::string& message);
    std::expected<std::string, std::string> AddAlarm(const std::string& hhmm,
                                                     const std::string& message);
    std::expected<std::string, std::string> Cancel(int id);
    std::string List();

    // 取出到点的最早一项（若无则 nullopt）；调用方负责触发响铃
    std::optional<Alarm> PopDue(time_t now);

    // 响铃态（见 GLOSSARY「响铃」）：循环短提示音，直到被停/超时/播报接管
    bool IsRinging() const { return ringing_.has_value(); }
    void StartRing(Alarm alarm, time_t now);
    void StopRing() { ringing_.reset(); }
    bool ShouldBeep(time_t now);          // 距上次提示音达到间隔时置位并记时间
    bool RingExpired(time_t now) const;   // 响铃超过时限，应自动停

private:
    AlarmManager() = default;
    std::string Add(Alarm alarm, time_t now);  // 落库并生成 JSON 回执
    static bool TimeSynced(time_t now);        // 与状态栏时钟同判据：服务器校准过

    std::vector<Alarm> alarms_;
    int next_id_ = 1;
    std::optional<Alarm> ringing_;
    time_t ring_start_ = 0;
    time_t last_beep_ = 0;
};

#endif  // _ALARM_MANAGER_H_
