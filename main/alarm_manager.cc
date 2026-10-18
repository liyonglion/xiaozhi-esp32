#include "alarm_manager.h"

#include <algorithm>
#include <cstdio>
#include <memory>

namespace {

constexpr int kBeepIntervalSeconds = 2;   // 响铃期间每 2 秒重响一次短提示音
constexpr int kRingTimeoutSeconds = 180;  // 3 分钟无人停则自动停（Q3 定案）
constexpr int kMaxTimerMinutes = 1440;    // 定时器上限 24 小时

struct cJSONDeleter {
    void operator()(cJSON* p) const { cJSON_Delete(p); }
};
using JsonPtr = std::unique_ptr<cJSON, cJSONDeleter>;

std::string PrintJson(const cJSON* json) {
    char* printed = cJSON_PrintUnformatted(json);
    if (printed == nullptr) {
        return "{}";
    }
    std::string result(printed);
    cJSON_free(printed);
    return result;
}

// "MM-DD HH:MM"，跨天闹钟一眼可辨
std::string FormatTime(time_t t) {
    struct tm tm_buf;
    localtime_r(&t, &tm_buf);
    char buf[24];
    strftime(buf, sizeof(buf), "%m-%d %H:%M", &tm_buf);
    return buf;
}

cJSON* alarmToJson(const AlarmManager::Alarm& a, time_t now) {
    cJSON* item = cJSON_CreateObject();
    cJSON_AddNumberToObject(item, "id", a.id);
    cJSON_AddStringToObject(item, "type", a.is_timer ? "timer" : "alarm");
    cJSON_AddStringToObject(item, "ring_at", FormatTime(a.deadline).c_str());
    cJSON_AddNumberToObject(item, "in_minutes", static_cast<double>((a.deadline - now + 59) / 60));
    if (!a.message.empty()) {
        cJSON_AddStringToObject(item, "message", a.message.c_str());
    }
    return item;
}

}  // namespace

bool AlarmManager::TimeSynced(time_t now) {
    struct tm tm_buf;
    localtime_r(&now, &tm_buf);
    return tm_buf.tm_year + 1900 >= 2025;
}

std::expected<std::string, std::string> AlarmManager::AddTimer(int minutes,
                                                               const std::string& message) {
    time_t now = time(NULL);
    if (!TimeSynced(now)) {
        return std::unexpected("Device time is not synced yet, cannot set a timer");
    }
    if (minutes < 1 || minutes > kMaxTimerMinutes) {
        return std::unexpected("Timer duration must be between 1 and 1440 minutes");
    }
    return Add(Alarm{0, now + static_cast<time_t>(minutes) * 60, true, message}, now);
}

std::expected<std::string, std::string> AlarmManager::AddAlarm(const std::string& hhmm,
                                                               const std::string& message) {
    time_t now = time(NULL);
    if (!TimeSynced(now)) {
        return std::unexpected("Device time is not synced yet, cannot set an alarm");
    }
    int hour = 0, minute = 0, consumed = 0;
    if (sscanf(hhmm.c_str(), "%d:%d%n", &hour, &minute, &consumed) != 2 ||
        consumed != static_cast<int>(hhmm.size()) || hour < 0 || hour > 23 || minute < 0 ||
        minute > 59) {
        return std::unexpected("Alarm time must be exactly 24-hour HH:MM, e.g. \"08:30\"");
    }
    struct tm tm_buf;
    localtime_r(&now, &tm_buf);
    tm_buf.tm_hour = hour;
    tm_buf.tm_min = minute;
    tm_buf.tm_sec = 0;
    time_t deadline = mktime(&tm_buf);
    if (deadline <= now) {
        deadline += 86400;  // 「8点半」今天已过 → 明天同时刻（Q7 定案）
    }
    return Add(Alarm{0, deadline, false, message}, now);
}

std::string AlarmManager::Add(Alarm alarm, time_t now) {
    alarm.id = next_id_++;
    JsonPtr json(alarmToJson(alarm, now));
    alarms_.push_back(std::move(alarm));
    return PrintJson(json.get());
}

std::expected<std::string, std::string> AlarmManager::Cancel(int id) {
    for (auto it = alarms_.begin(); it != alarms_.end(); ++it) {
        if (it->id == id) {
            alarms_.erase(it);
            return "{\"id\":" + std::to_string(id) + ",\"cancelled\":true}";
        }
    }
    return std::unexpected("No alarm or timer with id " + std::to_string(id) +
                           "; use self.clock.list to check existing ids");
}

std::string AlarmManager::List() {
    time_t now = time(NULL);
    JsonPtr json(cJSON_CreateObject());
    cJSON_AddNumberToObject(json.get(), "count", static_cast<double>(alarms_.size()));
    cJSON* array = cJSON_AddArrayToObject(json.get(), "alarms");
    for (const auto& alarm : alarms_) {
        cJSON_AddItemToArray(array, alarmToJson(alarm, now));
    }
    return PrintJson(json.get());
}

std::optional<AlarmManager::Alarm> AlarmManager::PopDue(time_t now) {
    auto it = std::min_element(alarms_.begin(), alarms_.end(),
                               [](const Alarm& a, const Alarm& b) { return a.deadline < b.deadline; });
    if (it == alarms_.end() || it->deadline > now) {
        return std::nullopt;
    }
    Alarm due = std::move(*it);
    alarms_.erase(it);
    return due;
}

void AlarmManager::StartRing(Alarm alarm, time_t now) {
    ringing_ = std::move(alarm);
    ring_start_ = now;
    last_beep_ = now;  // 首声由 FireAlarm 立即播放，这里只记账
}

bool AlarmManager::ShouldBeep(time_t now) {
    if (!ringing_ || now - last_beep_ < kBeepIntervalSeconds) {
        return false;
    }
    last_beep_ = now;
    return true;
}

bool AlarmManager::RingExpired(time_t now) const {
    return ringing_.has_value() && now - ring_start_ >= kRingTimeoutSeconds;
}
