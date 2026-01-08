#include "PeriodicScheduler.h"
#include <algorithm>

namespace ModbusLogger {

PeriodicScheduler::PeriodicScheduler() 
    : startTime(std::chrono::steady_clock::now()) {
}

void PeriodicScheduler::addRange(const RangeDefinition& range) {
    RangeSchedule schedule;
    schedule.range = &range;
    schedule.period = PeriodParser::parsePeriod(range.period);
    schedule.nextReadTime = std::chrono::steady_clock::now();
    
    schedules.push_back(schedule);
}

std::vector<const RangeDefinition*> PeriodicScheduler::getRangesToRead() {
    std::vector<const RangeDefinition*> result;
    auto now = std::chrono::steady_clock::now();
    
    for (auto& schedule : schedules) {
        if (now >= schedule.nextReadTime) {
            result.push_back(schedule.range);
        }
    }
    
    return result;
}

void PeriodicScheduler::markRangeRead(const RangeDefinition& range) {
    for (auto& schedule : schedules) {
        if (schedule.range == &range) {
            // Update next read time to current time + period
            schedule.nextReadTime = std::chrono::steady_clock::now() + schedule.period;
            break;
        }
    }
}

std::chrono::milliseconds PeriodicScheduler::getTimeUntilNextRead() const {
    if (schedules.empty()) {
        return std::chrono::milliseconds(1000); // Default 1 second if no ranges
    }
    
    auto now = std::chrono::steady_clock::now();
    auto minTime = now + std::chrono::hours(24); // Start with a large value
    
    for (const auto& schedule : schedules) {
        if (schedule.nextReadTime < minTime) {
            minTime = schedule.nextReadTime;
        }
    }
    
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(minTime - now);
    
    // Return at least 100ms to avoid busy waiting
    if (duration.count() < 100) {
        return std::chrono::milliseconds(100);
    }
    
    return duration;
}

bool PeriodicScheduler::hasRanges() const {
    return !schedules.empty();
}

} // namespace ModbusLogger

