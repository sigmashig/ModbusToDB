#include "PeriodicScheduler.h"
#include <algorithm>

namespace ModbusLogger {

PeriodicScheduler::PeriodicScheduler() 
    : startTime(std::chrono::steady_clock::now()) {
}

void PeriodicScheduler::addRegister(const RegisterDefinition& reg) {
    if (!reg.enabled) {
        return; // Skip disabled registers
    }
    
    RegisterSchedule schedule;
    schedule.reg = &reg;
    schedule.period = PeriodParser::parsePeriod(reg.period);
    schedule.nextReadTime = std::chrono::steady_clock::now();
    
    schedules[reg.address] = schedule;
}

std::vector<const RegisterDefinition*> PeriodicScheduler::getRegistersToRead() {
    std::vector<const RegisterDefinition*> result;
    auto now = std::chrono::steady_clock::now();
    
    for (auto& pair : schedules) {
        if (now >= pair.second.nextReadTime) {
            result.push_back(pair.second.reg);
        }
    }
    
    return result;
}

void PeriodicScheduler::markRegisterRead(const RegisterDefinition& reg) {
    auto it = schedules.find(reg.address);
    if (it != schedules.end()) {
        // Update next read time to current time + period
        it->second.nextReadTime = std::chrono::steady_clock::now() + it->second.period;
    }
}

std::chrono::milliseconds PeriodicScheduler::getTimeUntilNextRead() const {
    if (schedules.empty()) {
        return std::chrono::milliseconds(1000); // Default 1 second if no registers
    }
    
    auto now = std::chrono::steady_clock::now();
    auto minTime = now + std::chrono::hours(24); // Start with a large value
    
    for (const auto& pair : schedules) {
        if (pair.second.nextReadTime < minTime) {
            minTime = pair.second.nextReadTime;
        }
    }
    
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(minTime - now);
    
    // Return at least 100ms to avoid busy waiting
    if (duration.count() < 100) {
        return std::chrono::milliseconds(100);
    }
    
    return duration;
}

bool PeriodicScheduler::hasRegisters() const {
    return !schedules.empty();
}

} // namespace ModbusLogger

