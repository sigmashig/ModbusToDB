#ifndef PERIODICSCHEDULER_H
#define PERIODICSCHEDULER_H

#include "Types.h"
#include "PeriodParser.h"
#include <chrono>
#include <vector>
#include <map>
#include <cstdint>

namespace ModbusLogger {

class PeriodicScheduler {
public:
    PeriodicScheduler();
    
    // Add range to scheduler with its period
    void addRange(const RangeDefinition& range);
    
    // Get ranges that need to be read now
    std::vector<const RangeDefinition*> getRangesToRead();
    
    // Mark range as read (update next read time)
    void markRangeRead(const RangeDefinition& range);
    
    // Get time until next range needs reading (for sleep optimization)
    std::chrono::milliseconds getTimeUntilNextRead() const;
    
    // Check if any ranges are scheduled
    bool hasRanges() const;

private:
    struct RangeSchedule {
        const RangeDefinition* range;
        std::chrono::steady_clock::time_point nextReadTime;
        std::chrono::seconds period;
    };
    
    std::vector<RangeSchedule> schedules;
    std::chrono::steady_clock::time_point startTime;
};

} // namespace ModbusLogger

#endif // PERIODICSCHEDULER_H

