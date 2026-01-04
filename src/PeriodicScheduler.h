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
    
    // Add register to scheduler with its period
    void addRegister(const RegisterDefinition& reg);
    
    // Get registers that need to be read now
    std::vector<const RegisterDefinition*> getRegistersToRead();
    
    // Mark register as read (update next read time)
    void markRegisterRead(const RegisterDefinition& reg);
    
    // Get time until next register needs reading (for sleep optimization)
    std::chrono::milliseconds getTimeUntilNextRead() const;
    
    // Check if any registers are scheduled
    bool hasRegisters() const;

private:
    struct RegisterSchedule {
        const RegisterDefinition* reg;
        std::chrono::steady_clock::time_point nextReadTime;
        std::chrono::seconds period;
    };
    
    std::map<uint16_t, RegisterSchedule> schedules;
    std::chrono::steady_clock::time_point startTime;
};

} // namespace ModbusLogger

#endif // PERIODICSCHEDULER_H

