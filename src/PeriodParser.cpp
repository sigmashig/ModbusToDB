#include "PeriodParser.h"
#include "ConfigParser.h"
#include <regex>
#include <stdexcept>

namespace ModbusLogger {

std::chrono::seconds PeriodParser::parsePeriod(const std::string& periodStr) {
    if (periodStr.empty()) {
        throw ConfigParseException("Period string cannot be empty");
    }
    
    // Regex to match number followed by unit (s, m, h, d)
    std::regex pattern(R"((\d+)([smhd]))");
    std::smatch match;
    
    if (!std::regex_match(periodStr, match, pattern)) {
        throw ConfigParseException("Invalid period format: " + periodStr + " (expected format: e.g., '5s', '1m', '1h', '1d')");
    }
    
    int value = std::stoi(match[1].str());
    char unit = match[2].str()[0];
    
    int seconds = 0;
    switch (unit) {
        case 's':
            seconds = value;
            break;
        case 'm':
            seconds = value * 60;
            break;
        case 'h':
            seconds = value * 3600;
            break;
        case 'd':
            seconds = value * 86400;
            break;
        default:
            throw ConfigParseException("Invalid period unit: " + std::string(1, unit) + " (must be s, m, h, or d)");
    }
    
    std::chrono::seconds period(seconds);
    if (!validatePeriod(period)) {
        throw ConfigParseException("Period " + periodStr + " is out of range (must be between 5s and 1d)");
    }
    
    return period;
}

bool PeriodParser::validatePeriod(std::chrono::seconds period) {
    constexpr int MIN_PERIOD_SECONDS = 5;
    constexpr int MAX_PERIOD_SECONDS = 86400; // 1 day
    
    int seconds = period.count();
    return seconds >= MIN_PERIOD_SECONDS && seconds <= MAX_PERIOD_SECONDS;
}

} // namespace ModbusLogger

