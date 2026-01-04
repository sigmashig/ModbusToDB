#ifndef PERIODPARSER_H
#define PERIODPARSER_H

#include <chrono>
#include <string>

namespace ModbusLogger {

class PeriodParser {
public:
  // Parse human-readable period string to seconds
  // Valid formats: "5s", "30s", "1m", "5m", "1h", "1d"
  // Returns seconds, or throws ConfigParseException on error
  static std::chrono::seconds parsePeriod(const std::string &periodStr);

  // Validate period is within bounds (5s to 1 day)
  static bool validatePeriod(std::chrono::seconds period);
};

} // namespace ModbusLogger

#endif // PERIODPARSER_H
