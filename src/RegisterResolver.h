#ifndef REGISTERRESOLVER_H
#define REGISTERRESOLVER_H

#include "Types.h"
#include <vector>
#include <string>
#include <map>

namespace ModbusLogger {

class RegisterResolver {
public:
    explicit RegisterResolver(const DeviceConfig& deviceConfig);

    std::vector<RegisterDefinition> resolveFromRanges(const std::vector<std::string>& ranges);
    std::vector<RegisterDefinition> resolveFromFile(const std::string& filePath);
    std::vector<RegisterDefinition> resolveFromAddresses(const std::vector<uint16_t>& addresses);

private:
    std::vector<uint16_t> parseRange(const std::string& range);
    std::vector<uint16_t> parseCommaSeparated(const std::string& addresses);
    RegisterDefinition* findRegisterDefinition(uint16_t address);

    const DeviceConfig& deviceConfig;
    std::map<uint16_t, RegisterDefinition> registerMap;
};

} // namespace ModbusLogger

#endif // REGISTERRESOLVER_H

