#ifndef CONFIGPARSER_H
#define CONFIGPARSER_H

#include "Types.h"
#include <string>
#include <stdexcept>

namespace ModbusLogger {

class ConfigParser {
public:
    static Config parse(const std::string& configPath);
    
private:
    static RegisterType parseRegisterType(const std::string& typeStr);
    static char parseParity(const std::string& parityStr);
};

class ConfigParseException : public std::runtime_error {
public:
    explicit ConfigParseException(const std::string& message)
        : std::runtime_error("Config parse error: " + message) {}
};

} // namespace ModbusLogger

#endif // CONFIGPARSER_H

