#ifndef TYPES_H
#define TYPES_H

#include <string>
#include <vector>
#include <map>
#include <cstdint>

namespace ModbusLogger {

enum class RegisterType {
    Int16,
    Int32,
    Float32
};

struct RegisterDefinition {
    uint16_t address;
    std::string name;
    RegisterType type;
    double scale;
    bool preprocessing;
};

struct ConnectionParams {
    std::string port;
    int baudRate;
    char parity;
    int dataBits;
    int stopBits;
};

struct DeviceConfig {
    int id;
    ConnectionParams connection;
    std::vector<RegisterDefinition> registers;
};

struct Config {
    std::string databaseConnectionString;
    std::vector<DeviceConfig> devices;
};

struct RegisterValue {
    uint16_t address;
    std::string name;
    RegisterType type;
    double scale;
    bool preprocessing;
    int64_t rawValue;
    double processedValue;
};

using RegisterValueMap = std::map<uint16_t, RegisterValue>;

} // namespace ModbusLogger

#endif // TYPES_H

