#ifndef TYPES_H
#define TYPES_H

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace ModbusLogger {

enum class RegisterType { Int16, Int32, Float32, Uint16, Uint32, Uint64 };

enum class ModbusRegisterType { Coil, Discrete, Input, Holding };

struct RegisterDefinition {
  uint16_t address;
  std::string name;
  RegisterType type;
  ModbusRegisterType regType;
  double scale;
  bool preprocessing;
  std::string period; // Human-readable period: "5s", "1m", "1h", "1d"
  bool enabled;       // Include register in reading cycle
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
  bool isZero;
  bool enabled; // Include device in reading cycle
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
  ModbusRegisterType regType;
  double scale;
  bool preprocessing;
  int64_t rawValue;
  double processedValue;
};

using RegisterValueMap = std::map<uint16_t, RegisterValue>;

} // namespace ModbusLogger

#endif // TYPES_H
