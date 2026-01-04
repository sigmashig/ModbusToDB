#ifndef MODBUSCLIENT_H
#define MODBUSCLIENT_H

#include "Types.h"
#include <vector>
#include <memory>
#include <modbus/modbus.h>

namespace ModbusLogger {

class ModbusClient {
public:
    ModbusClient(const ConnectionParams& connection, int deviceId);
    ~ModbusClient();

    ModbusClient(const ModbusClient&) = delete;
    ModbusClient& operator=(const ModbusClient&) = delete;

    bool connect();
    void disconnect();
    bool isConnected() const;

    bool readRegisters(const std::vector<uint16_t>& addresses, std::vector<uint16_t>& values);
    bool readHoldingRegisters(uint16_t startAddress, uint16_t quantity, std::vector<uint16_t>& values);
    
    std::string getLastError() const;

private:
    ConnectionParams connectionParams;
    int deviceId;
    modbus_t* ctx;
    bool connected;
    mutable std::string lastError;
};

} // namespace ModbusLogger

#endif // MODBUSCLIENT_H

