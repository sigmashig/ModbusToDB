#include "ModbusClient.h"
#include <modbus/modbus-rtu.h>
#include <iostream>
#include <cstring>
#include <cerrno>

namespace ModbusLogger {

ModbusClient::ModbusClient(const ConnectionParams& connection, int deviceId)
    : connectionParams(connection)
    , deviceId(deviceId)
    , ctx(nullptr)
    , connected(false)
{
}

ModbusClient::~ModbusClient() {
    disconnect();
}

bool ModbusClient::connect() {
    if (connected) {
        return true;
    }

    ctx = modbus_new_rtu(
        connectionParams.port.c_str(),
        connectionParams.baudRate,
        connectionParams.parity,
        connectionParams.dataBits,
        connectionParams.stopBits
    );

    if (ctx == nullptr) {
        lastError = "Failed to create Modbus RTU context";
        std::cerr << "Error: " << lastError << std::endl;
        return false;
    }

    if (modbus_set_slave(ctx, deviceId) != 0) {
        lastError = "Failed to set Modbus slave ID: " + std::string(modbus_strerror(errno));
        std::cerr << "Error: " << lastError << std::endl;
        modbus_free(ctx);
        ctx = nullptr;
        return false;
    }

    // Set timeouts for RTU communication
    // Response timeout: 2 seconds (2000ms) - time to wait for response
    // Byte timeout: 0.1 second (100ms) - time to wait between bytes
    modbus_set_response_timeout(ctx, 2, 0);
    modbus_set_byte_timeout(ctx, 0, 100000); // 100ms in microseconds

    if (modbus_connect(ctx) != 0) {
        lastError = "Failed to connect to Modbus device: " + std::string(modbus_strerror(errno));
        std::cerr << "Error: " << lastError << std::endl;
        modbus_free(ctx);
        ctx = nullptr;
        return false;
    }

    connected = true;
    return true;
}

void ModbusClient::disconnect() {
    if (ctx != nullptr) {
        if (connected) {
            modbus_close(ctx);
        }
        modbus_free(ctx);
        ctx = nullptr;
        connected = false;
    }
}

bool ModbusClient::isConnected() const {
    return connected;
}

bool ModbusClient::readRegisters(const std::vector<uint16_t>& addresses, std::vector<uint16_t>& values) {
    if (!connected || ctx == nullptr) {
        lastError = "Not connected to Modbus device";
        std::cerr << "Error: " << lastError << std::endl;
        return false;
    }

    values.clear();
    values.reserve(addresses.size());

    for (uint16_t address : addresses) {
        uint16_t value;
        int result = modbus_read_registers(ctx, address, 1, &value);
        if (result == -1) {
            lastError = "Failed to read register at address " + std::to_string(address) + ": " + std::string(modbus_strerror(errno));
            std::cerr << "Error: " << lastError << std::endl;
            return false;
        }
        values.push_back(value);
    }

    return true;
}

bool ModbusClient::readHoldingRegisters(uint16_t startAddress, uint16_t quantity, std::vector<uint16_t>& values) {
    if (!connected || ctx == nullptr) {
        lastError = "Not connected to Modbus device";
        std::cerr << "Error: " << lastError << std::endl;
        return false;
    }

    values.resize(quantity);
    int result = modbus_read_registers(ctx, startAddress, quantity, values.data());
    if (result == -1) {
        lastError = "Failed to read holding registers starting at " + std::to_string(startAddress) + ": " + std::string(modbus_strerror(errno));
        std::cerr << "Error: " << lastError << std::endl;
        return false;
    }

    return true;
}

bool ModbusClient::readInputRegisters(uint16_t startAddress, uint16_t quantity, std::vector<uint16_t>& values) {
    if (!connected || ctx == nullptr) {
        lastError = "Not connected to Modbus device";
        std::cerr << "Error: " << lastError << std::endl;
        return false;
    }

    values.resize(quantity);
    int result = modbus_read_input_registers(ctx, startAddress, quantity, values.data());
    if (result == -1) {
        lastError = "Failed to read input registers starting at " + std::to_string(startAddress) + ": " + std::string(modbus_strerror(errno));
        std::cerr << "Error: " << lastError << std::endl;
        return false;
    }

    return true;
}

bool ModbusClient::readCoils(const std::vector<uint16_t>& addresses, std::vector<uint16_t>& values) {
    if (!connected || ctx == nullptr) {
        lastError = "Not connected to Modbus device";
        std::cerr << "Error: " << lastError << std::endl;
        return false;
    }

    values.clear();
    values.reserve(addresses.size());

    for (uint16_t address : addresses) {
        uint8_t bitValue;
        int result = modbus_read_bits(ctx, address, 1, &bitValue);
        if (result == -1) {
            lastError = "Failed to read coil at address " + std::to_string(address) + ": " + std::string(modbus_strerror(errno));
            std::cerr << "Error: " << lastError << std::endl;
            return false;
        }
        values.push_back(static_cast<uint16_t>(bitValue));
    }

    return true;
}

bool ModbusClient::readDiscreteInputs(const std::vector<uint16_t>& addresses, std::vector<uint16_t>& values) {
    if (!connected || ctx == nullptr) {
        lastError = "Not connected to Modbus device";
        std::cerr << "Error: " << lastError << std::endl;
        return false;
    }

    values.clear();
    values.reserve(addresses.size());

    for (uint16_t address : addresses) {
        uint8_t bitValue;
        int result = modbus_read_input_bits(ctx, address, 1, &bitValue);
        if (result == -1) {
            lastError = "Failed to read discrete input at address " + std::to_string(address) + ": " + std::string(modbus_strerror(errno));
            std::cerr << "Error: " << lastError << std::endl;
            return false;
        }
        values.push_back(static_cast<uint16_t>(bitValue));
    }

    return true;
}

void ModbusClient::flushBuffer() {
    if (ctx != nullptr && connected) {
        // Flush any remaining data in the serial buffer
        modbus_flush(ctx);
    }
}

std::string ModbusClient::getLastError() const {
    return lastError;
}

} // namespace ModbusLogger

