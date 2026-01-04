#include "ConfigParser.h"
#include "PeriodParser.h"
#include <nlohmann/json.hpp>
#include <fstream>
#include <sstream>
#include <iostream>

using json = nlohmann::json;

namespace ModbusLogger {

Config ConfigParser::parse(const std::string& configPath) {
    std::ifstream file(configPath);
    if (!file.is_open()) {
        throw ConfigParseException("Cannot open config file: " + configPath);
    }

    json configJson;
    try {
        file >> configJson;
    } catch (const json::parse_error& e) {
        throw ConfigParseException("JSON parse error: " + std::string(e.what()));
    }

    Config config;

    // Parse database connection
    if (!configJson.contains("database") || !configJson["database"].contains("connection_string")) {
        throw ConfigParseException("Missing 'database.connection_string' in config");
    }
    config.databaseConnectionString = configJson["database"]["connection_string"];

    // Parse devices
    if (!configJson.contains("devices") || !configJson["devices"].is_array()) {
        throw ConfigParseException("Missing or invalid 'devices' array in config");
    }

    for (const auto& deviceJson : configJson["devices"]) {
        DeviceConfig device;

        if (!deviceJson.contains("id") || !deviceJson["id"].is_number()) {
            throw ConfigParseException("Device missing or invalid 'id'");
        }
        device.id = deviceJson["id"];

        // Parse connection parameters
        if (!deviceJson.contains("connection")) {
            throw ConfigParseException("Device " + std::to_string(device.id) + " missing 'connection'");
        }

        const auto& connJson = deviceJson["connection"];
        if (!connJson.contains("port") || !connJson["port"].is_string()) {
            throw ConfigParseException("Device " + std::to_string(device.id) + " missing or invalid 'connection.port'");
        }
        device.connection.port = connJson["port"];

        if (!connJson.contains("baud_rate") || !connJson["baud_rate"].is_number()) {
            throw ConfigParseException("Device " + std::to_string(device.id) + " missing or invalid 'connection.baud_rate'");
        }
        device.connection.baudRate = connJson["baud_rate"];

        if (!connJson.contains("parity") || !connJson["parity"].is_string()) {
            throw ConfigParseException("Device " + std::to_string(device.id) + " missing or invalid 'connection.parity'");
        }
        device.connection.parity = parseParity(connJson["parity"]);

        if (!connJson.contains("data_bits") || !connJson["data_bits"].is_number()) {
            throw ConfigParseException("Device " + std::to_string(device.id) + " missing or invalid 'connection.data_bits'");
        }
        device.connection.dataBits = connJson["data_bits"];

        if (!connJson.contains("stop_bits") || !connJson["stop_bits"].is_number()) {
            throw ConfigParseException("Device " + std::to_string(device.id) + " missing or invalid 'connection.stop_bits'");
        }
        device.connection.stopBits = connJson["stop_bits"];

        // Parse isZero (defaults to true for 0-based addressing)
        if (deviceJson.contains("isZero") && deviceJson["isZero"].is_boolean()) {
            device.isZero = deviceJson["isZero"];
        } else {
            device.isZero = true;
        }

        // Parse enabled (defaults to true if not specified)
        if (deviceJson.contains("enabled") && deviceJson["enabled"].is_boolean()) {
            device.enabled = deviceJson["enabled"];
        } else {
            device.enabled = true;
        }

        // Parse registers
        if (!deviceJson.contains("registers") || !deviceJson["registers"].is_array()) {
            throw ConfigParseException("Device " + std::to_string(device.id) + " missing or invalid 'registers' array");
        }

        for (const auto& regJson : deviceJson["registers"]) {
            RegisterDefinition reg;

            if (!regJson.contains("address") || !regJson["address"].is_number()) {
                throw ConfigParseException("Register missing or invalid 'address' in device " + std::to_string(device.id));
            }
            reg.address = regJson["address"];

            if (!regJson.contains("name") || !regJson["name"].is_string()) {
                throw ConfigParseException("Register at address " + std::to_string(reg.address) + " missing or invalid 'name'");
            }
            reg.name = regJson["name"];

            if (!regJson.contains("type") || !regJson["type"].is_string()) {
                throw ConfigParseException("Register at address " + std::to_string(reg.address) + " missing or invalid 'type'");
            }
            reg.type = parseRegisterType(regJson["type"]);

            if (regJson.contains("regType") && regJson["regType"].is_string()) {
                reg.regType = parseModbusRegisterType(regJson["regType"]);
            } else {
                reg.regType = ModbusRegisterType::Holding;
            }

            if (regJson.contains("scale") && regJson["scale"].is_number()) {
                reg.scale = regJson["scale"];
            } else {
                reg.scale = 1.0;
            }

            if (regJson.contains("preprocessing") && regJson["preprocessing"].is_boolean()) {
                reg.preprocessing = regJson["preprocessing"];
            } else {
                reg.preprocessing = false;
            }

            // Parse period (required field)
            if (!regJson.contains("period") || !regJson["period"].is_string()) {
                throw ConfigParseException("Register at address " + std::to_string(reg.address) + " missing or invalid 'period' (expected format: e.g., '5s', '1m', '1h', '1d')");
            }
            std::string periodStr = regJson["period"];
            // Validate period format and range
            try {
                PeriodParser::parsePeriod(periodStr);
            } catch (const ConfigParseException& e) {
                throw ConfigParseException("Register at address " + std::to_string(reg.address) + " has invalid period: " + e.what());
            }
            reg.period = periodStr;

            // Parse enabled (defaults to true if not specified)
            if (regJson.contains("enabled") && regJson["enabled"].is_boolean()) {
                reg.enabled = regJson["enabled"];
            } else {
                reg.enabled = true;
            }

            device.registers.push_back(reg);
        }

        config.devices.push_back(device);
    }

    return config;
}

RegisterType ConfigParser::parseRegisterType(const std::string& typeStr) {
    if (typeStr == "int16") {
        return RegisterType::Int16;
    } else if (typeStr == "int32") {
        return RegisterType::Int32;
    } else if (typeStr == "float32") {
        return RegisterType::Float32;
    } else {
        throw ConfigParseException("Invalid register type: " + typeStr);
    }
}

ModbusRegisterType ConfigParser::parseModbusRegisterType(const std::string& regTypeStr) {
    if (regTypeStr == "coil") {
        return ModbusRegisterType::Coil;
    } else if (regTypeStr == "discrete") {
        return ModbusRegisterType::Discrete;
    } else if (regTypeStr == "input") {
        return ModbusRegisterType::Input;
    } else if (regTypeStr == "holding") {
        return ModbusRegisterType::Holding;
    } else {
        throw ConfigParseException("Invalid Modbus register type: " + regTypeStr + " (must be coil, discrete, input, or holding)");
    }
}

char ConfigParser::parseParity(const std::string& parityStr) {
    if (parityStr == "N" || parityStr == "n") {
        return 'N';
    } else if (parityStr == "E" || parityStr == "e") {
        return 'E';
    } else if (parityStr == "O" || parityStr == "o") {
        return 'O';
    } else {
        throw ConfigParseException("Invalid parity: " + parityStr + " (must be N, E, or O)");
    }
}

} // namespace ModbusLogger

