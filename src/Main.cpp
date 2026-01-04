#include "ConfigParser.h"
#include "ModbusClient.h"
#include "DatabaseManager.h"
#include "SchemaManager.h"
#include "RegisterResolver.h"
#include "DataProcessor.h"
#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <getopt.h>
#include <pqxx/pqxx>

namespace {
    constexpr const char* DEFAULT_CONFIG = "./config.json";
    constexpr int DEFAULT_DEVICE_ID = 1;
    
    std::string quoteIdentifier(const std::string& identifier) {
        std::string quoted = "\"";
        for (char c : identifier) {
            if (c == '"') {
                quoted += "\"\"";
            } else {
                quoted += c;
            }
        }
        quoted += "\"";
        return quoted;
    }
}

void printUsage(const char* programName) {
    std::cerr << "Usage: " << programName << " [OPTIONS]\n"
              << "Options:\n"
              << "  -c, --config <path>        Path to config file (default: ./config.json)\n"
              << "  -d, --device-id <id>       Modbus device ID (default: 1)\n"
              << "  -r, --registers <range>    Register addresses (e.g., 100-200 or 100,101,102)\n"
              << "                             Can be specified multiple times\n"
              << "  -f, --register-file <path> File containing register addresses (alternative to -r)\n"
              << "  -h, --help                 Show this help message\n";
}

int main(int argc, char* argv[]) {
    std::string configPath = DEFAULT_CONFIG;
    int deviceId = DEFAULT_DEVICE_ID;
    std::vector<std::string> registerRanges;
    std::string registerFilePath;

    // Parse command line arguments
    static struct option longOptions[] = {
        {"config", required_argument, nullptr, 'c'},
        {"device-id", required_argument, nullptr, 'd'},
        {"registers", required_argument, nullptr, 'r'},
        {"register-file", required_argument, nullptr, 'f'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0}
    };

    int optionIndex = 0;
    int c;
    while ((c = getopt_long(argc, argv, "c:d:r:f:h", longOptions, &optionIndex)) != -1) {
        switch (c) {
            case 'c':
                configPath = optarg;
                break;
            case 'd':
                deviceId = std::stoi(optarg);
                break;
            case 'r':
                registerRanges.push_back(optarg);
                break;
            case 'f':
                registerFilePath = optarg;
                break;
            case 'h':
                printUsage(argv[0]);
                return 0;
            default:
                printUsage(argv[0]);
                return 1;
        }
    }

    if (registerRanges.empty() && registerFilePath.empty()) {
        std::cerr << "Error: Must specify either --registers or --register-file" << std::endl;
        printUsage(argv[0]);
        return 1;
    }

    try {
        // Parse configuration
        ModbusLogger::Config config = ModbusLogger::ConfigParser::parse(configPath);

        // Find device configuration
        const ModbusLogger::DeviceConfig* deviceConfig = nullptr;
        for (const auto& device : config.devices) {
            if (device.id == deviceId) {
                deviceConfig = &device;
                break;
            }
        }

        if (deviceConfig == nullptr) {
            std::cerr << "Error: Device ID " << deviceId << " not found in configuration" << std::endl;
            return 1;
        }

        // Resolve register definitions
        ModbusLogger::RegisterResolver resolver(*deviceConfig);
        std::vector<ModbusLogger::RegisterDefinition> registers;

        if (!registerFilePath.empty()) {
            registers = resolver.resolveFromFile(registerFilePath);
        } else {
            registers = resolver.resolveFromRanges(registerRanges);
        }

        if (registers.empty()) {
            std::cerr << "Error: No valid registers found" << std::endl;
            return 1;
        }

        // Connect to Modbus device
        ModbusLogger::ModbusClient modbusClient(deviceConfig->connection, deviceId);
        if (!modbusClient.connect()) {
            std::cerr << "Error: Failed to connect to Modbus device: " << modbusClient.getLastError() << std::endl;
            return 1;
        }

        // Read registers from Modbus device (in order as specified in input parameters)
        // For int32 and float32, we need to read 2 consecutive registers
        // Build list of addresses to read, maintaining order
        std::vector<uint16_t> addressesToRead;
        
        for (const auto& reg : registers) {
            addressesToRead.push_back(reg.address);
            // For int32 and float32, also read the next register
            if (reg.type == ModbusLogger::RegisterType::Int32 || 
                reg.type == ModbusLogger::RegisterType::Float32) {
                addressesToRead.push_back(reg.address + 1);
            }
        }

        std::vector<uint16_t> allRawValues;
        if (!modbusClient.readRegisters(addressesToRead, allRawValues)) {
            std::cerr << "Error: Failed to read registers: " << modbusClient.getLastError() << std::endl;
            modbusClient.disconnect();
            return 1;
        }

        modbusClient.disconnect();

        // Process data - register values will be stored in memory accessible from all components
        ModbusLogger::DataProcessor processor;
        std::vector<ModbusLogger::RegisterValue> processedValues = processor.processRegisters(registers, allRawValues);

        // Connect to database
        ModbusLogger::DatabaseManager dbManager(config.databaseConnectionString);
        if (!dbManager.connect()) {
            std::cerr << "Error: Failed to connect to database: " << dbManager.getLastError() << std::endl;
            return 1;
        }

        // Ensure table exists with correct schema
        ModbusLogger::SchemaManager schemaManager(dbManager);
        if (!schemaManager.ensureTableExists(deviceId, registers)) {
            std::cerr << "Error: Failed to ensure table exists" << std::endl;
            dbManager.disconnect();
            return 1;
        }

        // Insert data into database
        // Verify that processed values match registers (should be in same order)
        if (processedValues.size() != registers.size()) {
            std::cerr << "Error: Mismatch between processed values and register definitions" << std::endl;
            dbManager.disconnect();
            return 1;
        }

        std::string tableName = "modbus_" + std::to_string(deviceId);
        pqxx::work txn(dbManager.getConnection());
        
        std::ostringstream insertQuery;
        insertQuery << "INSERT INTO " << quoteIdentifier(tableName) << " (";

        bool first = true;
        for (const auto& reg : registers) {
            if (!first) {
                insertQuery << ", ";
            }
            insertQuery << quoteIdentifier(reg.name);
            first = false;
        }

        insertQuery << ") VALUES (";

        first = true;
        for (size_t i = 0; i < processedValues.size(); ++i) {
            // Verify address matches (safety check)
            if (processedValues[i].address != registers[i].address) {
                std::cerr << "Error: Register order mismatch at index " << i << std::endl;
                dbManager.disconnect();
                return 1;
            }
            
            if (!first) {
                insertQuery << ", ";
            }
            insertQuery << txn.quote(processedValues[i].processedValue);
            first = false;
        }

        insertQuery << ")";

        try {
            txn.exec(insertQuery.str());
            txn.commit();
        } catch (const std::exception& e) {
            std::cerr << "Error: Failed to insert data: " << e.what() << std::endl;
            dbManager.disconnect();
            return 1;
        }

        dbManager.disconnect();

        return 0;

    } catch (const ModbusLogger::ConfigParseException& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}

