#include "ConfigParser.h"
#include "DataProcessor.h"
#include "DatabaseManager.h"
#include "ModbusClient.h"
#include "RegisterResolver.h"
#include "SchemaManager.h"
#include <chrono>
#include <cmath>
#include <getopt.h>
#include <iostream>
#include <map>
#include <pqxx/pqxx>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {
constexpr const char *DEFAULT_CONFIG = "./config.json";
constexpr int DEFAULT_DEVICE_ID = 1;
constexpr int MAX_RETRIES = 3;
constexpr int RETRY_DELAY_MS = 400;
constexpr int READ_DELAY_MS = 400;
constexpr int POST_RETRY_DELAY_MS = 300;

std::string quoteIdentifier(const std::string &identifier) {
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
} // namespace

void printUsage(const char *programName) {
  std::cerr
      << "Usage: " << programName << " [OPTIONS]\n"
      << "Options:\n"
      << "  -c, --config <path>        Path to config file (default: "
         "./config.json)\n"
      << "  -d, --device-id <id>       Modbus device ID (default: 1)\n"
      << "  -r, --registers <range>    Register addresses (e.g., 100-200 "
         "or 100,101,102)\n"
      << "                             Can be specified multiple times\n"
      << "  -f, --register-file <path> File containing register "
         "addresses (alternative to -r)\n"
      << "  -v, --verbose              Print register reading information\n"
      << "  -h, --help                 Show this help message\n";
}

int main(int argc, char *argv[]) {
  std::string configPath = DEFAULT_CONFIG;
  int deviceId = DEFAULT_DEVICE_ID;
  std::vector<std::string> registerRanges;
  std::string registerFilePath;
  bool verbose = false;

  // Parse command line arguments
  static struct option longOptions[] = {
      {"config", required_argument, nullptr, 'c'},
      {"device-id", required_argument, nullptr, 'd'},
      {"registers", required_argument, nullptr, 'r'},
      {"register-file", required_argument, nullptr, 'f'},
      {"verbose", no_argument, nullptr, 'v'},
      {"help", no_argument, nullptr, 'h'},
      {nullptr, 0, nullptr, 0}};

  int optionIndex = 0;
  int c;
  while ((c = getopt_long(argc, argv, "c:d:r:f:vh", longOptions,
                          &optionIndex)) != -1) {
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
    case 'v':
      verbose = true;
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
    std::cerr << "Error: Must specify either --registers or --register-file"
              << std::endl;
    printUsage(argv[0]);
    return 1;
  }

  try {
    // Parse configuration
    ModbusLogger::Config config = ModbusLogger::ConfigParser::parse(configPath);

    // Find device configuration
    const ModbusLogger::DeviceConfig *deviceConfig = nullptr;
    for (const auto &device : config.devices) {
      if (device.id == deviceId) {
        deviceConfig = &device;
        break;
      }
    }

    if (deviceConfig == nullptr) {
      std::cerr << "Error: Device ID " << deviceId
                << " not found in configuration" << std::endl;
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
      std::cerr << "Error: Failed to connect to Modbus device: "
                << modbusClient.getLastError() << std::endl;
      return 1;
    }

    // Read registers from Modbus device (in order as specified in input
    // parameters) For int32 and float32, we need to read 2 consecutive
    // registers Use appropriate read method based on regType
    std::vector<uint16_t> allRawValues;
    allRawValues.reserve(registers.size() *
                         2); // Reserve space for worst case (int32/float32)

    for (size_t regIndex = 0; regIndex < registers.size(); ++regIndex) {
      const auto &reg = registers[regIndex];
      std::vector<uint16_t> values;
      bool success = false;

      // Adjust address based on isZero: if isZero is false (1-based), subtract
      // 1 since libmodbus uses 0-based addressing
      uint16_t modbusAddress = reg.address;
      if (!deviceConfig->isZero) {
        if (modbusAddress == 0) {
          std::cerr
              << "Error: Register address cannot be 0 when isZero is false"
              << std::endl;
          modbusClient.disconnect();
          return 1;
        }
        modbusAddress = reg.address - 1;
      }

      // Determine how many registers to read (1 for int16/coil/discrete, 2 for
      // int32/float32)
      uint16_t quantity = (reg.type == ModbusLogger::RegisterType::Int32 ||
                           reg.type == ModbusLogger::RegisterType::Float32)
                              ? 2
                              : 1;

      if (verbose) {
        std::cout << "Reading register: " << reg.name
                  << " (address: " << reg.address;
        if (!deviceConfig->isZero) {
          std::cout << ", modbus address: " << modbusAddress;
        }
        std::cout << ", type: ";
        switch (reg.regType) {
        case ModbusLogger::ModbusRegisterType::Coil:
          std::cout << "coil";
          break;
        case ModbusLogger::ModbusRegisterType::Discrete:
          std::cout << "discrete";
          break;
        case ModbusLogger::ModbusRegisterType::Input:
          std::cout << "input";
          break;
        case ModbusLogger::ModbusRegisterType::Holding:
          std::cout << "holding";
          break;
        }
        std::cout << ")" << std::endl;
      }

      // Retry logic for reading registers
      int retryCount = 0;
      while (retryCount <= MAX_RETRIES) {
        switch (reg.regType) {
        case ModbusLogger::ModbusRegisterType::Coil: {
          std::vector<uint16_t> addresses;
          addresses.push_back(modbusAddress);
          if (quantity == 2) {
            addresses.push_back(modbusAddress + 1);
          }
          success = modbusClient.readCoils(addresses, values);
          break;
        }
        case ModbusLogger::ModbusRegisterType::Discrete: {
          std::vector<uint16_t> addresses;
          addresses.push_back(modbusAddress);
          if (quantity == 2) {
            addresses.push_back(modbusAddress + 1);
          }
          success = modbusClient.readDiscreteInputs(addresses, values);
          break;
        }
        case ModbusLogger::ModbusRegisterType::Input:
          success =
              modbusClient.readInputRegisters(modbusAddress, quantity, values);
          break;
        case ModbusLogger::ModbusRegisterType::Holding:
          success = modbusClient.readHoldingRegisters(modbusAddress, quantity,
                                                      values);
          break;
        }

        if (success) {
          // If we had retries, add extra delay after successful read
          if (retryCount > 0) {
            modbusClient.flushBuffer();
            std::this_thread::sleep_for(
                std::chrono::milliseconds(POST_RETRY_DELAY_MS));
          }
          break;
        }

        retryCount++;
        if (retryCount <= MAX_RETRIES) {
          if (verbose) {
            std::cout << "  Retry " << retryCount << "/" << MAX_RETRIES
                      << " after error: " << modbusClient.getLastError()
                      << std::endl;
          }
          // Flush buffer after error to clear any corrupted data
          modbusClient.flushBuffer();
          std::this_thread::sleep_for(
              std::chrono::milliseconds(RETRY_DELAY_MS));
        }
      }

      if (!success) {
        std::cerr << "Error: Failed to read register at address " << reg.address
                  << " after " << MAX_RETRIES
                  << " retries: " << modbusClient.getLastError() << std::endl;
        modbusClient.disconnect();
        return 1;
      }

      // Flush buffer and delay after successful read to ensure device is ready
      // for next request. Device needs 100-200ms to respond, so we wait longer
      // to ensure it's fully ready for the next read.
      if (regIndex < registers.size() - 1) {
        modbusClient.flushBuffer();
        std::this_thread::sleep_for(std::chrono::milliseconds(READ_DELAY_MS));
      }

      if (verbose) {
        std::cout << "  Raw value(s): ";
        for (size_t i = 0; i < values.size(); ++i) {
          if (i > 0)
            std::cout << ", ";
          std::cout << values[i];
        }
        std::cout << std::endl;
      }

      allRawValues.insert(allRawValues.end(), values.begin(), values.end());
    }

    modbusClient.disconnect();

    // Process data - register values will be stored in memory accessible from
    // all components
    ModbusLogger::DataProcessor processor;
    std::vector<ModbusLogger::RegisterValue> processedValues =
        processor.processRegisters(registers, allRawValues);

    if (verbose) {
      std::cout << "\nProcessed values:" << std::endl;
      for (const auto &value : processedValues) {
        std::cout << "  " << value.name << " (address " << value.address
                  << "): " << value.processedValue << std::endl;
      }
      std::cout << std::endl;
    }

    // Connect to database
    ModbusLogger::DatabaseManager dbManager(config.databaseConnectionString);
    if (!dbManager.connect()) {
      std::cerr << "Error: Failed to connect to database: "
                << dbManager.getLastError() << std::endl;
      return 1;
    }

    // Ensure table exists with correct schema
    ModbusLogger::SchemaManager schemaManager(dbManager);
    if (!schemaManager.ensureTableExists(deviceId, registers)) {
      std::cerr << "Error: Failed to ensure table exists" << std::endl;
      dbManager.disconnect();
      return 1;
    }

    // Insert data into database using normalized structure
    // Verify that processed values match registers (should be in same order)
    if (processedValues.size() != registers.size()) {
      std::cerr
          << "Error: Mismatch between processed values and register definitions"
          << std::endl;
      dbManager.disconnect();
      return 1;
    }

    constexpr const char *TABLE_NAME = "modbus_data";
    pqxx::work txn(dbManager.getConnection());

    // Get last values for each register to detect changes
    std::map<std::string, double> lastValues;
    try {
      std::ostringstream lastValueQuery;
      lastValueQuery
          << "SELECT DISTINCT ON (register_name) register_name, value "
          << "FROM " << quoteIdentifier(TABLE_NAME) << " "
          << "WHERE device_id = " << deviceId << " "
          << "ORDER BY register_name, timestamp DESC";
      pqxx::result lastResult = txn.exec(lastValueQuery.str());
      for (const auto &row : lastResult) {
        if (!row[1].is_null()) {
          lastValues[row[0].as<std::string>()] = row[1].as<double>();
        }
      }
    } catch (const std::exception &e) {
      // If table is empty or query fails, continue - we'll insert all values
      if (verbose) {
        std::cout << "Note: Could not fetch last values: " << e.what()
                  << std::endl;
      }
    }

    // Build batch insert for changed registers only
    std::ostringstream insertQuery;
    insertQuery << "INSERT INTO " << quoteIdentifier(TABLE_NAME)
                << " (device_id, timestamp, register_name, value) VALUES ";

    bool firstValue = true;
    int insertedCount = 0;

    for (size_t i = 0; i < processedValues.size(); ++i) {
      // Verify address matches (safety check)
      if (processedValues[i].address != registers[i].address) {
        std::cerr << "Error: Register order mismatch at index " << i
                  << std::endl;
        dbManager.disconnect();
        return 1;
      }

      const std::string &registerName = processedValues[i].name;
      double currentValue = processedValues[i].processedValue;

      // Check if value changed (or doesn't exist in last values)
      bool shouldInsert = true;
      if (lastValues.find(registerName) != lastValues.end()) {
        double lastValue = lastValues[registerName];
        // Use small epsilon for floating point comparison
        constexpr double EPSILON = 1e-9;
        if (std::abs(currentValue - lastValue) < EPSILON) {
          shouldInsert = false;
        }
      }

      if (shouldInsert) {
        if (!firstValue) {
          insertQuery << ", ";
        }
        insertQuery << "(" << deviceId << ", CURRENT_TIMESTAMP, "
                    << txn.quote(registerName) << ", "
                    << txn.quote(currentValue) << ")";
        firstValue = false;
        insertedCount++;
      }
    }

    // Only execute insert if there are values to insert
    if (insertedCount > 0) {
      try {
        txn.exec(insertQuery.str());
        txn.commit();
        if (verbose) {
          std::cout << "Inserted " << insertedCount
                    << " changed register value(s)" << std::endl;
        }
      } catch (const std::exception &e) {
        std::cerr << "Error: Failed to insert data: " << e.what() << std::endl;
        dbManager.disconnect();
        return 1;
      }
    } else {
      txn.commit();
      if (verbose) {
        std::cout << "No register values changed, skipping insert" << std::endl;
      }
    }

    dbManager.disconnect();

    return 0;

  } catch (const ModbusLogger::ConfigParseException &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }
}
