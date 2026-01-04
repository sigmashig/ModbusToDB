#include "ConfigParser.h"
#include "DaemonManager.h"
#include "DataProcessor.h"
#include "DatabaseManager.h"
#include "ModbusClient.h"
#include "PeriodicScheduler.h"
#include "SchemaManager.h"
#include <atomic>
#include <chrono>
#include <cmath>
#include <csignal>
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
constexpr const char *DEFAULT_PID_FILE = "/var/run/modbuslogger.pid";
constexpr int DEFAULT_DEVICE_ID = 1;
constexpr int MAX_RETRIES = 3;
constexpr int RETRY_DELAY_MS = 400;
constexpr int READ_DELAY_MS = 400;
constexpr int POST_RETRY_DELAY_MS = 300;
constexpr double VALUE_EPSILON = 1e-9;
constexpr const char *TABLE_NAME = "modbus_data";
constexpr uint16_t REGISTER_540 = 540;
constexpr int DEVICE_ID_1 = 1;

std::atomic<bool> g_shutdownRequested(false);

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

void shutdownHandler() { g_shutdownRequested = true; }

ModbusLogger::DataProcessor::PreprocessFunction
createPreprocessFunction(int deviceId) {
  return [deviceId](
             uint16_t address, double value,
             const ModbusLogger::RegisterValueMap & /* allValues */) -> double {
    // Process register 540 for device ID 1
    if (deviceId == DEVICE_ID_1 && address == REGISTER_540) {
      // Convert to integer to extract first digit
      int64_t intValue = static_cast<int64_t>(std::round(value));

      // Handle zero or negative values
      if (intValue <= 0) {
        return value;
      }

      // Find the highest power of 10 less than the value
      int64_t powerOf10 = 1;
      int64_t temp = intValue;
      while (temp >= 10) {
        powerOf10 *= 10;
        temp /= 10;
      }

      // Extract first digit
      int firstDigit = static_cast<int>(intValue / powerOf10);

      // Remove first digit from value
      int64_t remainingValue = intValue % powerOf10;

      // Apply scaling: multiply by 10^(-firstDigit)
      double scale = std::pow(10.0, -firstDigit);
      return static_cast<double>(remainingValue) * scale;
    }

    // For other registers, return value unchanged
    return value;
  };
}

} // namespace

void printUsage(const char *programName) {
  std::cerr
      << "Usage: " << programName << " [OPTIONS]\n"
      << "Options:\n"
      << "  -c, --config <path>        Path to config file (default: "
         "./config.json)\n"
      << "  -d, --device-id <id>       Modbus device ID (default: 1)\n"
      << "  -p, --pid-file <path>      PID file path (default: "
         "/var/run/modbuslogger.pid)\n"
      << "  -s, --single-run           Run once and exit (for testing)\n"
      << "  -v, --verbose              Print register reading information\n"
      << "  -h, --help                 Show this help message\n";
}

bool readSingleRegister(ModbusLogger::ModbusClient &modbusClient,
                        const ModbusLogger::RegisterDefinition &reg,
                        const ModbusLogger::DeviceConfig *deviceConfig,
                        bool verbose, std::vector<uint16_t> &values) {
  values.clear();

  // Adjust address based on isZero
  uint16_t modbusAddress = reg.address;
  if (!deviceConfig->isZero) {
    if (modbusAddress == 0) {
      std::cerr << "Error: Register address cannot be 0 when isZero is false"
                << std::endl;
      return false;
    }
    modbusAddress = reg.address - 1;
  }

  // Determine quantity
  uint16_t quantity = (reg.type == ModbusLogger::RegisterType::Int32 ||
                       reg.type == ModbusLogger::RegisterType::Float32)
                          ? 2
                          : 1;

  if (verbose) {
    std::cerr << "Reading register: " << reg.name
              << " (address: " << reg.address;
    if (!deviceConfig->isZero) {
      std::cerr << ", modbus address: " << modbusAddress;
    }
    std::cerr << ")" << std::endl;
  }

  // Retry logic
  int retryCount = 0;
  bool success = false;

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
      success =
          modbusClient.readHoldingRegisters(modbusAddress, quantity, values);
      break;
    }

    if (success) {
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
        std::cerr << "  Retry " << retryCount << "/" << MAX_RETRIES
                  << " after error: " << modbusClient.getLastError()
                  << std::endl;
      }
      modbusClient.flushBuffer();
      std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_DELAY_MS));
    }
  }

  if (!success) {
    std::cerr << "Error: Failed to read register at address " << reg.address
              << " after " << MAX_RETRIES
              << " retries: " << modbusClient.getLastError() << std::endl;
    return false;
  }

  modbusClient.flushBuffer();
  std::this_thread::sleep_for(std::chrono::milliseconds(READ_DELAY_MS));

  if (verbose) {
    std::cerr << "  Raw value(s): ";
    for (size_t i = 0; i < values.size(); ++i) {
      if (i > 0)
        std::cerr << ", ";
      std::cerr << values[i];
    }
    std::cerr << std::endl;
  }

  return true;
}

bool ensureDatabaseConnection(
    ModbusLogger::DatabaseManager &dbManager, int deviceId,
    const std::vector<ModbusLogger::RegisterDefinition> &registers) {
  if (dbManager.isConnected()) {
    return true;
  }

  std::cerr << "Reconnecting to database..." << std::endl;
  if (!dbManager.connect()) {
    std::cerr << "Error: Failed to reconnect to database: "
              << dbManager.getLastError() << std::endl;
    return false;
  }

  // Ensure table exists
  ModbusLogger::SchemaManager schemaManager(dbManager);
  if (!schemaManager.ensureTableExists(deviceId, registers)) {
    std::cerr << "Error: Failed to ensure table exists" << std::endl;
    return false;
  }

  return true;
}

bool ensureModbusConnection(ModbusLogger::ModbusClient &modbusClient) {
  if (modbusClient.isConnected()) {
    return true;
  }

  std::cerr << "Reconnecting to Modbus device..." << std::endl;
  if (!modbusClient.connect()) {
    std::cerr << "Error: Failed to reconnect to Modbus device: "
              << modbusClient.getLastError() << std::endl;
    return false;
  }

  return true;
}

bool storeValueIfChanged(ModbusLogger::DatabaseManager &dbManager, int deviceId,
                         const std::string &registerName, double value,
                         std::map<std::string, double> &lastValues) {
  // Check if value changed
  bool shouldInsert = true;
  if (lastValues.find(registerName) != lastValues.end()) {
    double lastValue = lastValues[registerName];
    if (std::abs(value - lastValue) < VALUE_EPSILON) {
      shouldInsert = false;
    }
  }

  if (!shouldInsert) {
    return true; // No change, nothing to do
  }

  // Insert into database
  try {
    pqxx::work txn(dbManager.getConnection());
    std::ostringstream insertQuery;
    insertQuery << "INSERT INTO " << quoteIdentifier(TABLE_NAME)
                << " (device_id, timestamp, register_name, value) VALUES ("
                << deviceId << ", CURRENT_TIMESTAMP, "
                << txn.quote(registerName) << ", " << txn.quote(value) << ")";
    txn.exec(insertQuery.str());
    txn.commit();

    // Update in-memory value
    lastValues[registerName] = value;
    return true;
  } catch (const std::exception &e) {
    std::cerr << "Error: Failed to insert data for " << registerName << ": "
              << e.what() << std::endl;
    return false;
  }
}

int runSingleMode(const ModbusLogger::Config &config, int deviceId,
                  bool verbose, bool deviceIdExplicit) {
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

  // If device was explicitly specified via -d, ignore enabled flag
  // Otherwise, check if device is enabled
  if (!deviceIdExplicit && !deviceConfig->enabled) {
    std::cerr << "Error: Device ID " << deviceId << " is disabled" << std::endl;
    return 1;
  }

  // Get enabled registers
  std::vector<ModbusLogger::RegisterDefinition> registers;
  for (const auto &reg : deviceConfig->registers) {
    if (reg.enabled) {
      registers.push_back(reg);
    }
  }

  if (registers.empty()) {
    std::cerr << "Error: No enabled registers found for device " << deviceId
              << std::endl;
    return 1;
  }

  // Connect to Modbus
  ModbusLogger::ModbusClient modbusClient(deviceConfig->connection, deviceId);
  if (!modbusClient.connect()) {
    std::cerr << "Error: Failed to connect to Modbus device: "
              << modbusClient.getLastError() << std::endl;
    return 1;
  }

  // Read all registers
  std::vector<uint16_t> allRawValues;
  allRawValues.reserve(registers.size() * 2);

  for (const auto &reg : registers) {
    std::vector<uint16_t> values;
    if (!readSingleRegister(modbusClient, reg, deviceConfig, verbose, values)) {
      modbusClient.disconnect();
      return 1;
    }
    allRawValues.insert(allRawValues.end(), values.begin(), values.end());
  }

  modbusClient.disconnect();

  // Process data
  ModbusLogger::DataProcessor processor;
  processor.setPreprocessFunction(createPreprocessFunction(deviceId));
  std::vector<ModbusLogger::RegisterValue> processedValues =
      processor.processRegisters(registers, allRawValues);

  if (verbose) {
    std::cerr << "\nProcessed values:" << std::endl;
    for (const auto &value : processedValues) {
      std::cerr << "  " << value.name << " (address " << value.address
                << "): " << value.processedValue << std::endl;
    }
  }

  // Connect to database
  ModbusLogger::DatabaseManager dbManager(config.databaseConnectionString);
  if (!dbManager.connect()) {
    std::cerr << "Error: Failed to connect to database: "
              << dbManager.getLastError() << std::endl;
    return 1;
  }

  // Ensure table exists
  ModbusLogger::SchemaManager schemaManager(dbManager);
  if (!schemaManager.ensureTableExists(deviceId, registers)) {
    std::cerr << "Error: Failed to ensure table exists" << std::endl;
    dbManager.disconnect();
    return 1;
  }

  // Get last values
  std::map<std::string, double> lastValues;
  try {
    pqxx::work txn(dbManager.getConnection());
    std::ostringstream lastValueQuery;
    lastValueQuery << "SELECT DISTINCT ON (register_name) register_name, value "
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
    // Continue if query fails
  }

  // Store changed values
  for (size_t i = 0; i < processedValues.size(); ++i) {
    if (processedValues[i].address != registers[i].address) {
      std::cerr << "Error: Register order mismatch" << std::endl;
      dbManager.disconnect();
      return 1;
    }

    storeValueIfChanged(dbManager, deviceId, processedValues[i].name,
                        processedValues[i].processedValue, lastValues);
  }

  dbManager.disconnect();
  return 0;
}

int runContinuousMode(const ModbusLogger::Config &config, int deviceId,
                      bool verbose, const std::string &pidFilePath,
                      bool deviceIdExplicit) {
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

  // If device was explicitly specified via -d, ignore enabled flag
  // Otherwise, check if device is enabled
  if (!deviceIdExplicit && !deviceConfig->enabled) {
    std::cerr << "Error: Device ID " << deviceId << " is disabled" << std::endl;
    return 1;
  }

  // Get enabled registers
  std::vector<ModbusLogger::RegisterDefinition> registers;
  for (const auto &reg : deviceConfig->registers) {
    if (reg.enabled) {
      registers.push_back(reg);
    }
  }

  if (registers.empty()) {
    std::cerr << "Error: No enabled registers found for device " << deviceId
              << std::endl;
    return 1;
  }

  // Daemonize
  ModbusLogger::DaemonManager daemonManager(pidFilePath);
  if (!daemonManager.daemonize()) {
    std::cerr << "Error: Failed to daemonize" << std::endl;
    return 1;
  }

  // Setup signal handlers
  daemonManager.setupSignalHandlers(shutdownHandler);

  // Initialize scheduler
  ModbusLogger::PeriodicScheduler scheduler;
  for (const auto &reg : registers) {
    scheduler.addRegister(reg);
  }

  if (!scheduler.hasRegisters()) {
    std::cerr << "Error: No registers to schedule" << std::endl;
    return 1;
  }

  // Connect to Modbus
  ModbusLogger::ModbusClient modbusClient(deviceConfig->connection, deviceId);
  if (!modbusClient.connect()) {
    std::cerr << "Error: Failed to connect to Modbus device: "
              << modbusClient.getLastError() << std::endl;
    return 1;
  }

  // Connect to database
  ModbusLogger::DatabaseManager dbManager(config.databaseConnectionString);
  if (!dbManager.connect()) {
    std::cerr << "Error: Failed to connect to database: "
              << dbManager.getLastError() << std::endl;
    modbusClient.disconnect();
    return 1;
  }

  // Ensure table exists
  ModbusLogger::SchemaManager schemaManager(dbManager);
  if (!schemaManager.ensureTableExists(deviceId, registers)) {
    std::cerr << "Error: Failed to ensure table exists" << std::endl;
    dbManager.disconnect();
    modbusClient.disconnect();
    return 1;
  }

  // Initialize last values from database
  std::map<std::string, double> lastValues;
  try {
    pqxx::work txn(dbManager.getConnection());
    std::ostringstream lastValueQuery;
    lastValueQuery << "SELECT DISTINCT ON (register_name) register_name, value "
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
    // Continue if query fails - start with empty last values
  }

  // Main loop
  ModbusLogger::DataProcessor processor;
  processor.setPreprocessFunction(createPreprocessFunction(deviceId));

  while (!g_shutdownRequested) {
    // Get registers that need reading
    auto registersToRead = scheduler.getRegistersToRead();

    for (const auto *reg : registersToRead) {
      // Ensure connections
      if (!ensureModbusConnection(modbusClient)) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        continue;
      }

      if (!ensureDatabaseConnection(dbManager, deviceId, registers)) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        continue;
      }

      // Read register
      std::vector<uint16_t> values;
      if (!readSingleRegister(modbusClient, *reg, deviceConfig, verbose,
                              values)) {
        // Continue to next register on error
        continue;
      }

      // Process value
      std::vector<ModbusLogger::RegisterDefinition> singleReg;
      singleReg.push_back(*reg);
      auto processedValues = processor.processRegisters(singleReg, values);

      if (processedValues.empty()) {
        continue;
      }

      // Store if changed
      storeValueIfChanged(dbManager, deviceId, processedValues[0].name,
                          processedValues[0].processedValue, lastValues);

      // Mark as read
      scheduler.markRegisterRead(*reg);
    }

    // Sleep until next read is needed
    auto sleepTime = scheduler.getTimeUntilNextRead();
    if (sleepTime.count() > 0) {
      std::this_thread::sleep_for(sleepTime);
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  modbusClient.disconnect();
  dbManager.disconnect();
  return 0;
}

int main(int argc, char *argv[]) {
  std::string configPath = DEFAULT_CONFIG;
  int deviceId = DEFAULT_DEVICE_ID;
  std::string pidFilePath = DEFAULT_PID_FILE;
  bool singleRun = false;
  bool verbose = false;
  bool deviceIdExplicit = false;

  // Parse command line arguments
  static struct option longOptions[] = {
      {"config", required_argument, nullptr, 'c'},
      {"device-id", required_argument, nullptr, 'd'},
      {"pid-file", required_argument, nullptr, 'p'},
      {"single-run", no_argument, nullptr, 's'},
      {"verbose", no_argument, nullptr, 'v'},
      {"help", no_argument, nullptr, 'h'},
      {nullptr, 0, nullptr, 0}};

  int optionIndex = 0;
  int c;
  while ((c = getopt_long(argc, argv, "c:d:p:svh", longOptions,
                          &optionIndex)) != -1) {
    switch (c) {
    case 'c':
      configPath = optarg;
      break;
    case 'd':
      deviceId = std::stoi(optarg);
      deviceIdExplicit = true;
      break;
    case 'p':
      pidFilePath = optarg;
      break;
    case 's':
      singleRun = true;
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

  try {
    // Parse configuration
    ModbusLogger::Config config = ModbusLogger::ConfigParser::parse(configPath);

    if (singleRun) {
      return runSingleMode(config, deviceId, verbose, deviceIdExplicit);
    } else {
      return runContinuousMode(config, deviceId, verbose, pidFilePath,
                               deviceIdExplicit);
    }

  } catch (const ModbusLogger::ConfigParseException &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }
}
