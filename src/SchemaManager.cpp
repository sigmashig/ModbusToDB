#include "SchemaManager.h"
#include <algorithm>
#include <cctype>
#include <iostream>
#include <pqxx/pqxx>
#include <sstream>

namespace ModbusLogger {

namespace {
constexpr const char *TIMESTAMP_COLUMN_NAME = "timestamp";

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

SchemaManager::SchemaManager(DatabaseManager &dbManager)
    : dbManager(dbManager) {}

bool SchemaManager::ensureTableExists(
    int deviceId, const std::vector<RegisterDefinition> &registers) {
  std::string tableName = getTableName(deviceId);

  if (dbManager.tableExists(tableName)) {
    // Table exists, check if we need to add columns
    return addMissingColumns(deviceId, registers);
  }

  // Create new table
  pqxx::work txn(dbManager.getConnection());
  std::ostringstream query;
  query << "CREATE TABLE " << quoteIdentifier(tableName) << " (";
  query << "id SERIAL PRIMARY KEY";
  query << ", " << quoteIdentifier(TIMESTAMP_COLUMN_NAME)
        << " TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP";

  for (const auto &reg : registers) {
    query << ", " << quoteIdentifier(reg.name) << " " << getColumnType(reg);
  }

  query << ")";
  std::string queryStr = query.str();
  txn.exec(queryStr);
  txn.commit();

  return true;
}

bool SchemaManager::addMissingColumns(
    int deviceId, const std::vector<RegisterDefinition> &registers) {
  std::string tableName = getTableName(deviceId);
  std::vector<std::string> existingColumns = getExistingColumns(tableName);

  // Ensure timestamp column exists
  if (!columnExists(tableName, TIMESTAMP_COLUMN_NAME)) {
    pqxx::work txn(dbManager.getConnection());
    std::ostringstream query;
    query << "ALTER TABLE " << quoteIdentifier(tableName);
    query << " ADD COLUMN " << quoteIdentifier(TIMESTAMP_COLUMN_NAME)
          << " TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP";

    try {
      txn.exec(query.str());
      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error: Failed to add timestamp column to table "
                << tableName << ": " << e.what() << std::endl;
      return false;
    }
  }

  for (const auto &reg : registers) {
    if (!columnExists(tableName, reg.name)) {
      pqxx::work txn(dbManager.getConnection());
      std::ostringstream query;
      query << "ALTER TABLE " << quoteIdentifier(tableName);
      query << " ADD COLUMN " << quoteIdentifier(reg.name) << " "
            << getColumnType(reg);

      try {
        txn.exec(query.str());
        txn.commit();
      } catch (const std::exception &e) {
        std::cerr << "Error: Failed to add column " << reg.name << " to table "
                  << tableName << ": " << e.what() << std::endl;
        return false;
      }
    } else {
      // Column exists, check if type needs to be altered
      std::string currentType = getCurrentColumnType(tableName, reg.name);
      std::string expectedType = getColumnType(reg);

      if (needsColumnAlter(currentType, expectedType)) {
        std::cout << "Column " << reg.name
                  << " type mismatch. Current: " << currentType
                  << ", Expected: " << expectedType
                  << ". Altering column type..." << std::endl;

        if (!alterColumnType(tableName, reg.name, expectedType)) {
          std::cerr << "Error: Failed to alter column " << reg.name
                    << " type in table " << tableName << std::endl;
          return false;
        }
      }
    }
  }

  return true;
}

std::string SchemaManager::getTableName(int deviceId) const {
  return "modbus_" + std::to_string(deviceId);
}

std::string SchemaManager::getColumnType(const RegisterDefinition &reg) const {
  switch (reg.type) {
  case RegisterType::Float32:
    return "FLOAT";

  case RegisterType::Int16:
  case RegisterType::Int32: {
    // Check if scale is integer
    bool isIntegerScale = (reg.scale == static_cast<int64_t>(reg.scale));

    if (isIntegerScale) {
      return "INTEGER";
    } else {
      if (reg.type == RegisterType::Int32) {
        return "DECIMAL(10, 2)";
      } else {
        return "DECIMAL(6, 2)";
      }
    }
  }

  default:
    return "FLOAT";
  }
}

std::vector<std::string>
SchemaManager::getExistingColumns(const std::string &tableName) {
  std::vector<std::string> columns;

  if (!dbManager.isConnected()) {
    return columns;
  }

  try {
    pqxx::work txn(dbManager.getConnection());
    std::string query = "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = " +
                        txn.quote(tableName);
    pqxx::result result = txn.exec(query);
    txn.commit();

    for (const auto &row : result) {
      columns.push_back(row[0].as<std::string>());
    }
  } catch (const std::exception &e) {
    std::cerr << "Error: Failed to get existing columns: " << e.what()
              << std::endl;
  }

  return columns;
}

bool SchemaManager::columnExists(const std::string &tableName,
                                 const std::string &columnName) {
  if (!dbManager.isConnected()) {
    return false;
  }

  try {
    pqxx::work txn(dbManager.getConnection());
    std::string query = "SELECT EXISTS (SELECT FROM information_schema.columns "
                        "WHERE table_name = " +
                        txn.quote(tableName) +
                        " AND column_name = " + txn.quote(columnName) + ")";
    pqxx::result result = txn.exec(query);
    txn.commit();

    if (!result.empty() && result[0][0].as<bool>()) {
      return true;
    }
    return false;
  } catch (const std::exception &e) {
    std::cerr << "Error: Failed to check column existence: " << e.what()
              << std::endl;
    return false;
  }
}

std::string SchemaManager::getCurrentColumnType(const std::string &tableName,
                                                const std::string &columnName) {
  if (!dbManager.isConnected()) {
    return "";
  }

  try {
    pqxx::work txn(dbManager.getConnection());
    std::string query = "SELECT data_type, numeric_precision, numeric_scale "
                        "FROM information_schema.columns "
                        "WHERE table_name = " +
                        txn.quote(tableName) +
                        " AND column_name = " + txn.quote(columnName);
    pqxx::result result = txn.exec(query);
    txn.commit();

    if (!result.empty()) {
      std::string dataType = result[0][0].as<std::string>();

      // For numeric/decimal types, include precision and scale
      if (dataType == "numeric" || dataType == "decimal") {
        if (!result[0][1].is_null() && !result[0][2].is_null()) {
          int precision = result[0][1].as<int>();
          int scale = result[0][2].as<int>();
          return "DECIMAL(" + std::to_string(precision) + ", " +
                 std::to_string(scale) + ")";
        }
        return "DECIMAL";
      }

      // Normalize other types to uppercase
      std::string normalized = dataType;
      std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                     ::toupper);
      return normalized;
    }
    return "";
  } catch (const std::exception &e) {
    std::cerr << "Error: Failed to get column type: " << e.what() << std::endl;
    return "";
  }
}

bool SchemaManager::needsColumnAlter(const std::string &currentType,
                                     const std::string &expectedType) {
  if (currentType.empty() || expectedType.empty()) {
    return false;
  }

  // Normalize both types for comparison
  std::string currentUpper = currentType;
  std::string expectedUpper = expectedType;
  std::transform(currentUpper.begin(), currentUpper.end(), currentUpper.begin(),
                 ::toupper);
  std::transform(expectedUpper.begin(), expectedUpper.end(),
                 expectedUpper.begin(), ::toupper);

  // Remove whitespace for comparison
  currentUpper.erase(
      std::remove_if(currentUpper.begin(), currentUpper.end(), ::isspace),
      currentUpper.end());
  expectedUpper.erase(
      std::remove_if(expectedUpper.begin(), expectedUpper.end(), ::isspace),
      expectedUpper.end());

  return currentUpper != expectedUpper;
}

bool SchemaManager::alterColumnType(const std::string &tableName,
                                    const std::string &columnName,
                                    const std::string &newType) {
  if (!dbManager.isConnected()) {
    return false;
  }

  try {
    pqxx::work txn(dbManager.getConnection());
    std::ostringstream query;
    query << "ALTER TABLE " << quoteIdentifier(tableName);
    query << " ALTER COLUMN " << quoteIdentifier(columnName);
    query << " TYPE " << newType;

    txn.exec(query.str());
    txn.commit();
    return true;
  } catch (const std::exception &e) {
    std::cerr << "Error: Failed to alter column type: " << e.what()
              << std::endl;
    return false;
  }
}

} // namespace ModbusLogger
