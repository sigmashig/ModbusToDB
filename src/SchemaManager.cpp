#include "SchemaManager.h"
#include <pqxx/pqxx>
#include <sstream>
#include <algorithm>
#include <iostream>

namespace ModbusLogger {

namespace {
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

SchemaManager::SchemaManager(DatabaseManager& dbManager)
    : dbManager(dbManager)
{
}

bool SchemaManager::ensureTableExists(int deviceId, const std::vector<RegisterDefinition>& registers) {
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

    for (const auto& reg : registers) {
        query << ", " << quoteIdentifier(reg.name) << " " << getColumnType(reg);
    }

    query << ")";
    std::string queryStr = query.str();
    txn.exec(queryStr);
    txn.commit();


    return true;
}

bool SchemaManager::addMissingColumns(int deviceId, const std::vector<RegisterDefinition>& registers) {
    std::string tableName = getTableName(deviceId);
    std::vector<std::string> existingColumns = getExistingColumns(tableName);

    for (const auto& reg : registers) {
        if (!columnExists(tableName, reg.name)) {
            pqxx::work txn(dbManager.getConnection());
            std::ostringstream query;
            query << "ALTER TABLE " << quoteIdentifier(tableName);
            query << " ADD COLUMN " << quoteIdentifier(reg.name) << " " << getColumnType(reg);

            try {
                txn.exec(query.str());
                txn.commit();
            } catch (const std::exception& e) {
                std::cerr << "Error: Failed to add column " << reg.name << " to table " << tableName << ": " << e.what() << std::endl;
                return false;
            }
        }
    }

    return true;
}

std::string SchemaManager::getTableName(int deviceId) const {
    return "modbus_" + std::to_string(deviceId);
}

std::string SchemaManager::getColumnType(const RegisterDefinition& reg) const {
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

std::vector<std::string> SchemaManager::getExistingColumns(const std::string& tableName) {
    std::vector<std::string> columns;

    if (!dbManager.isConnected()) {
        return columns;
    }

    try {
        pqxx::work txn(dbManager.getConnection());
        std::string query = "SELECT column_name FROM information_schema.columns WHERE table_name = " + 
                           txn.quote(tableName);
        pqxx::result result = txn.exec(query);
        txn.commit();

        for (const auto& row : result) {
            columns.push_back(row[0].as<std::string>());
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: Failed to get existing columns: " << e.what() << std::endl;
    }

    return columns;
}

bool SchemaManager::columnExists(const std::string& tableName, const std::string& columnName) {
    if (!dbManager.isConnected()) {
        return false;
    }

    try {
        pqxx::work txn(dbManager.getConnection());
        std::string query = "SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_name = " + 
                           txn.quote(tableName) + " AND column_name = " + txn.quote(columnName) + ")";
        pqxx::result result = txn.exec(query);
        txn.commit();

        if (!result.empty() && result[0][0].as<bool>()) {
            return true;
        }
        return false;
    } catch (const std::exception& e) {
        std::cerr << "Error: Failed to check column existence: " << e.what() << std::endl;
        return false;
    }
}

} // namespace ModbusLogger

