#include "DatabaseManager.h"
#include <iostream>
#include <stdexcept>

namespace ModbusLogger {

DatabaseManager::DatabaseManager(const std::string& connectionString)
    : connectionString(connectionString)
{
}

bool DatabaseManager::connect() {
    try {
        connection = std::make_unique<pqxx::connection>(connectionString);
        if (!connection->is_open()) {
            lastError = "Failed to open database connection";
            std::cerr << "Error: " << lastError << std::endl;
            return false;
        }
        return true;
    } catch (const std::exception& e) {
        lastError = "Database connection error: " + std::string(e.what());
        std::cerr << "Error: " << lastError << std::endl;
        return false;
    }
}

bool DatabaseManager::isConnected() const {
    return connection != nullptr && connection->is_open();
}

void DatabaseManager::disconnect() {
    if (connection && connection->is_open()) {
        connection->disconnect();
    }
    connection.reset();
}

bool DatabaseManager::executeQuery(const std::string& query) {
    if (!isConnected()) {
        lastError = "Database not connected";
        std::cerr << "Error: " << lastError << std::endl;
        return false;
    }

    try {
        pqxx::work txn(*connection);
        txn.exec(query);
        txn.commit();
        return true;
    } catch (const std::exception& e) {
        lastError = "Query execution error: " + std::string(e.what());
        std::cerr << "Error: " << lastError << std::endl;
        return false;
    }
}

bool DatabaseManager::tableExists(const std::string& tableName) {
    if (!isConnected()) {
        lastError = "Database not connected";
        std::cerr << "Error: " << lastError << std::endl;
        return false;
    }

    try {
        pqxx::work txn(*connection);
        std::string query = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = " + 
                           txn.quote(tableName) + ")";
        pqxx::result result = txn.exec(query);
        txn.commit();
        
        if (!result.empty() && result[0][0].as<bool>()) {
            return true;
        }
        return false;
    } catch (const std::exception& e) {
        lastError = "Table existence check error: " + std::string(e.what());
        std::cerr << "Error: " << lastError << std::endl;
        return false;
    }
}

std::string DatabaseManager::getLastError() const {
    return lastError;
}

pqxx::connection& DatabaseManager::getConnection() {
    if (!connection) {
        throw std::runtime_error("Database connection not established");
    }
    return *connection;
}

} // namespace ModbusLogger

