#ifndef DATABASEMANAGER_H
#define DATABASEMANAGER_H

#include <string>
#include <vector>
#include <map>
#include <memory>
#include <pqxx/pqxx>

namespace ModbusLogger {

class DatabaseManager {
public:
    explicit DatabaseManager(const std::string& connectionString);
    ~DatabaseManager() = default;

    DatabaseManager(const DatabaseManager&) = delete;
    DatabaseManager& operator=(const DatabaseManager&) = delete;

    bool connect();
    bool isConnected() const;
    void disconnect();

    bool executeQuery(const std::string& query);
    bool tableExists(const std::string& tableName);
    
    std::string getLastError() const;

    pqxx::connection& getConnection();

private:
    std::string connectionString;
    std::unique_ptr<pqxx::connection> connection;
    mutable std::string lastError;
};

} // namespace ModbusLogger

#endif // DATABASEMANAGER_H

