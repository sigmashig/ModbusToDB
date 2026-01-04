#ifndef SCHEMAMANAGER_H
#define SCHEMAMANAGER_H

#include "Types.h"
#include "DatabaseManager.h"
#include <vector>
#include <string>

namespace ModbusLogger {

class SchemaManager {
public:
    explicit SchemaManager(DatabaseManager& dbManager);

    bool ensureTableExists(int deviceId, const std::vector<RegisterDefinition>& registers);
    bool addMissingColumns(int deviceId, const std::vector<RegisterDefinition>& registers);

private:
    std::string getTableName(int deviceId) const;
    std::string getColumnType(const RegisterDefinition& reg) const;
    std::vector<std::string> getExistingColumns(const std::string& tableName);
    bool columnExists(const std::string& tableName, const std::string& columnName);
    std::string getCurrentColumnType(const std::string& tableName, const std::string& columnName);
    bool needsColumnAlter(const std::string& currentType, const std::string& expectedType);
    bool alterColumnType(const std::string& tableName, const std::string& columnName, const std::string& newType);

    DatabaseManager& dbManager;
};

} // namespace ModbusLogger

#endif // SCHEMAMANAGER_H

