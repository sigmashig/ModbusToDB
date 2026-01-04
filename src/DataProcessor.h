#ifndef DATAPROCESSOR_H
#define DATAPROCESSOR_H

#include "Types.h"
#include <vector>
#include <map>
#include <functional>

namespace ModbusLogger {

class DataProcessor {
public:
    using PreprocessFunction = std::function<double(uint16_t address, double value, const RegisterValueMap& allValues)>;

    DataProcessor();
    
    void setPreprocessFunction(PreprocessFunction func);
    
    RegisterValue processRegister(const RegisterDefinition& regDef, const std::vector<uint16_t>& rawValues, size_t index);
    std::vector<RegisterValue> processRegisters(const std::vector<RegisterDefinition>& regDefs, const std::vector<uint16_t>& rawValues);
    
    void updateRegisterValueMap(const RegisterValue& value);
    const RegisterValueMap& getRegisterValueMap() const;

private:
    double applyScale(double value, double scale);
    double convertToDouble(const RegisterDefinition& regDef, const std::vector<uint16_t>& rawValues, size_t index);
    double applyPreprocessing(const RegisterDefinition& regDef, double value);

    PreprocessFunction preprocessFunction;
    RegisterValueMap registerValueMap;
};

} // namespace ModbusLogger

#endif // DATAPROCESSOR_H

