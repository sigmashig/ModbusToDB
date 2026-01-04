#include "DataProcessor.h"
#include <cmath>
#include <cstring>

namespace ModbusLogger {

DataProcessor::DataProcessor()
{
}

void DataProcessor::setPreprocessFunction(PreprocessFunction func) {
    preprocessFunction = func;
}

RegisterValue DataProcessor::processRegister(const RegisterDefinition& regDef, const std::vector<uint16_t>& rawValues, size_t index) {
    RegisterValue result;
    result.address = regDef.address;
    result.name = regDef.name;
    result.type = regDef.type;
    result.scale = regDef.scale;
    result.preprocessing = regDef.preprocessing;

    // Convert raw values to double based on type
    double value = convertToDouble(regDef, rawValues, index);
    result.rawValue = static_cast<int64_t>(value);

    // Apply scale
    value = applyScale(value, regDef.scale);

    // Note: Preprocessing is deferred to second pass when all register values are available
    result.processedValue = value;
    return result;
}

std::vector<RegisterValue> DataProcessor::processRegisters(const std::vector<RegisterDefinition>& regDefs, const std::vector<uint16_t>& rawValues) {
    std::vector<RegisterValue> results;
    results.reserve(regDefs.size());

    // Process registers, tracking rawValues index separately since int32/float32 use 2 values
    size_t rawIndex = 0;
    for (size_t i = 0; i < regDefs.size(); ++i) {
        RegisterValue value = processRegister(regDefs[i], rawValues, rawIndex);
        results.push_back(value);
        registerValueMap[value.address] = value;
        
        // Advance rawIndex based on register type
        if (regDefs[i].type == RegisterType::Int16) {
            rawIndex += 1;
        } else if (regDefs[i].type == RegisterType::Int32 || regDefs[i].type == RegisterType::Float32) {
            rawIndex += 2;
        }
    }

    // Second pass: apply preprocessing with full context (all register values available)
    if (preprocessFunction) {
        for (auto& result : results) {
            const RegisterDefinition* regDef = nullptr;
            for (const auto& def : regDefs) {
                if (def.address == result.address) {
                    regDef = &def;
                    break;
                }
            }
            
            if (regDef && regDef->preprocessing) {
                result.processedValue = applyPreprocessing(*regDef, result.processedValue);
                // Update the map with the preprocessed value
                registerValueMap[result.address] = result;
            }
        }
    }

    return results;
}

void DataProcessor::updateRegisterValueMap(const RegisterValue& value) {
    registerValueMap[value.address] = value;
}

const RegisterValueMap& DataProcessor::getRegisterValueMap() const {
    return registerValueMap;
}

double DataProcessor::applyScale(double value, double scale) {
    return value * scale;
}

double DataProcessor::convertToDouble(const RegisterDefinition& regDef, const std::vector<uint16_t>& rawValues, size_t index) {
    switch (regDef.type) {
        case RegisterType::Int16: {
            if (index >= rawValues.size()) {
                return 0.0;
            }
            int16_t value = static_cast<int16_t>(rawValues[index]);
            return static_cast<double>(value);
        }

        case RegisterType::Int32: {
            if (index + 1 >= rawValues.size()) {
                return 0.0;
            }
            // Modbus registers are 16-bit, so int32 requires 2 registers
            uint32_t low = rawValues[index];
            uint32_t high = rawValues[index + 1];
            int32_t value = static_cast<int32_t>((high << 16) | low);
            return static_cast<double>(value);
        }

        case RegisterType::Float32: {
            if (index + 1 >= rawValues.size()) {
                return 0.0;
            }
            // Modbus registers are 16-bit, so float32 requires 2 registers
            uint32_t low = rawValues[index];
            uint32_t high = rawValues[index + 1];
            uint32_t combined = (high << 16) | low;
            
            float floatValue;
            std::memcpy(&floatValue, &combined, sizeof(float));
            return static_cast<double>(floatValue);
        }

        default:
            return 0.0;
    }
}

double DataProcessor::applyPreprocessing(const RegisterDefinition& regDef, double value) {
    if (preprocessFunction) {
        return preprocessFunction(regDef.address, value, registerValueMap);
    }
    return value;
}

} // namespace ModbusLogger

