#include "RegisterResolver.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

namespace ModbusLogger {

RegisterResolver::RegisterResolver(const DeviceConfig &deviceConfig)
    : deviceConfig(deviceConfig) {
  // Build map for quick lookup
  for (const auto &reg : deviceConfig.registers) {
    registerMap[reg.address] = reg;
  }
}

std::vector<RegisterDefinition>
RegisterResolver::resolveFromRanges(const std::vector<std::string> &ranges) {
  std::vector<RegisterDefinition> result;

  for (const std::string &range : ranges) {
    std::vector<uint16_t> addresses = parseRange(range);
    for (uint16_t address : addresses) {
      RegisterDefinition *regDef = findRegisterDefinition(address);
      if (regDef != nullptr) {
        result.push_back(*regDef);
      } else {
        // std::cerr << "Warning: Register address " << address << " not found
        // in device config" << std::endl;
      }
    }
  }

  return result;
}

std::vector<RegisterDefinition>
RegisterResolver::resolveFromFile(const std::string &filePath) {
  std::ifstream file(filePath);
  if (!file.is_open()) {
    throw std::runtime_error("Cannot open register file: " + filePath);
  }

  std::vector<std::string> ranges;
  std::string line;
  while (std::getline(file, line)) {
    // Trim whitespace
    line.erase(0, line.find_first_not_of(" \t\r\n"));
    line.erase(line.find_last_not_of(" \t\r\n") + 1);

    if (!line.empty()) {
      ranges.push_back(line);
    }
  }

  return resolveFromRanges(ranges);
}

std::vector<RegisterDefinition>
RegisterResolver::resolveFromAddresses(const std::vector<uint16_t> &addresses) {
  std::vector<RegisterDefinition> result;

  for (uint16_t address : addresses) {
    RegisterDefinition *regDef = findRegisterDefinition(address);
    if (regDef != nullptr) {
      result.push_back(*regDef);
    } else {
      std::cerr << "Warning: Register address " << address
                << " not found in device config" << std::endl;
    }
  }

  return result;
}

std::vector<uint16_t> RegisterResolver::parseRange(const std::string &range) {
  // Check if it's a range (contains '-')
  size_t dashPos = range.find('-');
  if (dashPos != std::string::npos) {
    std::string startStr = range.substr(0, dashPos);
    std::string endStr = range.substr(dashPos + 1);

    try {
      uint16_t start = static_cast<uint16_t>(std::stoul(startStr));
      uint16_t end = static_cast<uint16_t>(std::stoul(endStr));

      if (start > end) {
        std::cerr << "Warning: Invalid range " << range
                  << " (start > end), skipping" << std::endl;
        return {};
      }

      std::vector<uint16_t> addresses;
      for (uint16_t addr = start; addr <= end; ++addr) {
        addresses.push_back(addr);
      }
      return addresses;
    } catch (const std::exception &e) {
      std::cerr << "Warning: Invalid range format " << range << ", skipping"
                << std::endl;
      return {};
    }
  }

  // Check if it's comma-separated
  if (range.find(',') != std::string::npos) {
    return parseCommaSeparated(range);
  }

  // Single address
  try {
    uint16_t address = static_cast<uint16_t>(std::stoul(range));
    return {address};
  } catch (const std::exception &e) {
    std::cerr << "Warning: Invalid address format " << range << ", skipping"
              << std::endl;
    return {};
  }
}

std::vector<uint16_t>
RegisterResolver::parseCommaSeparated(const std::string &addresses) {
  std::vector<uint16_t> result;
  std::istringstream iss(addresses);
  std::string token;

  while (std::getline(iss, token, ',')) {
    // Trim whitespace
    token.erase(0, token.find_first_not_of(" \t"));
    token.erase(token.find_last_not_of(" \t") + 1);

    if (!token.empty()) {
      try {
        uint16_t address = static_cast<uint16_t>(std::stoul(token));
        result.push_back(address);
      } catch (const std::exception &e) {
        std::cerr << "Warning: Invalid address format " << token << ", skipping"
                  << std::endl;
      }
    }
  }

  return result;
}

RegisterDefinition *RegisterResolver::findRegisterDefinition(uint16_t address) {
  auto it = registerMap.find(address);
  if (it != registerMap.end()) {
    return &it->second;
  }
  return nullptr;
}

} // namespace ModbusLogger
