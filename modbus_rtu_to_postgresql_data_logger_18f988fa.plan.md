---
name: Modbus RTU to PostgreSQL Data Logger
overview: Create a Linux console application in C++ that reads Modbus RTU registers from a device, processes them according to configuration, and stores the data in PostgreSQL. The application will auto-create database tables based on register definitions and support register ranges as input.
todos:
  - id: setup-build
    content: Set up CMake build system with dependencies (libmodbus, libpq/libpqxx, nlohmann/json)
    status: completed
  - id: config-parser
    content: Implement ConfigParser to read and validate JSON configuration file
    status: completed
    dependencies:
      - setup-build
  - id: modbus-client
    content: Implement ModbusClient wrapper for libmodbus RTU communication
    status: completed
    dependencies:
      - setup-build
  - id: database-manager
    content: Implement DatabaseManager for PostgreSQL connection and operations
    status: completed
    dependencies:
      - setup-build
  - id: schema-manager
    content: Implement SchemaManager to auto-create database tables based on register definitions
    status: completed
    dependencies:
      - database-manager
      - config-parser
  - id: register-resolver
    content: Implement RegisterResolver to parse range notation and resolve register addresses from config
    status: completed
    dependencies:
      - config-parser
  - id: data-processor
    content: Implement DataProcessor to apply scale, type conversion, and preprocessing pipeline
    status: completed
    dependencies:
      - config-parser
  - id: main-application
    content: Implement main application with CLI argument parsing and orchestration logic
    status: completed
    dependencies:
      - modbus-client
      - database-manager
      - schema-manager
      - register-resolver
      - data-processor
  - id: error-handling
    content: Add comprehensive error handling and stderr logging throughout
    status: completed
    dependencies:
      - main-application
  - id: example-config
    content: Create example config.json file with documentation
    status: completed
    dependencies:
      - config-parser
---

# Modbus RTU to PostgreSQL Data Logger

## Language Choice

**C++17+** - Provides good performance for serial communication, strong type safety, and excellent library support for both Modbus RTU and PostgreSQL. Use object-oriented approach.

## Architecture Overview

```mermaid
flowchart TD
    A[Command Line Args] --> B[Config Parser]
    B --> C[Modbus RTU Client]
    B --> D[PostgreSQL Connection]
    B --> E[Register Resolver]
    E --> C
    C --> F[Data Processor]
    F --> G[Schema Manager]
    G --> D
    F --> D
    H[Error Handler] --> I[stderr]
    C -.errors.-> H
    D -.errors.-> H
    F -.errors.-> H
```



## Project Structure

```javascript
/
├── src/
│   ├── Main.cpp              # Entry point, CLI argument parsing
│   ├── ConfigParser.h/cpp    # JSON config file parsing
│   ├── ModbusClient.h/cpp    # Modbus RTU communication (using libmodbus)
│   ├── DatabaseManager.h/cpp # PostgreSQL connection and operations
│   ├── SchemaManager.h/cpp   # Auto-create/update database tables
│   ├── RegisterResolver.h/cpp # Resolve register addresses from ranges/config
│   ├── DataProcessor.h/cpp   # Apply scale, preprocessing, type conversion
│   └── Types.h               # Common types and constants
├── include/                  # Public headers if needed
├── test/                     # Unit tests
├── CMakeLists.txt            # Build configuration
├── config.json.example        # Example configuration file
└── README.md                 # Documentation
```



## Key Components

### 1. Configuration File Structure (`config.json`)

```json
{
  "database": {
    "connection_string": "postgresql://user:pass@host:5432/dbname"
  },
  "devices": [
    {
      "id": 1,
      "connection": {
        "port": "/dev/ttyUSB0",
        "baud_rate": 9600,
        "parity": "N",
        "data_bits": 8,
        "stop_bits": 1
      },
      "registers": [
        {
          "address": 100,
          "name": "temperature",
          "type": "int16",
          "scale": 0.1,
          "preprocessing": false
        }
      ]
    }
  ]
}
```



### 2. Command Line Interface

- `--config <path>` (default: `./config.json`) shorter: -c
- `--device-id <id>` (default: `1`) shorter -d
- `--registers <range>` (e.g., `100-200` or `100,101,102`  or `--registers 100-105 --registers 200` ) shorter -r
- `--register-file <path>` (alternative to `--registers`) shorter -f

### 3. Core Implementation Details

**ModbusClient** (`src/ModbusClient.h/cpp`):

- Use `libmodbus` library for Modbus RTU communication
- Handle connection, read holding/input registers based on type
- The registers are processing in the same order as set in input parameters. The values stored in the memory and accessible from all components
- Error handling and retry logic

**DatabaseManager** (`src/DatabaseManager.h/cpp`):

- Use `libpq` (PostgreSQL C client) or `pqxx` (C++ wrapper)
- Connection pooling, prepared statements
- Transaction management

**SchemaManager** (`src/SchemaManager.h/cpp`):

- Auto-detect table existence (`modbus_<device_id>`)
- Create table with appropriate column types based on register definitions
- Handle schema updates if registers change (no delete "old" columns) 

**RegisterResolver** (`src/RegisterResolver.h/cpp`):

- Parse range notation (`100-200` → addresses 100, 101, ..., 200)
- Resolve register definitions from config file
- Validate addresses exist in config

**DataProcessor** (`src/DataProcessor.h/cpp`):

- Apply scale factors
- Type conversion (int16/int32/float32)
- Preprocessing pipeline (extensible for future functions) - call method preprocess(address, value). This method should have access to all register values in this session 
- Map to PostgreSQL column types

### 4. Database Schema Generation

- Table: `modbus_<device_id>`
- Columns:
- `id SERIAL PRIMARY KEY`
- `<register_name> <TYPE>` where TYPE is:
    - `DECIMAL(10,2)` for int32 with non-integer scale
    - `DECIMAL(6,2)` for int16 with non-integer scale  
    - `INTEGER` for int16/int32 with integer scale
    - `FLOAT` for float32

### 5. Error Handling

- All errors to `stderr`
- Return appropriate exit codes
- Log Modbus communication errors
- Log database connection/query errors
- Validate config file structure

## Dependencies

- **libmodbus** - Modbus RTU/TCP library
- **libpq** or **libpqxx** - PostgreSQL client library
- **nlohmann/json** - JSON parsing (header-only)
- **CMake** - Build system
- **C++17** compiler (GCC/Clang)

## Implementation Steps

1. Set up CMake build system with dependencies
2. Implement JSON config parser
3. Implement Modbus RTU client wrapper
4. Implement PostgreSQL connection and basic operations
5. Implement schema auto-creation logic
6. Implement register resolver (range parsing)
7. Implement data processing pipeline
8. Implement main application logic with CLI parsing
9. Add error handling throughout
10. Create example config file
11. Write unit tests for core components

## Open Questions/Assumptions

- Range format: Assuming `100-200` notation (can be adjusted)