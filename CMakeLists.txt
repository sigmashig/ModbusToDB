cmake_minimum_required(VERSION 3.15)
project(ModbusLogger VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Set default build type to Release
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# Find dependencies
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBMODBUS REQUIRED libmodbus)

find_package(PostgreSQL REQUIRED)

# Try to find libpqxx via pkg-config first
pkg_check_modules(PQXX libpqxx QUIET)

if(PQXX_FOUND)
    set(PQXX_LIBRARY ${PQXX_LIBRARIES})
    set(PQXX_INCLUDE_DIR ${PQXX_INCLUDE_DIRS})
else()
    # Fallback to manual finding
    find_library(PQXX_LIBRARY pqxx)
    find_path(PQXX_INCLUDE_DIR pqxx/pqxx.h)
    
    if(NOT PQXX_LIBRARY OR NOT PQXX_INCLUDE_DIR)
        message(FATAL_ERROR "libpqxx not found. Please install libpqxx-dev")
    endif()
endif()

# Include directories
include_directories(${LIBMODBUS_INCLUDE_DIRS})
include_directories(${PQXX_INCLUDE_DIR})
include_directories(${PostgreSQL_INCLUDE_DIRS})

# Download nlohmann/json if not present
include(FetchContent)
FetchContent_Declare(
    json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.2
)
FetchContent_MakeAvailable(json)

# Source files
set(SOURCES
    src/Main.cpp
    src/ConfigParser.cpp
    src/ModbusClient.cpp
    src/DatabaseManager.cpp
    src/SchemaManager.cpp
    src/RegisterResolver.cpp
    src/DataProcessor.cpp
    src/DaemonManager.cpp
    src/PeriodicScheduler.cpp
    src/PeriodParser.cpp
)

# Headers
set(HEADERS
    src/ConfigParser.h
    src/ModbusClient.h
    src/DatabaseManager.h
    src/SchemaManager.h
    src/RegisterResolver.h
    src/DataProcessor.h
    src/Types.h
    src/DaemonManager.h
    src/PeriodicScheduler.h
    src/PeriodParser.h
)

# Create executable
add_executable(${PROJECT_NAME} ${SOURCES} ${HEADERS})

# Link libraries
target_link_libraries(${PROJECT_NAME}
    ${LIBMODBUS_LIBRARIES}
    ${PQXX_LIBRARY}
    ${PostgreSQL_LIBRARIES}
    nlohmann_json::nlohmann_json
)

target_compile_options(${PROJECT_NAME} PRIVATE ${LIBMODBUS_CFLAGS_OTHER})

# Compiler-specific options
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    target_compile_options(${PROJECT_NAME} PRIVATE -Wall -Wextra -Wpedantic)
endif()

# Copy config.json to build directory
if(EXISTS ${CMAKE_SOURCE_DIR}/config.json)
    configure_file(
        ${CMAKE_SOURCE_DIR}/config.json
        ${CMAKE_BINARY_DIR}/config.json
        COPYONLY
    )
    message(STATUS "config.json will be copied to build directory")
else()
    message(WARNING "config.json not found in source directory. Create it from config.json.example")
endif()

# Installation
install(TARGETS ${PROJECT_NAME} DESTINATION bin)

# Install config.json to /etc/modbuslogger
if(EXISTS ${CMAKE_SOURCE_DIR}/config.json)
    install(FILES ${CMAKE_SOURCE_DIR}/config.json 
            DESTINATION /etc/modbuslogger
            PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)
    message(STATUS "config.json will be installed to /etc/modbuslogger/config.json")
else()
    message(WARNING "config.json not found. It will not be installed.")
endif()

