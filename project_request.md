Create a new project (please, ask clarification if needed):
- it will be run as linux console application
- propose a language for the project
- input parameters:
    - path to the config file (default= ./config.json
    - id of modbus device (default= 1)
    - list of register addresses or range of addresses
    - or filename with the list of these registers
- config file includes:
    - list of available modbus devices
    - list of available registers for each device
    - connection parameters for each device
    - connection string to PostgreSQL database
- the list of registers has format: {"address": "value", "name": "register_name", "type": "int16", "scale": 1.0, "preprocessing": true}
-- where:
-- address is the address of the register in the modbus device
-- name is the name of the register
-- type is the type of the register (int16, int32, float32)
-- scale is the scale of the register (default= 1.0)
-- preprocessing is the preprocessing function for the register (default= false)

- the project will:
    - read the registers from the modbus device
    - store the registers in the PostgreSQL database
    - print errors to the stderr

The data stored in the PostgreSQL database has format:
- table name: modbus_<device id>
- columns:
    - id: serial primary key
    - <name of register>: <TYPE_OF_COLUMN>
where <TYPE_OF_COLUMN> is:
- DECIMAL(10, 2) for int32 and scale is not integer
- DECIMAL(6, 2) for int16 and scale is not integer
- INTEGER for int16/int32 and scale is integer
- FLOAT for float32
    