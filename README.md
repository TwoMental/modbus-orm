# ModbusORM
Object Relational Mapping (ORM) for Modbus

## What is ModbusORM
ModbusORM is a golang package allows you to read/write Modbus data by struct with tag (`morm`).

## Features
- **Multiple Register Types Support**
  - Coil (Read/Write)
  - Discrete Input (Read only)
  - Input Register (Read only)
  - Holding Register (Read/Write)

- **Data Types**
  - U16 (Unsigned 16-bit)
  - S16 (Signed 16-bit)
  - U32 (Unsigned 32-bit)
  - S32 (Signed 32-bit)
  - Float (32-bit floating point)
  - String
  - OriginByte (Raw byte array)
  - Arrays/Slices of above types

- **Connection Types**
  - Modbus TCP/IP
  - Modbus RTU (Serial)

- **Advanced Features**
  - Connection pooling with configurable pool size and connection lifetime
  - Connection reuse for same IP:Port (TCP) or same serial port (RTU)
  - Support different slave IDs on the same connection (TCP/RTU)
  - Block reading mode for efficient bulk data reading
  - Coefficient and offset support for data transformation
  - Byte order support (Big Endian / Little Endian)
  - Context support for cancellation and timeout

## Usage
- Define the points
    ```go
    // modbusorm.Point is a map with string key and modbusorm.PointDetails value.
    // key is the point name, which will be used in tag (morm)
    point := modbusorm.Point{
		"voltage": modbusorm.PointDetails{
			// Address of this point.
			Addr: 100,
			// Quantity of this poiont.
			Quantity: 1,
			// Coefficient of this point. Default 1.
			// For example:
			//      if read 10 from modbus server,
			//      and coefficient is 0.1,
			//      then you will get 1 in result.
			Coefficient: 0.1,
			// Data type of this point
			//      U16, S16, U32, S32, Float
			DataType: modbusorm.PointDataTypeU16,
			// RegisterType of this point
			//		Coil, Discrete Input, Input Register, Holding Register
			RegisterType: modbusorm.RegisterTypeHoldingRegister,
		},
	}
    ```
- Define a struct with `morm` tag.
    ```go
    type Data struct {
        Voltage     *float64             `morm:"voltage"`
        Temperature float64              `morm:"temperature"`
        Star        []float64            `morm:"star"`
        Origin      modbusorm.OriginByte `morm:"origin"`
        Word        string               `morm:"word"`
        Unknown     *float64             `morm:"unkonwn"`
    }
    ```
- Read/Write with your modbus server.
    ```go
    // new
	conn := modbusorm.NewModbusTCP(
		// Host of modbus server.
		"localhost",
		// Port of modbus server.
		1502,
		// Point define before.
		point,
		// Block mode setting. Default false.
		//  With block mode, ModbusORM will try to read data by block,
		//  rather than by single point.
		//  If set to true, two more parameters is avaliable.
		modbusorm.WithBlock(true),
		// Max block size. Default 100.
		//  Only work with block mode.
		modbusorm.WithMaxBlockSize(100),
		// Max gap in block. Default 10.
		//  Only work with block mode.
		modbusorm.WithMaxGapInBlock(10),
		// timeout setting.
		modbusorm.WithTimeout(1*time.Second),
		// max open connections in connection pool.
		//  When MaxOpenConns == 1, only one connection will be established with the slave,
		//  and connection pool mechanism is not used (a single connection is maintained directly).
		modbusorm.WithMaxOpenConns(3),
		// max connection lifetime in connection pool.
		modbusorm.WithConnMaxLifetime(30*time.Minute),
		// reuse connection for same ip:port (TCP) or same serial port (RTU).
		//  When enabled, multiple Modbus instances with same IP:Port (TCP) or ComAddr (RTU)
		//  will share the same connection pool, and can use different slave IDs.
		modbusorm.WithReuseConn(false),
		// slave ID setting.
		modbusorm.WithSlaveID(1),
	)
	// connect
	conn.Conn()
	// read
	data := &Data{}
	conn.GetValues(context.Background(), data)
    ```
- See more details in [_example](./_example/)

## Demo
- Modbus TCP
    - go to example folder:  `cd _example/modbus_tcp`
    - start a demo server: `go run server.go`
    - start a demo client: `go run client.go`

## TODOs
- [ ] Example
- [ ] Logger
- [x] Modbus RTU 
- [x] More data types (U16, S16, U32, S32, Float)
- [x] Coil and Discrete Input register types support
- [x] Precision control for floating point calculations
- [x] Block reading mode optimization
- [x] Support different slave id on the same serial port (RTU) and same IP:Port (TCP)