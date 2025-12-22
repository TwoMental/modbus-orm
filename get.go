package modbusorm

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sort"
	"time"

	"github.com/pkg/errors"
)

// GetValue Get value from modbus and write to v.
/*
	point: the point name
	v: the value to write (must be a pointer)
*/
func (m *Modbus) GetValue(ctx context.Context, point string, v any) error {
	// check if the point exists
	fieldDetail, ok := m.points[point]
	if !ok {
		return fmt.Errorf("point for %s not found", point)
	}

	// connection
	conn, err := m.connPool.Get()
	if err != nil {
		return fmt.Errorf("conn slave failed: %w", err)
	}
	defer m.Put(ctx, conn)

	// read data
	data, err := m.readDataByType(ctx, conn, fieldDetail.Addr, fieldDetail.getQuantity(), fieldDetail.RegisterType)
	if err != nil {
		return fmt.Errorf("ReadHoldingRegisters for %s failed, %w", point, err)
	}

	// parse data
	var parseErr error
	if fieldDetail.RegisterType == RegisterTypeCoil || fieldDetail.RegisterType == RegisterTypeDiscreteInput {
		dataInt := data[0] & 1
		switch v := v.(type) {
		case *int:
			*v = int(dataInt)
		case *int8:
			*v = int8(dataInt)
		case *int16:
			*v = int16(dataInt)
		case *int32:
			*v = int32(dataInt)
		case *int64:
			*v = int64(dataInt)
		case *uint:
			*v = uint(dataInt)
		case *uint8:
			*v = uint8(dataInt)
		case *uint16:
			*v = uint16(dataInt)
		case *uint32:
			*v = uint32(dataInt)
		case *uint64:
			*v = uint64(dataInt)
		case *bool:
			*v = dataInt != 0
		case *float32:
			*v = float32(dataInt)
		case *float64:
			*v = float64(dataInt)
		default:
			parseErr = fmt.Errorf("unsupported data type (%v) for coil/discrete", reflect.TypeOf(v))
		}
	} else {
		dataFloat64 := cal(parseDataToFloat64(data, fieldDetail.DataType, fieldDetail.OrderType), fieldDetail.getCoefficient(), fieldDetail.DataType) + fieldDetail.Offset
		switch v := v.(type) {
		case *float32:
			*v = float32(dataFloat64)
		case *float64:
			*v = dataFloat64
		case *int:
			*v = int(dataFloat64)
		case *int8:
			*v = int8(dataFloat64)
		case *int16:
			*v = int16(dataFloat64)
		case *int32:
			*v = int32(dataFloat64)
		case *int64:
			*v = int64(dataFloat64)
		case *uint:
			*v = uint(dataFloat64)
		case *uint8:
			*v = uint8(dataFloat64)
		case *uint16:
			*v = uint16(dataFloat64)
		case *uint32:
			*v = uint32(dataFloat64)
		case *string:
			*v = byte2String(data, fieldDetail.OrderType)
		case *[]int, *[]int8, *[]int16, *[]int32, *[]int64, *[]uint, *[]uint8, *[]uint16, *[]uint32, *[]uint64:
			elemType := reflect.TypeOf(v).Elem().Elem()
			elemSize := elemType.Size()
			if len(data) < int(elemSize) {
				parseErr = fmt.Errorf("insufficient data for slice type: %v", elemType)
			} else {
				buf := bytes.NewBuffer(data)
				sliceValue := reflect.ValueOf(v).Elem()
				for buf.Len() >= int(elemSize) {
					elemValue := reflect.New(elemType).Elem()
					binary.Read(buf, binary.BigEndian, elemValue.Addr().Interface())
					sliceValue.Set(reflect.Append(sliceValue, elemValue))
				}
			}
		case *OriginByte:
			*v = data
		default:
			parseErr = fmt.Errorf("unsupported data type: %v", reflect.TypeOf(v))
		}
	}

	return parseErr
}

// GetValues Get values from modbus and write to v.
/*
	Fields need to be set should have tag "morm"
	v should be a struct pointer
*/
func (m *Modbus) GetValues(ctx context.Context, v any, filter ...string) error {
	if m.withBlock {
		return m.getValuesBlock(ctx, v, filter...)
	}
	return m.getValuesSingle(ctx, v, filter...)
}

func (m *Modbus) getValuesSingle(ctx context.Context, v any, filter ...string) error {
	conn, err := m.connPool.Get()
	if err != nil {
		return fmt.Errorf("conn slave failed: %w", err)
	}
	defer m.Put(ctx, conn)
	return m.getValues(ctx, v, conn, filter...)
}

func (m *Modbus) getValues(ctx context.Context, v any, conn Client, filter ...string) error {
	// validate v
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Ptr {
		return fmt.Errorf("not support for %s", val.Kind().String())
	}

	valueElem := val.Elem()
	typeElem := reflect.TypeOf(v).Elem()

	if valueElem.Kind() != reflect.Struct {
		return fmt.Errorf("not support for %s pointer", valueElem.Kind().String())
	}

	// filter
	var needFilter bool
	var filterMap map[string]bool
	if len(filter) != 0 {
		needFilter = true
		filterMap = parseFilter(filter)
	}

	for i := 0; i < valueElem.NumField(); i++ {
		value := valueElem.Field(i)
		if value.Kind() == reflect.Struct {
			// dive
			if !value.CanAddr() {
				continue
			}
			addr := value.Addr()
			if !addr.IsValid() || !addr.CanInterface() {
				continue
			}
			if e := m.getValues(ctx, addr.Interface(), conn, filter...); e != nil {
				return e
			}
			continue
		}
		exist, fieldName := getPointTag(typeElem.Field(i))
		if !exist {
			continue
		}
		if needFilter && !filterMap[fieldName] {
			continue
		}
		fieldDetail, ok := m.points[fieldName]
		if !ok {
			continue
		}
		data, err := m.readData(ctx, conn, fieldDetail.Addr, fieldDetail.getQuantity(), fieldDetail.RegisterType)
		if err != nil {
			return fmt.Errorf("ReadHoldingRegisters for %s failed, %w", fieldName, err)
		}

		if fieldDetail.RegisterType == RegisterTypeCoil || fieldDetail.RegisterType == RegisterTypeDiscreteInput {
			dataInt := data[0] & 1
			switch value.Type().Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				value.SetInt(int64(dataInt))
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				value.SetUint(uint64(dataInt))
			case reflect.Bool:
				value.SetBool(dataInt != 0)
			case reflect.Pointer:
				// get the type pointed to by the pointer
				ptrType := value.Type().Elem()
				newValue := reflect.New(ptrType)
				// create a new instance based on the type
				switch ptrType.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					newValue.Elem().SetInt(int64(dataInt))
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					newValue.Elem().SetUint(uint64(dataInt))
				case reflect.Float32, reflect.Float64:
					newValue.Elem().SetFloat(float64(dataInt))
				case reflect.Bool:
					newValue.Elem().SetBool(dataInt != 0)
				default:
					return fmt.Errorf("parse coil/discrete for %s pointer not supported", value.Type().Kind())
				}
				// set the new pointer to the field
				value.Set(newValue)
			default:
				return fmt.Errorf("parse coil/discrete for %s not supported", value.Type().Kind())
			}
		} else {
			dataFloat64 := cal(parseDataToFloat64(data, fieldDetail.DataType, fieldDetail.OrderType), fieldDetail.getCoefficient(), fieldDetail.DataType) + fieldDetail.Offset
			switch value.Type().Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				value.SetInt(int64(dataFloat64))
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				value.SetUint(uint64(dataFloat64))
			case reflect.Float32, reflect.Float64:
				value.SetFloat(dataFloat64)
			case reflect.String:
				value.SetString(byte2String(data, fieldDetail.OrderType))
			case reflect.Pointer:
				// get the type pointed to by the pointer
				ptrType := value.Type().Elem()
				newValue := reflect.New(ptrType)
				// create a new instance based on the type
				switch ptrType.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					newValue.Elem().SetInt(int64(dataFloat64))
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					newValue.Elem().SetUint(uint64(dataFloat64))
				case reflect.Float32, reflect.Float64:
					newValue.Elem().SetFloat(dataFloat64)
				case reflect.String:
					newValue.SetString(byte2String(data, fieldDetail.OrderType))
				default:
					return fmt.Errorf("parse for %s pointer not supported", value.Type().Kind())
				}
				// set the new pointer to the field
				value.Set(newValue)
			case reflect.Slice, reflect.Array:
				// create a new slice
				newSlice := reflect.MakeSlice(value.Type(), 0, 0)

				// add elements to the slice
				if value.Type().Name() == OriginByteName { // need original bytes
					for _, b := range data {
						newSlice = reflect.Append(newSlice, reflect.ValueOf(b))
					}
				} else {
					size := 2
					if fieldDetail.DataType == PointDataTypeU32 || fieldDetail.DataType == PointDataTypeS32 {
						size = 4
					}
					for i := 0; i+size < len(data); i += size {
						newSlice = reflect.Append(newSlice, reflect.ValueOf(cal(parseDataToFloat64(data[i:i+size], fieldDetail.DataType, fieldDetail.OrderType), fieldDetail.getCoefficient(), fieldDetail.DataType)+fieldDetail.Offset)) // TODO: only support float64 now
					}
				}

				// set the field value
				value.Set(newSlice)
			default:
				// TODO: other data types
				return fmt.Errorf("parse for %s not supported", value.Type().Kind())
			}
		}
		time.Sleep(1 * time.Millisecond)
	}
	return nil
}

// addrTodo is a map of address need to read
//
//	{
//		RegisterTypeCoil: {1: {}, 2: {}},
//		RegisterTypeHoldingRegister: {1: {}, 10: {}},
//	}
type addrTodo map[RegisterType]map[uint16]struct{}

func initAddrTodo() addrTodo {
	m := make(addrTodo)
	for _, k := range RegisterTypeList {
		m[k] = make(map[uint16]struct{})
	}
	return m
}

func (m *Modbus) getValuesBlock(ctx context.Context, v any, filter ...string) error {
	// Get the address blocks
	addrMap := initAddrTodo()
	filterMap := parseFilter(filter)
	err := m.collectAddresses(ctx, v, addrMap, filterMap)
	if err != nil {
		return errors.Wrap(err, "collectAddresses failed")
	}
	if len(addrMap) == 0 {
		return fmt.Errorf("no address found")
	}

	// each register type
	for _, k := range RegisterTypeList {
		if len(addrMap[k]) == 0 {
			continue
		}
		// Convert the map to block list
		blocks := m.addrMapToBlocks(addrMap[k])
		// Read the blocks
		if e := m.readBlocks(ctx, blocks, k); e != nil {
			return errors.Wrap(e, "readBlocks failed")
		}
		// Set the values
		if e := m.setAddressValues(ctx, v, blocks, k, filterMap); e != nil {
			return errors.Wrap(e, "setAddressValues failed")
		}
	}
	return nil
}

func (m *Modbus) addrMapToBlocks(addrMap map[uint16]struct{}) blocks {
	// Convert the map to a slice of addresses
	addrs := make([]uint16, 0, len(addrMap))
	for addr := range addrMap {
		addrs = append(addrs, addr)
	}

	// Sort the addresses
	sort.Slice(addrs, func(i, j int) bool { return addrs[i] < addrs[j] })

	// Group continuous addresses into blocks, merge blocks with small gaps
	bs := make(blocks)
	start := addrs[0]
	end := addrs[0]
	for i := 1; i < len(addrs); i++ {
		if addrs[i] == end+1 && addrs[i]-start+1 <= m.maxBlockSize {
			end = addrs[i]
		} else if addrs[i]-end <= m.maxGapInBlock && addrs[i]-start+1 <= m.maxBlockSize {
			end = addrs[i]
		} else {
			bs[start] = &block{start: start, end: end}
			start = addrs[i]
			end = addrs[i]
		}
	}
	bs[start] = &block{start: start, end: end}

	return bs
}

func (m *Modbus) collectAddresses(ctx context.Context, v any, addrMap addrTodo, filterMap map[string]bool) error {
	// validate v
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Ptr {
		return fmt.Errorf("not support for %s", val.Kind().String())
	}

	valueElem := val.Elem()
	typeElem := reflect.TypeOf(v).Elem()

	if valueElem.Kind() != reflect.Struct {
		return fmt.Errorf("not support for %s pointer", valueElem.Kind().String())
	}

	// filter
	needFilter := len(filterMap) != 0

	for i := 0; i < valueElem.NumField(); i++ {
		value := valueElem.Field(i)
		if value.Kind() == reflect.Struct {
			// dive
			if !value.CanAddr() {
				continue
			}
			addr := value.Addr()
			if !addr.IsValid() || !addr.CanInterface() {
				continue
			}
			if e := m.collectAddresses(ctx, addr.Interface(), addrMap, filterMap); e != nil {
				return e
			}
			continue
		}
		exist, fieldName := getPointTag(typeElem.Field(i))
		if !exist {
			continue
		}
		if needFilter && !filterMap[fieldName] {
			continue
		}
		fieldDetail, ok := m.points[fieldName]
		if !ok {
			continue
		}
		for j := uint16(0); j < fieldDetail.getQuantity(); j++ {
			addrMap[fieldDetail.RegisterType][fieldDetail.Addr+j] = struct{}{}
		}

	}
	return nil
}

func (m *Modbus) readBlocks(ctx context.Context, blocks blocks, registerType RegisterType) error {
	// Get a connection
	conn, err := m.connPool.Get()
	if err != nil {
		return fmt.Errorf("conn slave failed: %w", err)
	}
	defer m.Put(ctx, conn)
	// Read each block
	for _, block := range blocks {
		data, err := m.readData(ctx, conn, uint16(block.start), uint16(block.end-block.start+1), registerType)
		if err != nil {
			return err
		}
		switch registerType {
		case RegisterTypeHoldingRegister, RegisterTypeDefault, RegisterTypeInputRegister:
			if len(data) != int(block.end-block.start+1)*2 {
				return fmt.Errorf("read hoding/input block failed, want %d, got %d (register from %d to %d)", (block.end-block.start+1)*2, len(data), block.start, block.end)
			}
		case RegisterTypeCoil, RegisterTypeDiscreteInput:
			if len(data)*16 < int(block.end-block.start+1)*2 {
				return fmt.Errorf("read coil/discrete block failed, want %d, got %d (register from %d to %d)", (block.end-block.start+1)*2, len(data)*16, block.start, block.end)
			}
		}
		block.values = data
		// avoid device overload
		time.Sleep(1 * time.Millisecond)
	}
	return nil
}

func (m *Modbus) setAddressValues(ctx context.Context, v any, values blocks, registerType RegisterType, filterMap map[string]bool) error {
	needFilter := len(filterMap) != 0
	valueElem := reflect.ValueOf(v).Elem()
	typeElem := reflect.TypeOf(v).Elem()

	for i := 0; i < valueElem.NumField(); i++ {
		value := valueElem.Field(i)
		if value.Kind() == reflect.Struct {
			// dive
			if !value.CanAddr() {
				continue
			}
			addr := value.Addr()
			if !addr.IsValid() || !addr.CanInterface() {
				continue
			}
			if e := m.setAddressValues(ctx, addr.Interface(), values, registerType, filterMap); e != nil {
				return e
			}
			continue
		}
		exist, fieldName := getPointTag(typeElem.Field(i))
		if !exist {
			continue
		}
		if needFilter && !filterMap[fieldName] {
			continue
		}
		fieldDetail, ok := m.points[fieldName]
		if !ok || fieldDetail.RegisterType != registerType {
			continue
		}
		// find data
		if registerType == RegisterTypeCoil || registerType == RegisterTypeDiscreteInput {
			data := m.getFieldDataBit(values, fieldDetail.Addr, registerType)
			// set value
			switch value.Type().Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				value.SetInt(int64(data))
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				value.SetUint(uint64(data))
			case reflect.Float32, reflect.Float64:
				value.SetFloat(float64(data))
			case reflect.Bool:
				value.SetBool(data != 0)
			case reflect.Pointer:
				// get the type pointed to by the pointer
				ptrType := value.Type().Elem()
				newValue := reflect.New(ptrType)
				// create a new instance based on the type
				switch ptrType.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					newValue.Elem().SetInt(int64(data))
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					newValue.Elem().SetUint(uint64(data))
				case reflect.Float32, reflect.Float64:
					newValue.Elem().SetFloat(float64(data))
				case reflect.Bool:
					newValue.Elem().SetBool(data != 0)
				default:
					return fmt.Errorf("register type coils/discrete input not support for %s pointer, point = %s", value.Type().Kind(), fieldName)
				}
				// set the new pointer to the field
				value.Set(newValue)
			default:
				return fmt.Errorf("register type coils/discrete input not support for %s, point = %s", value.Type().Kind(), fieldName)
			}
		} else {
			data := m.getFieldData([]byte{}, values, fieldDetail.Addr, fieldDetail.getQuantity())

			// set value
			dataFloat64 := cal(parseDataToFloat64(data, fieldDetail.DataType, fieldDetail.OrderType), fieldDetail.getCoefficient(), fieldDetail.DataType) + fieldDetail.Offset
			switch value.Type().Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				value.SetInt(int64(dataFloat64))
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				value.SetUint(uint64(dataFloat64))
			case reflect.Float32, reflect.Float64:
				value.SetFloat(dataFloat64)
			case reflect.String:
				value.SetString(byte2String(data, fieldDetail.OrderType))
			case reflect.Pointer:
				// get the type pointed to by the pointer
				ptrType := value.Type().Elem()
				newValue := reflect.New(ptrType)
				// create a new instance based on the type
				switch ptrType.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					newValue.Elem().SetInt(int64(dataFloat64))
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					newValue.Elem().SetUint(uint64(dataFloat64))
				case reflect.Float32, reflect.Float64:
					newValue.Elem().SetFloat(dataFloat64)
				case reflect.String:
					newValue.SetString(byte2String(data, fieldDetail.OrderType))
				default:
					return fmt.Errorf("parse for %s pointer not supported", value.Type().Kind())
				}
				// set the new pointer to the field
				value.Set(newValue)
			case reflect.Slice, reflect.Array:
				// create a new slice
				elemType := value.Type().Elem()
				newSlice := reflect.MakeSlice(reflect.SliceOf(elemType), 0, 0)

				// add elements to the slice
				if value.Type().Name() == OriginByteName { // need original bytes
					for _, b := range data {
						newSlice = reflect.Append(newSlice, reflect.ValueOf(b))
					}
				} else {
					size := 2
					if fieldDetail.DataType == PointDataTypeU32 || fieldDetail.DataType == PointDataTypeS32 {
						size = 4
					}
					for i := 0; i+size <= len(data); i += size {
						val := reflect.ValueOf(cal(parseDataToFloat64(data[i:i+size], fieldDetail.DataType, fieldDetail.OrderType), fieldDetail.getCoefficient(), fieldDetail.DataType) + fieldDetail.Offset)
						if elemType.Kind() == reflect.Float64 {
							newSlice = reflect.Append(newSlice, val)
						} else {
							newSlice = reflect.Append(newSlice, val.Convert(elemType))
						}
					}
				}

				// set the field value
				value.Set(newSlice)
			default:
				return fmt.Errorf("parse for %s not supported, point = %s", value.Type().Kind(), fieldName)
			}
		}
	}
	return nil
}

func (m *Modbus) getFieldData(data []byte, values blocks, addr uint16, quantity uint16) []byte {
	for start, block := range values {
		if start <= addr && addr <= block.end {
			if addr+quantity-1 <= block.end {
				data = append(data, block.values[(addr-start)*2:(addr-start)*2+quantity*2]...)
				return data
			} else {
				data = append(data, block.values[(addr-start)*2:]...)
				return m.getFieldData(data, values, block.end+1, quantity-(block.end-addr+1))
			}
		}
	}
	return data
}

func (m *Modbus) getFieldDataBit(values blocks, addr uint16, registerType RegisterType) int8 {
	for start, block := range values {
		if start <= addr && addr <= block.end {
			if registerType == RegisterTypeCoil {
				var b byte
				if len(block.values) < int(addr-start)+1 {
					b = block.values[(addr-start)/8] >> ((addr - start) % 8)
				} else {
					b = block.values[addr-start]
				}
				return int8(b & 1)
			} else {
				b := block.values[(addr-start)/8] >> ((addr - start) % 8)
				return int8(b & 1)
			}
		}
	}
	return 0
}

// readData allow to read quantity larger than maxQuantity
func (m *Modbus) readData(ctx context.Context, conn Client, addr uint16, quantity uint16, registerType RegisterType) (results []byte, err error) {
	if quantity <= m.maxQuantity {
		return m.readDataByType(ctx, conn, addr, quantity, registerType)
	}
	for quantity > 0 {
		// calculate the quantity of this request
		currentQuantity := min(quantity, m.maxQuantity)
		// send request
		data, err := m.readDataByType(ctx, conn, addr, currentQuantity, registerType)
		if err != nil {
			return nil, err
		}
		// append results to result slice
		results = append(results, data...)
		// update start address and remaining quantity
		addr += currentQuantity
		quantity -= currentQuantity
	}
	return results, nil
}

// readDataByType reads data (not allow to exceed maxQuantity)
func (m *Modbus) readDataByType(_ context.Context, conn Client, addr uint16, quantity uint16, registerType RegisterType) ([]byte, error) {
	switch registerType {
	case RegisterTypeHoldingRegister, RegisterTypeDefault:
		return conn.ReadHoldingRegisters(addr, quantity, m.slaveID)
	case RegisterTypeInputRegister:
		return conn.ReadInputRegisters(addr, quantity, m.slaveID)
	case RegisterTypeCoil:
		return conn.ReadCoils(addr, quantity, m.slaveID)
	case RegisterTypeDiscreteInput:
		return conn.ReadDiscreteInputs(addr, quantity, m.slaveID)
	default:
		return nil, fmt.Errorf("unsupported register type: %v", registerType)
	}
}

func (m Modbus) Put(ctx context.Context, conn Client) {
	if err := m.connPool.Put(conn); err != nil {
		// for log
	}
}

func min(a, b uint16) uint16 {
	if a < b {
		return a
	}
	return b
}

func parseFilter(fields []string) map[string]bool {
	m := map[string]bool{}
	for _, field := range fields {
		m[field] = true
	}
	return m
}

// cal calculate value based on coefficient and control precision to avoid floating point precision issues
//
// Necessity:
//  1. Precision control requirement: In Modbus scenarios, devices usually return integer values, which are converted
//     to actual values (such as 31.25) through coefficients (such as 0.1, 0.01, 0.001). Need to retain the corresponding
//     decimal places according to the coefficient, rather than directly using before*c which may produce infinite decimals
//     or precision errors.
//
// 2. Floating point precision issues: Direct use of before*c may produce precision errors, for example:
//   - 3125 * 0.01 = 31.250000000000004 (floating point precision error)
//   - Expected result: 31.25 (retain 2 decimal places)
//
// 3. Business requirements: Modbus data usually needs precise decimal places, cannot have extra decimal places or precision errors.
//
// How to avoid precision issues:
// 1. For coefficient >= 1: directly return before*c, no precision control (because point = 1/c < 1 will cause precision loss)
// 2. For coefficient < 1:
//   - Calculate point = 1/c (e.g., when c=0.01, point=100, means retain 2 decimal places)
//   - If point is an integer, use integer arithmetic: Round(result*pointInt) / pointInt
//   - If point is not an integer, use floating point arithmetic: Round(result*point) / point
//   - This can avoid precision accumulation errors in floating point division
//
// Parameters:
//   - before: original value (usually integer value read from Modbus device)
//   - c: coefficient (e.g., 0.1 means retain 1 decimal place, 0.01 means retain 2 decimal places, 0.001 means retain 3 decimal places)
//   - dataType: data type (currently unused, reserved for future extension)
//
// Return value:
//
//	calculated value, already retained corresponding decimal places according to coefficient, and avoided floating point precision issues
//
// Limitations:
//
//  1. When before is a decimal:
//     - When before itself is a decimal (such as 123.456), the function will still retain decimal places according to the coefficient
//     - For example: cal(123.456, 0.1, ...) returns 12.3 (retain 1 place), not 12.3456
//     - This is by design, conforms to Modbus business requirements (control precision according to coefficient)
//     - If business requirements need to retain more decimal places, this function may not be applicable
//
//  2. Precision accumulation issues:
//     - In some edge cases, precision errors may occur
//     - For example: cal(31.25, 0.01, ...) may return 0.30999999999999999778 instead of 0.3125
//     - This usually occurs when before is already a decimal and multiple precision controls are performed
//     - In Modbus scenarios, before should be an integer. If decimals appear, it may be an upstream data processing issue
//
//  3. Applicable scenarios:
//     - This function is designed for Modbus scenarios, where before is usually an integer (original value returned by device)
//     - If before may be a decimal, need to judge whether to use this function according to business requirements
//     - For scenarios that need to retain as much precision as possible, other implementation solutions may be needed
//
// Examples:
//
//	cal(3125, 0.01, PointDataTypeU16)  // returns 31.25 (retain 2 decimal places)
//	cal(1234, 0.1, PointDataTypeU16)  // returns 123.4 (retain 1 decimal place)
//	cal(100, 1.0, PointDataTypeU16)  // returns 100.0 (coefficient>=1, directly return)
//	cal(123.456, 0.1, PointDataTypeU16)  // returns 12.3 (retain 1 decimal place, note: not 12.3456)
func cal(before, c float64, _ PointDataType) float64 {
	// apply coefficient first
	result := before * c

	// if coefficient is 0 or >= 1, directly return result, no precision control
	// because when coefficient >= 1, point = 1/c < 1, will cause precision loss
	if c == 0 || c >= 1.0 {
		return result
	}

	// determine precision digits according to coefficient (only applicable when c < 1)
	// if c = 0.1, point = 10, retain 1 decimal place
	// if c = 0.01, point = 100, retain 2 decimal places
	// if c = 0.001, point = 1000, retain 3 decimal places
	point := 1.0 / c

	// to avoid floating point precision issues, use integer arithmetic
	// check if point is an integer (or close to integer, error < 1e-9)
	pointInt := int64(math.Round(point))
	// check pointInt > 0 first to avoid unnecessary math.Abs call
	if pointInt > 0 && math.Abs(point-float64(pointInt)) < 1e-9 {
		// point is an integer, use integer arithmetic to avoid precision issues
		return math.Round(result*float64(pointInt)) / float64(pointInt)
	}

	// for non-integer point, use floating point arithmetic
	// but apply coefficient first then round, to avoid precision accumulation
	return math.Round(result*point) / point
}
