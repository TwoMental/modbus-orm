package modbusorm

import (
	"errors"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/TwoMental/modbus"
)

type ModbusTCPPool struct {
	mutex       sync.Mutex
	connections chan Client
	connection  Client // if config.MaxOpenConns==1 is true, this is the only connection
	factory     func() (Client, error)
	closed      bool
	config      ModbusTCPPoolConfig
}

type ModbusTCPPoolConfig struct {
	// MaxOpenConns is the maximum number of open connections to the server.
	MaxOpenConns int
	// ConnMaxLifetime is the maximum time a connection can be used.
	ConnMaxLifetime time.Duration
}

type ModbusTCPClient struct {
	Client     modbus.Client
	Handler    *modbus.TCPClientHandler
	createTime time.Time
}

func (c *ModbusTCPClient) Connect() error {
	return c.Handler.Connect()
}

func (c *ModbusTCPClient) Close() error {
	return c.Handler.Close()
}

func (c *ModbusTCPClient) IsAlive() bool {
	_, err := c.Client.ReadHoldingRegisters(1, 1)
	if err != nil {
		if strings.Contains(err.Error(), "EOF") {
			return false
		} else if strings.Contains(err.Error(), "connection refused") {
			return false
		} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return false
		} else if errors.Is(err, syscall.EPIPE) {
			return false
		} else if strings.Contains(err.Error(), "response transaction id") && strings.Contains(err.Error(), "does not match request") {
			// if last request time-out, this request's transaction id will be wrong.
			return false
		}
	}
	return true
}

func (c *ModbusTCPClient) CreateTime() time.Time {
	return c.createTime
}

func (c *ModbusTCPClient) ReadCoils(address, quantity uint16, SlaveID ...byte) (results []byte, err error) {
	return c.Client.ReadCoils(address, quantity, SlaveID...)
}

func (c *ModbusTCPClient) ReadDiscreteInputs(address, quantity uint16, SlaveID ...byte) (results []byte, err error) {
	return c.Client.ReadDiscreteInputs(address, quantity, SlaveID...)
}

func (c *ModbusTCPClient) WriteSingleCoil(address, value uint16, SlaveID ...byte) (results []byte, err error) {
	return c.Client.WriteSingleCoil(address, value, SlaveID...)
}

func (c *ModbusTCPClient) WriteMultipleCoils(address, quantity uint16, value []byte, SlaveID ...byte) (results []byte, err error) {
	return c.Client.WriteMultipleCoils(address, quantity, value, SlaveID...)
}

func (c *ModbusTCPClient) ReadInputRegisters(address, quantity uint16, SlaveID ...byte) (results []byte, err error) {
	return c.Client.ReadInputRegisters(address, quantity, SlaveID...)
}

func (c *ModbusTCPClient) ReadHoldingRegisters(address, quantity uint16, SlaveID ...byte) (results []byte, err error) {
	return c.Client.ReadHoldingRegisters(address, quantity, SlaveID...)
}

func (c *ModbusTCPClient) WriteSingleRegister(address, value uint16, SlaveID ...byte) (results []byte, err error) {
	return c.Client.WriteSingleRegister(address, value, SlaveID...)
}

func (c *ModbusTCPClient) WriteMultipleRegisters(address, quantity uint16, value []byte, SlaveID ...byte) (results []byte, err error) {
	return c.Client.WriteMultipleRegisters(address, quantity, value, SlaveID...)
}

func (c *ModbusTCPClient) ReadWriteMultipleRegisters(readAddress, readQuantity, writeAddress, writeQuantity uint16, value []byte, SlaveID ...byte) (results []byte, err error) {
	return c.Client.ReadWriteMultipleRegisters(readAddress, readQuantity, writeAddress, writeQuantity, value, SlaveID...)
}

func (c *ModbusTCPClient) MaskWriteRegister(address, andMask, orMask uint16, SlaveID ...byte) (results []byte, err error) {
	return c.Client.MaskWriteRegister(address, andMask, orMask, SlaveID...)
}

func (c *ModbusTCPClient) ReadFIFOQueue(address uint16, SlaveID ...byte) (results []byte, err error) {
	return c.Client.ReadFIFOQueue(address, SlaveID...)
}

func NewModbusTCPPool(config ModbusTCPPoolConfig, factory func() (Client, error)) (ConnPool, error) {
	if factory == nil {
		return nil, ErrFactoryNil
	}
	if config.MaxOpenConns <= 0 {
		config.MaxOpenConns = 5
	}

	pool := &ModbusTCPPool{
		factory: factory,
		config:  config,
	}

	if config.MaxOpenConns == 1 {
		// if only one connection is allowed, create and maintain a single connection directly
		conn, err := factory()
		if err != nil {
			return nil, err
		}
		pool.connection = conn
	} else {
		// if multiple connections are allowed, create MaxOpenConns connections
		pool.connections = make(chan Client, config.MaxOpenConns)
		for i := 0; i < config.MaxOpenConns; i++ {
			conn, err := factory()
			if err != nil {
				return nil, err
			}
			pool.connections <- conn
		}
	}

	return pool, nil
}

// Get get a connection from pool
func (p *ModbusTCPPool) Get() (Client, error) {
	if p.closed {
		return nil, ErrPoolClosed
	}

	// if only one connection is allowed, return it directly
	if p.config.MaxOpenConns == 1 {
		return p.connection, nil
	}

	select {
	case conn := <-p.connections:
		return conn, nil
	default:
		// if no avaliable connction, new one
		return p.factory()
	}
}

// Put put the connection to the pool
func (p *ModbusTCPPool) Put(conn Client) error {
	if p.closed {
		return conn.Close()
	}

	// if only one connection is allowed, return directly
	if p.config.MaxOpenConns == 1 {
		// TODO: keep-alive check for the single connection
		return nil
	}

	if time.Since(conn.CreateTime()) > p.config.ConnMaxLifetime {
		// if connection is expired, close it
		return conn.Close()
	}
	if !conn.IsAlive() {
		// if connection is not alive, close it
		return conn.Close()
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	select {
	case p.connections <- conn:
		return nil
	default:
		// if the pool is full, close the connection
		return conn.Close()
	}
}

// Close close the pool
func (p *ModbusTCPPool) Close() error {
	if p.config.MaxOpenConns == 1 {
		return p.connection.Close()
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.closed {
		return ErrPoolClosed
	}

	p.closed = true

	for {
		select {
		case conn, ok := <-p.connections:
			if !ok {
				return nil
			}
			conn.Close()
		default:
			close(p.connections)
			return nil
		}
	}
}
