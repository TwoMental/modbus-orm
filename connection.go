package modbusorm

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/TwoMental/modbus"
)

type Modbus struct {
	connType ConnType
	modbusTCP
	modbusRTU
	slaveID       uint8
	maxQuantity   uint16
	withBlock     bool
	maxBlockSize  uint16
	maxGapInBlock uint16
	timeout       time.Duration

	points Point

	connPool ConnPool
}

func newDefaultModbus() *Modbus {
	return &Modbus{
		slaveID:       1,
		maxQuantity:   125,
		timeout:       1 * time.Second,
		withBlock:     false,
		maxBlockSize:  100,
		maxGapInBlock: 50,
	}
}

// NewModbusTCP new TCP connection configuration
func NewModbusTCP(host string, port uint, points Point, opts ...ModbusOption) *Modbus {
	m := newDefaultModbus()
	m.connType = ConnTypeTCP
	m.points = points
	m.modbusTCP = modbusTCP{
		Host:            host,
		Port:            port,
		MaxOpenConns:    3,
		ConnMaxLifetime: 30 * time.Minute,
	}

	for _, opt := range opts {
		opt(m)
	}
	return m
}

// NewModbusRTU new RTU connection configuration
func NewModbusRTU(comAddr string, points Point, opts ...ModbusOption) *Modbus {
	m := newDefaultModbus()
	m.connType = ConnTypeRTU
	m.points = points
	m.modbusRTU = modbusRTU{
		ComAddr:  comAddr,
		BaudRate: 9600,
		DataBits: 8,
		StopBits: 1,
		Parity:   "N",
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Conn connect to modbus server
func (m *Modbus) Conn() error {
	if m.connType == ConnTypeTCP {
		if m.ReuseConn {
			return m.connTCPReuse()
		} else {
			return m.connTCP()
		}
	} else if m.connType == ConnTypeRTU {
		return m.connRTU()
	}
	return nil
}

func (m *Modbus) connTCP() error {
	addr := fmt.Sprintf("%s:%d", m.Host, m.Port)
	factory := func() (Client, error) {
		handler := modbus.NewTCPClientHandler(addr)
		handler.Timeout = m.timeout
		handler.IdleTimeout = 60 * time.Second
		handler.SlaveId = m.slaveID
		if e := handler.Connect(); e != nil {
			return nil, e
		}
		client := modbus.NewClient(handler)
		return &ModbusTCPClient{Client: client, Handler: handler, createTime: time.Now()}, nil
	}
	config := ModbusTCPPoolConfig{
		MaxOpenConns:    m.MaxOpenConns,
		ConnMaxLifetime: m.ConnMaxLifetime,
	}

	pool, err := NewModbusTCPPool(config, factory)
	if err != nil {
		return fmt.Errorf("failed to create TCP pool: %w", err)
	}
	m.connPool = pool
	return nil
}

func (m *Modbus) connTCPReuse() error {
	// if reuse connection, set max connections to 1
	m.MaxOpenConns = 1

	addr := fmt.Sprintf("%s:%d", m.Host, m.Port)
	old, ok := tcpPool.Load(addr)
	if ok {
		oldConn, _ := old.(*tcpConn)
		oldConn.slaves[m.slaveID] = struct{}{}
		m.connPool = oldConn.pool
		tcpPool.Store(addr, oldConn)
	} else {
		// connection pool
		factory := func() (Client, error) {
			handler := modbus.NewTCPClientHandler(addr)
			handler.Timeout = m.timeout
			handler.IdleTimeout = 60 * time.Second
			handler.SlaveId = m.slaveID
			if e := handler.Connect(); e != nil {
				return nil, e
			}
			client := modbus.NewClient(handler)
			return &ModbusTCPClient{Client: client, Handler: handler, createTime: time.Now()}, nil
		}
		config := ModbusTCPPoolConfig{
			MaxOpenConns:    m.MaxOpenConns,
			ConnMaxLifetime: m.ConnMaxLifetime,
		}

		pool, err := NewModbusTCPPool(config, factory)
		if err != nil {
			return fmt.Errorf("failed to create TCP pool: %w", err)
		}
		tcpPool.Store(addr, &tcpConn{pool: pool, slaves: map[byte]struct{}{m.slaveID: {}}})
		m.connPool = pool
	}
	return nil
}

var rtuPool sync.Map // map[ComAddr]*rtuConn
var tcpPool sync.Map // map[IP:Port]*tcpConn

func init() {
	rtuPool = sync.Map{}
	tcpPool = sync.Map{}
}

type tcpConn struct {
	pool   ConnPool
	slaves map[byte]struct{}
}

type rtuConn struct {
	h      *modbus.RTUClientHandler
	c      modbus.Client
	slaves map[byte]struct{}
}

func (m *Modbus) connRTU() error {
	var pool ConnPool
	var err error
	old, ok := rtuPool.Load(m.ComAddr)
	if ok {
		oldConn, _ := old.(*rtuConn)
		if oldConn.h.BaudRate != m.BaudRate || oldConn.h.DataBits != m.DataBits || oldConn.h.Parity != m.Parity || oldConn.h.StopBits != m.StopBits {
			return errors.New("the baud rate, data bits, parity or stop bits of the same com port must be the same")
		}
		pool, err = NewModbusRTUPool(&ModbusRTUClient{Client: oldConn.c, Handler: oldConn.h, createTime: time.Now()})
		if err != nil {
			return fmt.Errorf("failed to create RTU pool: %w", err)
		}
		oldConn.slaves[m.slaveID] = struct{}{}
		rtuPool.Store(m.ComAddr, oldConn)
	} else {
		handler := modbus.NewRTUClientHandler(m.ComAddr)
		handler.BaudRate = m.BaudRate
		handler.DataBits = m.DataBits
		handler.Parity = m.Parity
		handler.StopBits = m.StopBits
		handler.SlaveId = m.slaveID
		handler.Timeout = m.timeout
		if e := handler.Connect(); e != nil {
			return e
		}
		client := modbus.NewClient(handler)

		pool, err = NewModbusRTUPool(&ModbusRTUClient{Client: client, Handler: handler, createTime: time.Now()})
		if err != nil {
			return fmt.Errorf("failed to create RTU pool: %w", err)
		}
		rtuPool.Store(m.ComAddr, &rtuConn{h: handler, c: client, slaves: map[byte]struct{}{m.slaveID: {}}})
	}

	m.connPool = pool
	return nil

}

func (m *Modbus) Close() error {
	if m.connType == ConnTypeTCP {
		return m.CloseTCP()
	} else if m.connType == ConnTypeRTU {
		return m.CloseRTU()
	}
	return nil
}

func (m *Modbus) CloseTCP() error {
	if m.ReuseConn {
		old, ok := tcpPool.Load(fmt.Sprintf("%s:%d", m.Host, m.Port))
		if !ok {
			return errors.New("modbus handler not found")
		}
		delete(old.(*tcpConn).slaves, m.slaveID)
		if len(old.(*tcpConn).slaves) == 0 {
			if err := m.connPool.Close(); err != nil {
				return err
			}
			tcpPool.Delete(fmt.Sprintf("%s:%d", m.Host, m.Port))
			return nil
		}
		return nil
	} else {
		if err := m.connPool.Close(); err != nil {
			return err
		}
		return nil
	}
}

func (m *Modbus) CloseRTU() error {
	old, ok := rtuPool.Load(m.ComAddr)
	if !ok {
		return errors.New("modbus handler not found")
	}
	delete(old.(*rtuConn).slaves, m.slaveID)
	if len(old.(*rtuConn).slaves) == 0 {
		// if no other connection needs this serial port
		if err := m.connPool.Close(); err != nil {
			return err
		}
		rtuPool.Delete(m.ComAddr)
		return nil
	} else {
		// if there are other connections that need this serial port
		rtuPool.Store(m.ComAddr, old)
		return nil
	}
}
