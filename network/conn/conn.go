package conn

import (
	"context"
	"github.com/FinnTew/FincasKV/network/protocol"
	"github.com/cloudwego/netpoll"
	"sync"
	"time"
)

type Stats struct {
	Created    time.Time
	LastActive time.Time
	ReadBytes  int64
	WriteBytes int64
	ReadCmds   int64
	WriteCmds  int64
	Errors     int64
}

type Connection struct {
	conn   netpoll.Connection
	parser *protocol.Parser
	writer *protocol.Writer
	stats  *Stats
	ctx    context.Context
	cancel context.CancelFunc
	closed bool
	mu     sync.RWMutex
}

func New(conn netpoll.Connection) *Connection {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Connection{
		conn:   conn,
		parser: protocol.NewParser(conn),
		writer: protocol.NewWriter(conn),
		stats: &Stats{
			Created:    time.Now(),
			LastActive: time.Now(),
		},
		ctx:    ctx,
		cancel: cancel,
	}

	return c
}

func (c *Connection) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return *c.stats
}

func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	c.cancel()
	return c.conn.Close()
}

func (c *Connection) IsClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

func (c *Connection) ReadCommand() (*protocol.Command, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmd, err := c.parser.Parse()
	if err != nil {
		c.stats.Errors++
		return nil, err
	}

	c.stats.ReadCmds++
	c.stats.LastActive = time.Now()
	return cmd, nil
}

func (c *Connection) WriteString(s string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.writer.WriteString(s)
	if err != nil {
		c.stats.Errors++
		return err
	}

	c.stats.WriteCmds++
	c.stats.LastActive = time.Now()
	return nil
}

func (c *Connection) WriteError(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	werr := c.writer.WriteError(err)
	if werr != nil {
		c.stats.Errors++
		return werr
	}

	c.stats.Errors++
	c.stats.LastActive = time.Now()
	return nil
}

func (c *Connection) WriteInteger(n int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.writer.WriteInteger(n)
	if err != nil {
		c.stats.Errors++
		return err
	}

	c.stats.WriteCmds++
	c.stats.LastActive = time.Now()
	return nil
}

func (c *Connection) WriteBulk(b []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.writer.WriteBulk(b)
	if err != nil {
		c.stats.Errors++
		return err
	}

	c.stats.WriteCmds++
	c.stats.LastActive = time.Now()
	return nil
}

func (c *Connection) WriteArray(arr [][]byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.writer.WriteArray(arr)
	if err != nil {
		c.stats.Errors++
		return err
	}

	c.stats.WriteCmds++
	c.stats.LastActive = time.Now()
	return nil
}
