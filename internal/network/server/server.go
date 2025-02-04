package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/FinnTew/FincasKV/internal/config"
	"github.com/FinnTew/FincasKV/internal/database"
	"github.com/FinnTew/FincasKV/internal/network/conn"
	"github.com/FinnTew/FincasKV/internal/network/handler"
	"github.com/cloudwego/netpoll"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	Addr           string
	IdleTimeout    time.Duration
	MaxConnections int
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
}

type Server struct {
	cfg       *Config
	db        *database.FincasDB
	handler   *handler.Handler
	eventLoop netpoll.EventLoop

	conns  sync.Map
	connWg sync.WaitGroup

	stats *Stats

	ctx     context.Context
	cancel  context.CancelFunc
	closed  bool
	closeMu sync.RWMutex

	metricsTicker *time.Ticker
	metricsCancel context.CancelFunc
}

func New(db *database.FincasDB) (*Server, error) {
	var (
		addr           = ":8911"
		idleTimeout    = 5 * time.Second
		maxConnections = 1000
		readTimeout    = 10 * time.Second
		writeTimeout   = 10 * time.Second
	)

	if config.Get().Network.Addr != "" {
		addr = config.Get().Network.Addr
	}
	if config.Get().Network.IdleTimeout != 0 {
		idleTimeout = config.Get().Network.IdleTimeout * time.Second
	}
	if config.Get().Network.MaxConns != 0 {
		maxConnections = config.Get().Network.MaxConns
	}
	if config.Get().Network.ReadTimeout != 0 {
		readTimeout = config.Get().Network.ReadTimeout * time.Second
	}
	if config.Get().Network.WriteTimeout != 0 {
		writeTimeout = config.Get().Network.WriteTimeout * time.Second
	}

	cfg := &Config{
		Addr:           addr,
		IdleTimeout:    idleTimeout,
		MaxConnections: maxConnections,
		ReadTimeout:    readTimeout,
		WriteTimeout:   writeTimeout,
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &Server{
		cfg:     cfg,
		db:      db,
		handler: handler.New(db),
		stats:   &Stats{StartTime: time.Now()},
		ctx:     ctx,
		cancel:  cancel,
	}

	eventLoop, err := netpoll.NewEventLoop(
		func(ctx context.Context, conn netpoll.Connection) error {
			return s.handleConnection(ctx, conn)
		},
		netpoll.WithOnPrepare(func(connection netpoll.Connection) context.Context {
			return context.Background()
		}),
		netpoll.WithIdleTimeout(idleTimeout),
		netpoll.WithReadTimeout(readTimeout),
		netpoll.WithWriteTimeout(writeTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create netpoll eventLoop: %v", err)
	}

	s.eventLoop = eventLoop

	return s, nil
}

func (s *Server) Start() error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return fmt.Errorf("server is already closed")
	}
	s.closeMu.Unlock()

	s.startMetricsCollection()

	listener, err := netpoll.CreateListener("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("failed to create listener: %v", err)
	}

	log.Printf("listening on %s", s.cfg.Addr)
	if err := s.eventLoop.Serve(listener); err != nil {
		return fmt.Errorf("failed to start eventLoop: %v", err)
	}

	return nil
}

func (s *Server) Stop() error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return fmt.Errorf("server already closed")
	}
	s.closed = true
	s.closeMu.Unlock()

	s.cancel()

	if s.metricsCancel != nil {
		s.metricsCancel()
	}

	s.conns.Range(func(key, value interface{}) bool {
		if c, ok := value.(conn.Connection); ok {
			c.Close()
		}
		return true
	})

	s.connWg.Wait()

	return s.eventLoop.Shutdown(context.Background())
}

func (s *Server) handleConnection(ctx context.Context, c netpoll.Connection) error {
	if atomic.LoadInt64(&s.stats.ConnCount) >= int64(s.cfg.MaxConnections) {
		c.Close()
		return fmt.Errorf("max connections reached")
	}

	connection := conn.New(c)
	s.conns.Store(c, connection)
	s.stats.IncrConnCount()
	s.connWg.Add(1)

	defer func() {
		connection.Close()
		s.conns.Delete(c)
		s.stats.DecrConnCount()
		s.connWg.Done()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			start := time.Now()
			cmd, err := connection.ReadCommand()
			if err != nil {
				if errors.Is(err, netpoll.ErrConnClosed) {
					return nil
				}
				s.stats.IncrErrorCount()
				log.Printf("failed to read command: %v", err)
				continue
			}

			if err := s.handler.Handle(connection, cmd); err != nil {
				s.stats.IncrErrorCount()
				log.Printf("failed to handle command: %v", err)
			}

			s.stats.IncrCmdCount()
			if time.Since(start) > time.Millisecond*10 {
				s.stats.IncrSlowCount()
			}
		}
	}
}

func (s *Server) startMetricsCollection() {
	ctx, cancel := context.WithCancel(context.Background())
	s.metricsCancel = cancel

	s.metricsTicker = time.NewTicker(1 * time.Second)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.metricsTicker.C:
				s.collectMetrics()
			}
		}
	}()
}

func (s *Server) collectMetrics() {
	var totalReadBytes int64
	var totalWriteBytes int64

	s.conns.Range(func(key, value interface{}) bool {
		if c, ok := value.(conn.Connection); ok {
			stats := c.Stats()
			atomic.AddInt64(&totalReadBytes, stats.ReadBytes)
			atomic.AddInt64(&totalWriteBytes, stats.WriteBytes)
		}
		return true
	})

	atomic.StoreInt64(&s.stats.BytesReceived, totalReadBytes)
	atomic.StoreInt64(&s.stats.BytesSent, totalWriteBytes)

	//log.Printf("Metrics: connections=%d commands=%d errors=%d slow_queries=%d bytes_recv=%d bytes_sent=%d",
	//	atomic.LoadInt64(&s.stats.ConnCount),
	//	atomic.LoadInt64(&s.stats.CmdCount),
	//	atomic.LoadInt64(&s.stats.ErrorCount),
	//	atomic.LoadInt64(&s.stats.SlowCount),
	//	totalReadBytes,
	//	totalWriteBytes,
	//)
}
