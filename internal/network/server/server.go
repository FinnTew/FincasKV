package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/FinnTew/FincasKV/internal/cluster/command"
	"github.com/FinnTew/FincasKV/internal/cluster/node"
	"github.com/FinnTew/FincasKV/internal/config"
	"github.com/FinnTew/FincasKV/internal/database"
	"github.com/FinnTew/FincasKV/internal/network/conn"
	"github.com/FinnTew/FincasKV/internal/network/handler"
	"github.com/cloudwego/netpoll"
	"log"
	"strings"
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

	node *node.Node
}

func New(db *database.FincasDB, address *string) (*Server, error) {
	var (
		addr           = ":8911"
		idleTimeout    = 5 * time.Second
		maxConnections = 1000
		readTimeout    = 10 * time.Second
		writeTimeout   = 10 * time.Second
	)

	if config.Get().Network.Addr != "" && *address == "" {
		addr = config.Get().Network.Addr
	} else if *address != "" {
		addr = *address
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

	if s.node != nil {
		if err := s.node.Shutdown(); err != nil {
			log.Printf("failed to shutdown node: %v", err)
		}
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

			// 处理 cluster 命令
			if strings.ToUpper(cmd.Name) == "CLUSTER" {
				if err := s.handleClusterCommand(connection, cmd); err != nil {
					log.Printf("failed to handle cluster command: %v", err)
				}
				continue
			}

			// 禁止非Leader节点处理写操作
			cmdP, ok := isWriteCommand(cmd.Name)
			if ok && s.node != nil && !s.node.IsLeader() {
				leaderAddr := s.node
				return connection.WriteError(fmt.Errorf("redirect to leader: %s", leaderAddr))
			}

			if err := s.handler.Handle(connection, cmd); err != nil {
				s.stats.IncrErrorCount()
				log.Printf("failed to handle command: %v", err)
			} else if s.node != nil {
				err := s.node.Apply(command.New(cmdP.CmdType, cmdP.Method, cmd.Args))
				if err != nil {
					return fmt.Errorf("failed to apply command: %v", err)
				}
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

func (s *Server) initCluster(conf *node.Config) error {
	n, err := node.New(s.db, conf)
	if err != nil {
		return fmt.Errorf("failed to create node: %v", err)
	}

	s.node = n
	return nil
}

type cmdPair struct {
	CmdType command.CmdTyp
	Method  command.MethodTyp
}

func isWriteCommand(cmd string) (cmdPair, bool) {
	wCmds := map[string]cmdPair{
		"SET": {command.CmdString, command.MethodSet}, "DEL": {command.CmdString, command.MethodDel}, "INCR": {command.CmdString, command.MethodIncr}, "INCRBY": {command.CmdString, command.MethodIncrBy},
		"DECR": {command.CmdString, command.MethodDecr}, "DECRBY": {command.CmdString, command.MethodDecrBy}, "APPEND": {command.CmdString, command.MethodAppend}, "GETSET": {command.CmdString, command.MethodGetSet},
		"SETNX": {command.CmdString, command.MethodSetNX}, "MSET": {command.CmdString, command.MethodMSet},
		"HSET": {command.CmdHash, command.MethodHSet}, "HMSET": {command.CmdHash, command.MethodHMSet}, "HDEL": {command.CmdHash, command.MethodHDel}, "HINCRBY": {command.CmdHash, command.MethodHIncrBy},
		"HINCRBYFLOAT": {command.CmdHash, command.MethodHIncrByFloat}, "HSETNX": {command.CmdHash, command.MethodHSetNX},
		"LPUSH": {command.CmdList, command.MethodLPush}, "RPUSH": {command.CmdList, command.MethodRPush}, "LPOP": {command.CmdList, command.MethodLPop}, "RPOP": {command.CmdList, command.MethodRPop},
		"LTRIM": {command.CmdList, command.MethodLTrim}, "LINSERT": {command.CmdList, command.MethodLInsert},
		"SADD": {command.CmdSet, command.MethodSAdd}, "SREM": {command.CmdSet, command.MethodSRem}, "SPOP": {command.CmdSet, command.MethodSPop}, "SMOVE": {command.CmdSet, command.MethodSMove},
		"ZADD": {command.CmdZSet, command.MethodZAdd}, "ZREM": {command.CmdZSet, command.MethodZRem}, "ZINCRBY": {command.CmdZSet, command.MethodZIncrBy},
		"ZREMRANGEBYRANK": {command.CmdZSet, command.MethodZRemRangeByRank}, "ZREMRANGEBYSCORE": {command.CmdZSet, command.MethodZRemRangeByScore},
	}
	val, ok := wCmds[strings.ToUpper(cmd)]
	return val, ok
	//writeCommands := map[string]bool{
	//	"SET": true, "DEL": true, "INCR": true, "INCRBY": true,
	//	"DECR": true, "DECRBY": true, "APPEND": true, "GETSET": true,
	//	"SETNX": true, "MSET": true,
	//	"HSET": true, "HMSET": true, "HDEL": true, "HINCRBY": true,
	//	"HINCRBYFLOAT": true, "HSETNX": true,
	//	"LPUSH": true, "RPUSH": true, "LPOP": true, "RPOP": true,
	//	"LTRIM": true, "LINSERT": true,
	//	"SADD": true, "SREM": true, "SPOP": true, "SMOVE": true,
	//	"ZADD": true, "ZREM": true, "ZINCRBY": true,
	//	"ZREMRANGEBYRANK": true, "ZREMRANGEBYSCORE": true,
	//}
	//return writeCommands[strings.ToUpper(cmd)]
}
