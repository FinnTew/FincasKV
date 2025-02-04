package server

import (
	"sync/atomic"
	"time"
)

type Stats struct {
	StartTime     time.Time
	ConnCount     int64
	CmdCount      int64
	BytesReceived int64
	BytesSent     int64
	ErrorCount    int64
	SlowCount     int64
}

func (s *Stats) IncrConnCount() {
	atomic.AddInt64(&s.ConnCount, 1)
}

func (s *Stats) DecrConnCount() {
	atomic.AddInt64(&s.ConnCount, -1)
}

func (s *Stats) IncrCmdCount() {
	atomic.AddInt64(&s.CmdCount, 1)
}

func (s *Stats) AddBytesReceived(n int64) {
	atomic.AddInt64(&s.BytesReceived, n)
}

func (s *Stats) AddBytesSent(n int64) {
	atomic.AddInt64(&s.BytesSent, n)
}

func (s *Stats) IncrErrorCount() {
	atomic.AddInt64(&s.ErrorCount, 1)
}

func (s *Stats) IncrSlowCount() {
	atomic.AddInt64(&s.SlowCount, 1)
}
