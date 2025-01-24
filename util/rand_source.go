package util

import (
	"crypto/rand"
	"encoding/binary"
	"math/bits"
	"sync"
	"time"
)

type SecureRandSource struct {
	lock sync.Mutex
	seed uint64
}

func (s *SecureRandSource) Seed(seed int64) {
	s.lock.Lock()
	s.seed = uint64(seed)
	s.lock.Unlock()
}

func (s *SecureRandSource) Int63() int64 {
	s.lock.Lock()
	defer s.lock.Unlock()

	// 使用 PCG 算法生成随机数
	oldState := s.seed
	s.seed = oldState*6364136223846793005 + 1

	xorshifted := uint32(((oldState >> 18) ^ oldState) >> 27)
	rot := uint32(oldState >> 59)

	return int64((xorshifted >> rot) | (xorshifted << ((-rot) & 31)))
}

func (s *SecureRandSource) Uint64() uint64 {
	return uint64(s.Int63())>>31 | uint64(s.Int63())<<32
}

func NewSecureRandSource() (*SecureRandSource, error) {
	var seed uint64

	entropy := make([]byte, 16)

	if _, err := rand.Read(entropy[:8]); err != nil {
		return nil, err
	}

	timeNano := uint64(time.Now().UnixNano())
	binary.LittleEndian.PutUint64(entropy[8:], timeNano)

	seed = binary.LittleEndian.Uint64(entropy[:8])
	seed ^= timeNano
	seed = bits.RotateLeft64(seed, 13) ^ binary.LittleEndian.Uint64(entropy[8:])

	return &SecureRandSource{seed: seed}, nil
}
