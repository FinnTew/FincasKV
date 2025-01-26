package util

import (
	"bytes"
	"encoding/gob"
	"errors"
	"hash"
	"hash/crc64"
	"hash/fnv"
	"math"
	"sync"
)

type BloomFilter struct {
	bits   []byte
	m      uint64
	k      uint
	seeds  []uint64
	mu     sync.RWMutex
	hasher hash.Hash64
}

func optimalM(n uint64, p float64) uint64 {
	m := -float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)
	return uint64(math.Ceil(m))
}

func optimalK(m, n uint64) uint {
	k := math.Ceil((float64(m) / float64(n)) * math.Ln2)
	return uint(math.Max(1, k))
}

func NewBloomFilter(n uint64, p float64) (*BloomFilter, error) {
	if n == 0 {
		return nil, errors.New("n must be > 0")
	}
	if p <= 0 || p >= 1 {
		return nil, errors.New("p must be > 0 and < 1")
	}

	m := optimalM(n, p)
	k := optimalK(m, n)

	seeds := []uint64{0x8b3d35e3d94e5659, 0x7a47d13d11a51a9a}

	return &BloomFilter{
		bits:   make([]byte, m),
		m:      m,
		k:      k,
		seeds:  seeds,
		hasher: fnv.New64a(),
	}, nil
}

func (bf *BloomFilter) Add(item []byte) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	h1, h2 := bf.hashValues(item)
	for i := uint(0); i < bf.k; i++ {
		pos := (h1 + uint64(i)*h2) % bf.m
		bf.setBit(pos)
	}
}

func (bf *BloomFilter) Contains(item []byte) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	h1, h2 := bf.hashValues(item)
	for i := uint(0); i < bf.k; i++ {
		pos := (h1 + uint64(i)*h2) % bf.m
		if !bf.getBit(pos) {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) hashValues(item []byte) (uint64, uint64) {
	bf.hasher.Reset()
	bf.hasher.Write(item)
	h1 := bf.hasher.Sum64()

	crcTable := crc64.MakeTable(crc64.ECMA)
	h2 := crc64.Checksum(item, crcTable)

	h1 ^= bf.seeds[0]
	h2 ^= bf.seeds[1]

	return h1, h2
}

func (bf *BloomFilter) setBit(pos uint64) {
	idx := pos / 8
	bit := pos % 8
	bf.bits[idx] |= 1 << bit
}

func (bf *BloomFilter) getBit(pos uint64) bool {
	idx := pos / 8
	bit := pos % 8
	return (bf.bits[idx] & (1 << bit)) != 0
}

func (bf *BloomFilter) Reset() {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	for i := range bf.bits {
		bf.bits[i] = 0
	}
}

func (bf *BloomFilter) MarshalBinary() ([]byte, error) {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	if err := enc.Encode(bf.bits); err != nil {
		return nil, err
	}
	if err := enc.Encode(bf.m); err != nil {
		return nil, err
	}
	if err := enc.Encode(bf.k); err != nil {
		return nil, err
	}
	if err := enc.Encode(bf.seeds); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (bf *BloomFilter) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	if err := dec.Decode(&bf.bits); err != nil {
		return err
	}
	if err := dec.Decode(&bf.m); err != nil {
		return err
	}
	if err := dec.Decode(&bf.k); err != nil {
		return err
	}
	if err := dec.Decode(&bf.seeds); err != nil {
		return err
	}

	bf.hasher = fnv.New64a()
	return nil
}

func (bf *BloomFilter) EstimatedFillRatio() float64 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	setBits := 0
	for _, b := range bf.bits {
		setBits += bitsSetCount(b)
	}
	return float64(setBits) / float64(bf.m)
}

func bitsSetCount(b byte) int {
	count := 0
	for b > 0 {
		count += int(b & 1)
		b >>= 1
	}
	return count
}
