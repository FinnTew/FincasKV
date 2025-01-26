package util

import (
	"math"
	"strconv"
	"testing"
)

func TestBloomFilter(t *testing.T) {
	tests := []struct {
		name        string
		n           uint64
		p           float64
		wantErr     bool
		testActions func(*testing.T, *BloomFilter)
	}{
		{
			name:    "invalid zero elements",
			n:       0,
			p:       0.01,
			wantErr: true,
		},
		{
			name:    "invalid probability=1",
			n:       1000,
			p:       1.0,
			wantErr: true,
		},
		{
			name:    "basic functionality",
			n:       1000,
			p:       0.01,
			wantErr: false,
			testActions: func(t *testing.T, bf *BloomFilter) {
				for i := 0; i < 1000; i++ {
					data := []byte("key" + strconv.Itoa(i))
					bf.Add(data)
					if !bf.Contains(data) {
						t.Errorf("元素 %d 未找到", i)
					}
				}

				falsePositives := 0
				totalTests := 1000
				for i := 1000; i < 1000+totalTests; i++ {
					data := []byte("key" + strconv.Itoa(i))
					if bf.Contains(data) {
						falsePositives++
					}
				}

				fpRate := float64(falsePositives) / float64(totalTests)
				if fpRate > 0.02 { // 允许2%误差
					t.Errorf("误判率过高: %.4f > 0.02", fpRate)
				}
			},
		},
		{
			name:    "serialization roundtrip",
			n:       1000,
			p:       0.01,
			wantErr: false,
			testActions: func(t *testing.T, bf *BloomFilter) {
				for i := 0; i < 1000; i++ {
					bf.Add([]byte("data" + strconv.Itoa(i)))
				}

				data, err := bf.MarshalBinary()
				if err != nil {
					t.Fatal("序列化失败:", err)
				}

				newBF := &BloomFilter{}
				if err := newBF.UnmarshalBinary(data); err != nil {
					t.Fatal("反序列化失败:", err)
				}

				for i := 0; i < 1000; i++ {
					key := []byte("data" + strconv.Itoa(i))
					if !newBF.Contains(key) {
						t.Errorf("反序列化后元素 %d 未找到", i)
					}
				}

				origRatio := bf.EstimatedFillRatio()
				newRatio := newBF.EstimatedFillRatio()
				if math.Abs(origRatio-newRatio) > 0.0001 {
					t.Errorf("填充率不一致 原始: %.4f 新: %.4f", origRatio, newRatio)
				}
			},
		},
		{
			name:    "reset functionality",
			n:       1000,
			p:       0.01,
			wantErr: false,
			testActions: func(t *testing.T, bf *BloomFilter) {
				bf.Add([]byte("test"))
				if !bf.Contains([]byte("test")) {
					t.Error("重置前元素未找到")
				}

				bf.Reset()

				if bf.Contains([]byte("test")) {
					t.Error("重置后元素不应存在")
				}

				if ratio := bf.EstimatedFillRatio(); ratio > 0 {
					t.Errorf("重置后填充率应为0，实际为 %.4f", ratio)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bf, err := NewBloomFilter(tt.n, tt.p)
			if (err != nil) != tt.wantErr {
				t.Fatalf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			if tt.testActions != nil {
				tt.testActions(t, bf)
			}
		})
	}
}
