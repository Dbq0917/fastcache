package fastcache

import (
	"fmt"
	"testing"
)

const (
	benchMixedSmallValueSize = 4 * 1024
	benchMixedBigValueSize   = 256 * 1024
	benchMixedSmallCount     = 4096
	benchMixedBigCount       = 128
)

func BenchmarkCacheMixedGet(b *testing.B) {
	c, smallKeys, bigKeys := newMixedBenchCache()
	defer c.Reset()

	b.Run("Small", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(benchMixedSmallValueSize)
		var dst []byte
		for i := 0; i < b.N; i++ {
			dst = c.Get(dst[:0], smallKeys[i%len(smallKeys)])
			if len(dst) != benchMixedSmallValueSize {
				b.Fatalf("unexpected small value length; got %d; want %d", len(dst), benchMixedSmallValueSize)
			}
		}
	})

	b.Run("Big", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(benchMixedBigValueSize)
		var dst []byte
		for i := 0; i < b.N; i++ {
			dst = c.GetBig(dst[:0], bigKeys[i%len(bigKeys)])
			if len(dst) != benchMixedBigValueSize {
				b.Fatalf("unexpected big value length; got %d; want %d", len(dst), benchMixedBigValueSize)
			}
		}
	})
}

func newMixedBenchCache() (*Cache, [][]byte, [][]byte) {
	c := New(512 << 20)
	smallKeys := make([][]byte, benchMixedSmallCount)
	for i := range smallKeys {
		k := []byte(fmt.Sprintf("small-%08d", i))
		v := make([]byte, benchMixedSmallValueSize)
		for j := range v {
			v[j] = byte(i + j)
		}
		c.Set(k, v)
		smallKeys[i] = k
	}
	bigKeys := make([][]byte, benchMixedBigCount)
	for i := range bigKeys {
		k := []byte(fmt.Sprintf("big-%08d", i))
		v := make([]byte, benchMixedBigValueSize)
		for j := range v {
			v[j] = byte(i + j)
		}
		c.SetBig(k, v)
		bigKeys[i] = k
	}
	return c, smallKeys, bigKeys
}
