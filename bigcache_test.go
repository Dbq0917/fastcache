package fastcache

import (
	"bytes"
	"fmt"
	"testing"

	xxhash "github.com/cespare/xxhash/v2"
)

func TestSetGetBig(t *testing.T) {
	c := New(256 * 1024 * 1024)
	const valuesCount = 10
	for _, valueSize := range []int{1, 100, 1<<16 - 1, 1 << 16, 1<<16 + 1, 1 << 17, 1<<17 + 1, 1<<17 - 1, 1 << 19} {
		t.Run(fmt.Sprintf("valueSize_%d", valueSize), func(t *testing.T) {
			for seed := range 3 {
				testSetGetBig(t, c, valueSize, valuesCount, seed)
			}
		})
	}
}

func testSetGetBig(t *testing.T, c *Cache, valueSize, valuesCount, seed int) {
	m := make(map[string][]byte)
	var buf []byte
	for i := range valuesCount {
		key := []byte(fmt.Sprintf("key %d", i))
		value := createValue(valueSize, seed)
		c.SetBig(key, value)
		m[string(key)] = value
		buf = c.GetBig(buf[:0], key)
		if !bytes.Equal(buf, value) {
			t.Fatalf("seed=%d; unexpected value obtained for key=%q; got len(value)=%d; want len(value)=%d", seed, key, len(buf), len(value))
		}
	}
	var s Stats
	c.UpdateStats(&s)
	if s.SetBigCalls < uint64(valuesCount) {
		t.Fatalf("expecting SetBigCalls >= %d; got %d", valuesCount, s.SetBigCalls)
	}
	if s.GetBigCalls < uint64(valuesCount) {
		t.Fatalf("expecting GetBigCalls >= %d; got %d", valuesCount, s.GetBigCalls)
	}

	// Verify that values stil exist
	for key, value := range m {
		buf = c.GetBig(buf[:0], []byte(key))
		if !bytes.Equal(buf, value) {
			t.Fatalf("seed=%d; unexpected value obtained for key=%q; got len(value)=%d; want len(value)=%d", seed, key, len(buf), len(value))
		}
	}
}

func createValue(size, seed int) []byte {
	var buf []byte
	for i := range size {
		buf = append(buf, byte(i+seed))
	}
	return buf
}

func TestSetGetBigMixedWithSmall(t *testing.T) {
	for _, tc := range []struct {
		name         string
		maxBytes     int
		bigValueSize int
	}{
		{name: "inline", maxBytes: 512 << 20, bigValueSize: 256 << 10},
		{name: "segmented", maxBytes: 128 << 20, bigValueSize: 512 << 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := New(tc.maxBytes)
			defer c.Reset()

			smallValueSize := 3 * 1024
			smallKeys := make([][]byte, 256)
			smallValues := make([][]byte, len(smallKeys))
			bigKeys := make([][]byte, 8)
			bigValues := make([][]byte, len(bigKeys))

			for i := range smallKeys {
				smallKeys[i] = []byte(fmt.Sprintf("small-key-%03d", i))
				smallValues[i] = createValue(smallValueSize, i)
				if i < len(bigKeys) {
					bigKeys[i] = []byte(fmt.Sprintf("big-key-%03d", i))
					bigValues[i] = createValue(tc.bigValueSize, i)
				}
				c.Set(smallKeys[i], smallValues[i])
				if i < len(bigKeys) {
					c.SetBig(bigKeys[i], bigValues[i])
				}
			}

			var buf []byte
			for i := range smallKeys {
				buf = c.Get(buf[:0], smallKeys[i])
				if !bytes.Equal(buf, smallValues[i]) {
					t.Fatalf("unexpected small value for key %q; got len %d; want %d", smallKeys[i], len(buf), len(smallValues[i]))
				}
				if i < len(bigKeys) {
					buf = c.GetBig(buf[:0], bigKeys[i])
					if !bytes.Equal(buf, bigValues[i]) {
						t.Fatalf("unexpected big value for key %q; got len %d; want %d", bigKeys[i], len(buf), len(bigValues[i]))
					}
					if !c.Has(bigKeys[i]) {
						t.Fatalf("cannot find big entry for key %q", bigKeys[i])
					}
				}
			}
		})
	}
}

func TestSaveLoadBig(t *testing.T) {
	c := New(64 << 20)
	defer c.Reset()

	keys := [][]byte{
		[]byte("small"),
		[]byte("boundary"),
		[]byte("inline-ish"),
		[]byte("segmented"),
		[]byte("large-segmented"),
	}
	values := [][]byte{
		createValue(1024, 1),
		createValue(chunkSize-bigEntryHeaderLen-len("boundary"), 2),
		createValue(100*1024, 3),
		createValue(256*1024, 4),
		createValue(1024*1024, 5),
	}
	c.Set(keys[0], values[0])
	c.SetBig(keys[1], values[1])
	c.SetBig(keys[2], values[2])
	c.SetBig(keys[3], values[3])
	c.SetBig(keys[4], values[4])

	filePath := t.TempDir() + "/cache"
	if err := c.SaveToFile(filePath); err != nil {
		t.Fatalf("cannot save cache: %s", err)
	}
	c, err := LoadFromFile(filePath)
	if err != nil {
		t.Fatalf("cannot load cache: %s", err)
	}
	if !c.verifyBigHashes {
		t.Fatalf("loaded cache must verify big value hashes")
	}

	if got := c.Get(nil, keys[0]); !bytes.Equal(got, values[0]) {
		t.Fatalf("unexpected small value after loading; got len %d; want %d", len(got), len(values[0]))
	}
	for i := 1; i < len(keys); i++ {
		got := c.GetBig(nil, keys[i])
		if !bytes.Equal(got, values[i]) {
			t.Fatalf("unexpected big value for key %q after loading; got len %d; want %d", keys[i], len(got), len(values[i]))
		}
	}
}

func TestGetBigLegacyFormat(t *testing.T) {
	c := New(64 << 20)
	defer c.Reset()

	key := []byte("legacy-big")
	value := createValue(300*1024, 42)
	setBigLegacy(c, key, value)

	got := c.GetBig(nil, key)
	if !bytes.Equal(got, value) {
		t.Fatalf("unexpected legacy big value; got len %d; want %d", len(got), len(value))
	}
}

func TestSetGetBigMaxKeySegmented(t *testing.T) {
	c := New(128 << 20)
	defer c.Reset()

	key := createValue(maxKeyLen, 1)
	value := createValue(512*1024, 2)
	c.SetBig(key, value)

	got := c.GetBig(nil, key)
	if !bytes.Equal(got, value) {
		t.Fatalf("unexpected big value for max-length key; got len %d; want %d", len(got), len(value))
	}
	if !c.Has(key) {
		t.Fatalf("cannot find big entry for max-length key")
	}
}

func setBigLegacy(c *Cache, key, value []byte) {
	valueLen := len(value)
	valueHash := xxhash.Sum64(value)
	subkey := make([]byte, 16)
	for i := 0; len(value) > 0; i++ {
		n := len(value)
		if n > maxSubvalueLen {
			n = maxSubvalueLen
		}
		marshalUint64(subkey[:0], valueHash)
		marshalUint64(subkey[8:8], uint64(i))
		c.Set(subkey, value[:n])
		value = value[n:]
	}
	meta := make([]byte, 16)
	marshalUint64(meta[:0], valueHash)
	marshalUint64(meta[8:8], uint64(valueLen))
	c.Set(key, meta)
}
