package fastcache

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	xxhash "github.com/cespare/xxhash/v2"
)

func TestSaveLoadSmall(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "test")
	if err != nil {
		t.Fatal(err)
	}
	filePath := filepath.Join(tmpDir, "TestSaveLoadSmall.fastcache")
	defer os.RemoveAll(filePath)

	c := New(1)
	defer c.Reset()

	key := []byte("foobar")
	value := []byte("abcdef")
	c.Set(key, value)
	if err := c.SaveToFile(filePath); err != nil {
		t.Fatalf("SaveToFile error: %s", err)
	}

	c1, err := LoadFromFile(filePath)
	if err != nil {
		t.Fatalf("LoadFromFile error: %s", err)
	}
	vv := c1.Get(nil, key)
	if string(vv) != string(value) {
		t.Fatalf("unexpected value obtained from cache; got %q; want %q", vv, value)
	}

	// Verify that key can be overwritten.
	newValue := []byte("234fdfd")
	c1.Set(key, newValue)
	vv = c1.Get(nil, key)
	if string(vv) != string(newValue) {
		t.Fatalf("unexpected new value obtained from cache; got %q; want %q", vv, newValue)
	}
}

func TestLoadFileNotExist(t *testing.T) {
	c, err := LoadFromFile(`non-existing-file`)
	if err == nil {
		t.Fatalf("LoadFromFile must return error; got nil")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("LoadFromFile must return os.ErrNotExist; got: %s", err)
	}
	if c != nil {
		t.Fatalf("LoadFromFile must return nil cache")
	}
}

func TestSaveLoadFile(t *testing.T) {
	for _, concurrency := range []int{0, 1, 2, 4, 10} {
		t.Run(fmt.Sprintf("concurrency_%d", concurrency), func(t *testing.T) {
			testSaveLoadFile(t, concurrency)
		})
	}
}

func testSaveLoadFile(t *testing.T, concurrency int) {
	var s Stats
	tmpDir, err := ioutil.TempDir("", "test")
	if err != nil {
		t.Fatal(err)
	}
	filePath := filepath.Join(tmpDir, fmt.Sprintf("TestSaveLoadFile.%d.fastcache", concurrency))
	defer os.RemoveAll(filePath)

	const itemsCount = 10000
	const maxBytes = bucketsCount * chunkSize * 2
	c := New(maxBytes)
	for i := range itemsCount {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		c.Set(k, v)
		vv := c.Get(nil, k)
		if string(v) != string(vv) {
			t.Fatalf("unexpected cache value for k=%q; got %q; want %q; bucket[0]=%#v", k, vv, v, &c.buckets[0])
		}
	}
	if concurrency == 1 {
		if err := c.SaveToFile(filePath); err != nil {
			t.Fatalf("SaveToFile error: %s", err)
		}
	} else {
		if err := c.SaveToFileConcurrent(filePath, concurrency); err != nil {
			t.Fatalf("SaveToFileConcurrent(%d) error: %s", concurrency, err)
		}
	}
	s.Reset()
	c.UpdateStats(&s)
	if s.EntriesCount != itemsCount {
		t.Fatalf("unexpected entriesCount; got %d; want %d", s.EntriesCount, itemsCount)
	}
	c.Reset()

	// Verify LoadFromFile
	c, err = LoadFromFile(filePath)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	s.Reset()
	c.UpdateStats(&s)
	if s.EntriesCount != itemsCount {
		t.Fatalf("unexpected entriesCount; got %d; want %d", s.EntriesCount, itemsCount)
	}
	for i := range itemsCount {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		vv := c.Get(nil, k)
		if string(v) != string(vv) {
			t.Fatalf("unexpected cache value for k=%q; got %q; want %q; bucket[0]=%#v", k, vv, v, &c.buckets[0])
		}
	}
	c.Reset()

	// Verify LoadFromFileOrNew
	c = LoadFromFileOrNew(filePath, maxBytes)
	s.Reset()
	c.UpdateStats(&s)
	if s.EntriesCount != itemsCount {
		t.Fatalf("unexpected entriesCount; got %d; want %d", s.EntriesCount, itemsCount)
	}
	for i := range itemsCount {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		vv := c.Get(nil, k)
		if string(v) != string(vv) {
			t.Fatalf("unexpected cache value for k=%q; got %q; want %q; bucket[0]=%#v", k, vv, v, &c.buckets[0])
		}
	}
	c.Reset()

	// Overwrite existing keys
	for i := range itemsCount {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		c.Set(k, v)
		vv := c.Get(nil, k)
		if string(v) != string(vv) {
			t.Fatalf("unexpected cache value for k=%q; got %q; want %q; bucket[0]=%#v", k, vv, v, &c.buckets[0])
		}
	}

	// Add new keys
	for i := range itemsCount {
		k := []byte(fmt.Sprintf("new key %d", i))
		v := []byte(fmt.Sprintf("new value %d", i))
		c.Set(k, v)
		vv := c.Get(nil, k)
		if string(v) != string(vv) {
			t.Fatalf("unexpected cache value for k=%q; got %q; want %q; bucket[0]=%#v", k, vv, v, &c.buckets[0])
		}
	}

	// Verify all the keys exist
	for i := range itemsCount {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		vv := c.Get(nil, k)
		if string(v) != string(vv) {
			t.Fatalf("unexpected cache value for k=%q; got %q; want %q; bucket[0]=%#v", k, vv, v, &c.buckets[0])
		}
		k = []byte(fmt.Sprintf("new key %d", i))
		v = []byte(fmt.Sprintf("new value %d", i))
		vv = c.Get(nil, k)
		if string(v) != string(vv) {
			t.Fatalf("unexpected cache value for k=%q; got %q; want %q; bucket[0]=%#v", k, vv, v, &c.buckets[0])
		}
	}

	// Verify incorrect maxBytes passed to LoadFromFileOrNew
	c = LoadFromFileOrNew(filePath, maxBytes*10)
	s.Reset()
	c.UpdateStats(&s)
	if s.EntriesCount != 0 {
		t.Fatalf("unexpected non-zero entriesCount; got %d", s.EntriesCount)
	}
	c.Reset()
}

func TestSaveLoadConcurrent(t *testing.T) {
	c := New(1024)
	defer c.Reset()
	c.Set([]byte("foo"), []byte("bar"))

	stopCh := make(chan struct{})

	// Start concurrent workers that run Get and Set on c.
	var wgWorkers sync.WaitGroup
	for range 5 {
		wgWorkers.Add(1)
		go func() {
			defer wgWorkers.Done()
			var buf []byte
			j := 0
			for {
				k := []byte(fmt.Sprintf("key %d", j))
				v := []byte(fmt.Sprintf("value %d", j))
				c.Set(k, v)
				buf = c.Get(buf[:0], k)
				if string(buf) != string(v) {
					panic(fmt.Errorf("unexpected value for key %q; got %q; want %q", k, buf, v))
				}
				j++
				select {
				case <-stopCh:
					return
				default:
				}
			}
		}()
	}

	// Start concurrent SaveToFile and LoadFromFile calls.
	tmpDir, err := ioutil.TempDir("", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	var wgSavers sync.WaitGroup
	for i := range 4 {
		wgSavers.Add(1)
		filePath := filepath.Join(tmpDir, fmt.Sprintf("TestSaveLoadFile.%d.fastcache", i))
		go func() {
			defer wgSavers.Done()
			defer os.RemoveAll(filePath)
			for range 3 {
				if err := c.SaveToFileConcurrent(filePath, 3); err != nil {
					panic(fmt.Errorf("cannot save cache to %q: %s", filePath, err))
				}
				cc, err := LoadFromFile(filePath)
				if err != nil {
					panic(fmt.Errorf("cannot load cache from %q: %s", filePath, err))
				}
				var s Stats
				cc.UpdateStats(&s)
				if s.EntriesCount == 0 {
					panic(fmt.Errorf("unexpected empty cache loaded from %q", filePath))
				}
				cc.Reset()
			}
		}()
	}

	wgSavers.Wait()

	close(stopCh)
	wgWorkers.Wait()
}

func TestSaveToFileShrinksAfterDeleteAndWrite(t *testing.T) {
	const maxChunks = 8
	c := New(bucketsCount * maxChunks * chunkSize)
	defer c.Reset()
	bucketIdx := xxhash.Sum64([]byte("key-000000")) % bucketsCount
	b := &c.buckets[bucketIdx]

	rnd := rand.New(rand.NewSource(1))
	value := make([]byte, 1024)
	var keys [][]byte
	var hashes []uint64
	for i := 0; b.chunksCount < 4; i++ {
		k := []byte(fmt.Sprintf("key-%06d", i))
		h := xxhash.Sum64(k)
		if h%bucketsCount != bucketIdx {
			continue
		}
		if _, err := rnd.Read(value); err != nil {
			t.Fatalf("cannot generate value: %s", err)
		}
		b.Set(k, value, h)
		keys = append(keys, k)
		hashes = append(hashes, h)
	}

	tmpDir, err := ioutil.TempDir("", "fastcache-file-size")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	fillPath := filepath.Join(tmpDir, "fill.fastcache")
	if err := c.SaveToFile(fillPath); err != nil {
		t.Fatalf("cannot save filled cache: %s", err)
	}
	fillSize := getFileSize(t, fillPath)

	for i := range keys {
		if i%4 != 0 {
			b.Del(hashes[i])
		}
	}
	deletePath := filepath.Join(tmpDir, "delete.fastcache")
	if err := c.SaveToFile(deletePath); err != nil {
		t.Fatalf("cannot save cache after deletion: %s", err)
	}
	deleteSize := getFileSize(t, deletePath)
	if deleteSize*4 >= fillSize*3 {
		t.Fatalf("cache file was not reclaimed after deletion; fillSize=%d; deleteSize=%d", fillSize, deleteSize)
	}

	newKeys := make([][]byte, 0, len(keys)/2)
	for i := 0; len(newKeys) < len(keys)/2; i++ {
		k := []byte(fmt.Sprintf("new-key-%06d", i))
		h := xxhash.Sum64(k)
		if h%bucketsCount != bucketIdx {
			continue
		}
		if _, err := rnd.Read(value); err != nil {
			t.Fatalf("cannot generate value: %s", err)
		}
		b.Set(k, value, h)
		newKeys = append(newKeys, k)
	}
	writePath := filepath.Join(tmpDir, "write.fastcache")
	if err := c.SaveToFile(writePath); err != nil {
		t.Fatalf("cannot save cache after writes: %s", err)
	}
	writeSize := getFileSize(t, writePath)
	t.Logf("file size curve: fill=%d delete=%d write=%d", fillSize, deleteSize, writeSize)

	c2, err := LoadFromFile(writePath)
	if err != nil {
		t.Fatalf("cannot load cache after writes: %s", err)
	}
	defer c2.Reset()
	for i := range keys {
		got := c2.Get(nil, keys[i])
		if i%4 == 0 {
			if len(got) != len(value) {
				t.Fatalf("cannot read live key %q after reload; got len=%d; want=%d", keys[i], len(got), len(value))
			}
		} else if len(got) != 0 {
			t.Fatalf("deleted key %q is readable after reload", keys[i])
		}
	}
	for _, k := range newKeys {
		if got := c2.Get(nil, k); len(got) != len(value) {
			t.Fatalf("cannot read new key %q after reload; got len=%d; want=%d", k, len(got), len(value))
		}
	}
}

func getFileSize(t *testing.T, filePath string) int64 {
	t.Helper()
	var size int64
	if err := filepath.Walk(filePath, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			size += info.Size()
		}
		return nil
	}); err != nil {
		t.Fatalf("cannot calculate size of %q: %s", filePath, err)
	}
	return size
}
