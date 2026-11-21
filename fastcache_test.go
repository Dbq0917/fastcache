package fastcache

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xxhash "github.com/cespare/xxhash/v2"
)

func TestCacheSmall(t *testing.T) {
	c := New(1)
	defer c.Reset()

	if v := c.Get(nil, []byte("aaa")); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); exist || len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}

	c.Set([]byte("key"), []byte("value"))
	if v := c.Get(nil, []byte("key")); string(v) != "value" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "value")
	}
	if v := c.Get(nil, nil); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}
	if v, exist := c.HasGet(nil, nil); exist {
		t.Fatalf("unexpected nil-keyed value obtained in small cache: %q", v)
	}
	if v := c.Get(nil, []byte("aaa")); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}

	c.Set([]byte("aaa"), []byte("bbb"))
	if v := c.Get(nil, []byte("aaa")); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); !exist || string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}

	c.Reset()
	if v := c.Get(nil, []byte("aaa")); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from empty cache: %q", v)
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); exist || len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}

	// Test empty value
	k := []byte("empty")
	c.Set(k, nil)
	if v := c.Get(nil, k); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from empty entry: %q", v)
	}
	if v, exist := c.HasGet(nil, k); !exist {
		t.Fatalf("cannot find empty entry for key %q", k)
	} else if len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from empty entry: %q", v)
	}
	if !c.Has(k) {
		t.Fatalf("cannot find empty entry for key %q", k)
	}
	if c.Has([]byte("foobar")) {
		t.Fatalf("non-existing entry found in the cache")
	}
}

func TestCacheWrap(t *testing.T) {
	c := New(bucketsCount * chunkSize * 1.5)
	defer c.Reset()

	calls := uint64(5e6)

	for i := range calls {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		c.Set(k, v)
		vv := c.Get(nil, k)
		if string(vv) != string(v) {
			t.Fatalf("unexpected value for key %q; got %q; want %q", k, vv, v)
		}
	}
	for i := range calls / 10 {
		x := i * 10
		k := []byte(fmt.Sprintf("key %d", x))
		v := []byte(fmt.Sprintf("value %d", x))
		vv := c.Get(nil, k)
		if len(vv) > 0 && string(v) != string(vv) {
			t.Fatalf("unexpected value for key %q; got %q; want %q", k, vv, v)
		}
	}

	var s Stats
	c.UpdateStats(&s)
	getCalls := calls + calls/10
	if s.GetCalls != getCalls {
		t.Fatalf("unexpected number of getCalls; got %d; want %d", s.GetCalls, getCalls)
	}
	if s.SetCalls != calls {
		t.Fatalf("unexpected number of setCalls; got %d; want %d", s.SetCalls, calls)
	}
	if s.Misses == 0 || s.Misses >= calls/10 {
		t.Fatalf("unexpected number of misses; got %d; it should be between 0 and %d", s.Misses, calls/10)
	}
	if s.Collisions != 0 {
		t.Fatalf("unexpected number of collisions; got %d; want 0", s.Collisions)
	}
	if s.EntriesCount < calls/5 {
		t.Fatalf("unexpected number of items; got %d; cannot be smaller than %d", s.EntriesCount, calls/5)
	}
	if s.BytesSize < 1024 {
		t.Fatalf("unexpected BytesSize; got %d; cannot be smaller than %d", s.BytesSize, 1024)
	}
	if s.MaxBytesSize < 32*1024*1024 {
		t.Fatalf("unexpected MaxBytesSize; got %d; cannot be smaller than %d", s.MaxBytesSize, 32*1024*1024)
	}
}

func TestCacheDel(t *testing.T) {
	c := New(1024)
	defer c.Reset()
	for i := range 100 {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		c.Set(k, v)
		vv := c.Get(nil, k)
		if string(vv) != string(v) {
			t.Fatalf("unexpected value for key %q; got %q; want %q", k, vv, v)
		}
		c.Del(k)
		vv = c.Get(nil, k)
		if len(vv) > 0 {
			t.Fatalf("unexpected non-empty value got for key %q: %q", k, vv)
		}
	}
}

func TestCacheBigKeyValue(t *testing.T) {
	c := New(1024)
	defer c.Reset()

	// Both key and value exceed 64Kb
	k := make([]byte, 90*1024)
	v := make([]byte, 100*1024)
	c.Set(k, v)
	vv := c.Get(nil, k)
	if len(vv) > 0 {
		t.Fatalf("unexpected non-empty value got for key %q: %q", k, vv)
	}

	// len(key) + len(value) > 64Kb
	k = make([]byte, 40*1024)
	v = make([]byte, 40*1024)
	c.Set(k, v)
	vv = c.Get(nil, k)
	if len(vv) > 0 {
		t.Fatalf("unexpected non-empty value got for key %q: %q", k, vv)
	}
}

func TestCacheSetGetSerial(t *testing.T) {
	itemsCount := 10000
	c := New(30 * itemsCount)
	defer c.Reset()
	if err := testCacheGetSet(c, itemsCount); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestCacheGetSetConcurrent(t *testing.T) {
	itemsCount := 10000
	const gorotines = 10
	c := New(30 * itemsCount * gorotines)
	defer c.Reset()

	ch := make(chan error, gorotines)
	for range gorotines {
		go func() {
			ch <- testCacheGetSet(c, itemsCount)
		}()
	}
	for range gorotines {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout")
		}
	}
}

func testCacheGetSet(c *Cache, itemsCount int) error {
	for i := range itemsCount {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		c.Set(k, v)
		vv := c.Get(nil, k)
		if string(vv) != string(v) {
			return fmt.Errorf("unexpected value for key %q after insertion; got %q; want %q", k, vv, v)
		}
	}
	misses := 0
	for i := range itemsCount {
		k := []byte(fmt.Sprintf("key %d", i))
		vExpected := fmt.Sprintf("value %d", i)
		v := c.Get(nil, k)
		if string(v) != string(vExpected) {
			if len(v) > 0 {
				return fmt.Errorf("unexpected value for key %q after all insertions; got %q; want %q", k, v, vExpected)
			}
			misses++
		}
	}
	if misses >= itemsCount/100 {
		return fmt.Errorf("too many cache misses; got %d; want less than %d", misses, itemsCount/100)
	}
	return nil
}

func TestCacheResetUpdateStatsSetConcurrent(t *testing.T) {
	c := New(12334)

	stopCh := make(chan struct{})

	// run workers for cache reset
	var resettersWG sync.WaitGroup
	for range 10 {
		resettersWG.Add(1)
		go func() {
			defer resettersWG.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
					c.Reset()
					runtime.Gosched()
				}
			}
		}()
	}

	// run workers for update cache stats
	var statsWG sync.WaitGroup
	for range 10 {
		statsWG.Add(1)
		go func() {
			defer statsWG.Done()
			var s Stats
			for {
				select {
				case <-stopCh:
					return
				default:
					c.UpdateStats(&s)
					runtime.Gosched()
				}
			}
		}()
	}

	// run workers for setting data to cache
	var settersWG sync.WaitGroup
	for range 10 {
		settersWG.Add(1)
		go func() {
			defer settersWG.Done()
			for j := range 100 {
				key := []byte(fmt.Sprintf("key_%d", j))
				value := []byte(fmt.Sprintf("value_%d", j))
				c.Set(key, value)
				runtime.Gosched()
			}
		}()
	}

	// wait for setters
	settersWG.Wait()
	close(stopCh)
	statsWG.Wait()
	resettersWG.Wait()
}

func TestCacheResetAtomicVisibilityDuringConcurrentReads(t *testing.T) {
	c := New(1)
	defer c.Reset()

	var earlyKey, lateKey, lateOtherKey []byte
	for i := 0; earlyKey == nil || lateKey == nil || lateOtherKey == nil; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		switch xxhash.Sum64(k) % bucketsCount {
		case 0:
			earlyKey = k
		case bucketsCount - 1:
			if lateKey == nil {
				lateKey = k
			} else if lateOtherKey == nil {
				lateOtherKey = k
			}
		}
	}
	earlyValue := []byte("early-value")
	lateValue := []byte("late-value")
	lateOtherValue := []byte("late-other-value")
	c.Set(earlyKey, earlyValue)
	c.Set(lateKey, lateValue)
	c.Set(lateOtherKey, lateOtherValue)
	if v, ok := c.HasGet(nil, earlyKey); !ok || string(v) != string(earlyValue) {
		t.Fatalf("cannot read early key before reset; got %q; ok=%v", v, ok)
	}
	if v, ok := c.HasGet(nil, lateKey); !ok || string(v) != string(lateValue) {
		t.Fatalf("cannot read late key before reset; got %q; ok=%v", v, ok)
	}
	if v, ok := c.HasGet(nil, lateOtherKey); !ok || string(v) != string(lateOtherValue) {
		t.Fatalf("cannot read other late key before reset; got %q; ok=%v", v, ok)
	}

	startGen := atomic.LoadUint64(&c.resetGen)
	lateBucket := &c.buckets[bucketsCount-1]
	pauseBucket := &c.buckets[1]
	pauseBucket.mu.RLock()
	pauseBucketLocked := true
	resetDone := make(chan struct{})
	go func() {
		c.Reset()
		close(resetDone)
	}()
	defer func() {
		if pauseBucketLocked {
			pauseBucket.mu.RUnlock()
		}
		select {
		case <-resetDone:
		case <-time.After(5 * time.Second):
			t.Errorf("reset did not finish")
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for atomic.LoadUint64(&c.resetGen) == startGen {
		if time.Now().After(deadline) {
			t.Fatalf("reset did not advance the cache generation")
		}
		runtime.Gosched()
	}
	for {
		earlyBucket := &c.buckets[0]
		earlyBucket.mu.RLock()
		earlyReset := earlyBucket.resetGen != startGen
		earlyBucket.mu.RUnlock()
		if earlyReset {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("reset did not clear the first bucket")
		}
		runtime.Gosched()
	}
	if pauseBucket.resetGen != startGen {
		t.Fatalf("pause bucket was reset before the test could observe it; resetGen=%d; startGen=%d", pauseBucket.resetGen, startGen)
	}
	if lateBucket.resetGen != startGen {
		t.Fatalf("late bucket was reset before the test could observe it; resetGen=%d; startGen=%d", lateBucket.resetGen, startGen)
	}

	errCh := make(chan error, 1)
	stopCh := make(chan struct{})
	var reads uint64
	var readersWG sync.WaitGroup
	for range 4 {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				for _, k := range [][]byte{earlyKey, lateKey, lateOtherKey} {
					if _, ok := c.HasGet(nil, k); ok {
						select {
						case errCh <- fmt.Errorf("key %q is readable after reset became visible", k):
						default:
						}
						return
					}
					atomic.AddUint64(&reads, 1)
				}
			}
		}()
	}
	for atomic.LoadUint64(&reads) < 10000 {
		if time.Now().After(deadline) {
			close(stopCh)
			readersWG.Wait()
			t.Fatalf("concurrent readers did not make progress during reset")
		}
		runtime.Gosched()
	}
	close(stopCh)
	readersWG.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}

	newEarlyValue := []byte("new-early-value")
	c.Set(earlyKey, newEarlyValue)
	if v, ok := c.HasGet(nil, earlyKey); !ok || string(v) != string(newEarlyValue) {
		t.Fatalf("write to the reset bucket was lost; got %q; ok=%v", v, ok)
	}
	newLateValue := []byte("new-late-value")
	c.Set(lateKey, newLateValue)
	if v, ok := c.HasGet(nil, lateKey); !ok || string(v) != string(newLateValue) {
		t.Fatalf("write to the unreset bucket was lost; got %q; ok=%v", v, ok)
	}
	if v, ok := c.HasGet(nil, lateOtherKey); ok || len(v) != 0 {
		t.Fatalf("old late key is readable after switching the bucket generation; got %q; ok=%v", v, ok)
	}

	pauseBucket.mu.RUnlock()
	pauseBucketLocked = false
	select {
	case <-resetDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("reset did not finish after the late bucket was unblocked")
	}

	if v, ok := c.HasGet(nil, lateKey); !ok || string(v) != string(newLateValue) {
		t.Fatalf("write to the unreset bucket was lost by reset; got %q; ok=%v", v, ok)
	}
	if v, ok := c.HasGet(nil, lateOtherKey); ok || len(v) != 0 {
		t.Fatalf("old late key is readable after reset; got %q; ok=%v", v, ok)
	}
	if v, ok := c.HasGet(nil, earlyKey); !ok || string(v) != string(newEarlyValue) {
		t.Fatalf("write during reset was lost; got %q; ok=%v", v, ok)
	}
}
