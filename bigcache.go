package fastcache

import (
	"sync"
	"sync/atomic"

	xxhash "github.com/cespare/xxhash/v2"
)

// maxSubvalueLen is the maximum size of subvalue chunk.
//
// - 16 bytes are for subkey encoding
// - 4 bytes are for len(key)+len(value) encoding inside fastcache
// - 1 byte is implementation detail of fastcache
const maxSubvalueLen = chunkSize - 16 - 4 - 1

// maxKeyLen is the maximum size of key.
//
// - 16 bytes are for (hash + valueLen)
// - 4 bytes are for len(key)+len(subkey)
// - 1 byte is implementation detail of fastcache
const maxKeyLen = chunkSize - 16 - 4 - 1

const (
	// bigEntryHeaderLen is the size of the header for an entry stored via SetBig:
	//
	// - 4 bytes for key length and the big entry marker
	// - 8 bytes for value length
	// - 8 bytes for value hash
	bigEntryHeaderLen = 4 + 8 + 8

	// bigValueLenMarker distinguishes entries stored via SetBig from regular entries.
	//
	// A regular entry cannot have this value length, since it wouldn't fit a chunk.
	bigValueLenMarker = 1<<16 - 1

	// bigSegmentedMarker marks a key whose value is stored as several contiguous
	// bucket-local records.
	bigSegmentedMarker = 1<<16 - 2
)

// SetBig sets (k, v) to c where len(v) may exceed 64KB.
//
// GetBig must be used for reading stored values.
//
// The stored entry may be evicted at any time either due to cache
// overflow or due to unlikely hash collision.
// Pass higher maxBytes value to New if the added items disappear
// frequently.
//
// It is safe to store entries smaller than 64KB with SetBig.
//
// k and v contents may be modified after returning from SetBig.
func (c *Cache) SetBig(k, v []byte) {
	atomic.AddUint64(&c.bigStats.SetBigCalls, 1)
	if len(k) > maxKeyLen {
		atomic.AddUint64(&c.bigStats.TooBigKeyErrors, 1)
		return
	}
	valueHash := xxhash.Sum64(v)
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	b := &c.buckets[idx]
	if uint64(bigEntryHeaderLen)+uint64(len(k))+uint64(len(v)) < uint64(len(b.chunks))*chunkSize {
		b.setBig(k, v, h, valueHash, uint64(len(v)), bigValueLenMarker)
		return
	}
	c.setBigSegmented(b, k, v, h, valueHash)
}

// GetBig searches for the value for the given k, appends it to dst
// and returns the result.
//
// GetBig returns only values stored via SetBig. It doesn't work
// with values stored via other methods.
//
// k contents may be modified after returning from GetBig.
func (c *Cache) GetBig(dst, k []byte) (r []byte) {
	atomic.AddUint64(&c.bigStats.GetBigCalls, 1)
	dstWasNil := dst == nil
	defer func() {
		if len(r) == 0 && dstWasNil {
			// Guarantee that if the caller provided nil and this is a cache miss that
			// the caller can accurately test for a cache miss with `if r == nil`.
			r = nil
		}
	}()

	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	var status bigEntryStatus
	var meta []byte
	dst, status, meta = c.buckets[idx].GetBig(dst, k, h, c.verifyBigHashes)
	switch status {
	case bigEntryLegacy:
		dst = c.getBigLegacy(dst, meta)
	case bigEntrySegmented:
		dst = c.getBigSegmented(dst, meta)
	case bigEntryInvalidValueLen:
		atomic.AddUint64(&c.bigStats.InvalidValueLenErrors, 1)
	case bigEntryInvalidValueHash:
		atomic.AddUint64(&c.bigStats.InvalidValueHashErrors, 1)
	}
	return dst
}

func (c *Cache) setBigSegmented(b *bucket, k, v []byte, h, valueHash uint64) {
	capacity := uint64(len(b.chunks)) * chunkSize
	maxSegmentLen := maxBigSegmentLen(capacity)
	if maxSegmentLen <= 0 {
		return
	}

	var subkey [16]byte
	for offset := 0; offset < len(v); {
		segmentLen := len(v) - offset
		if segmentLen > maxSegmentLen {
			segmentLen = maxSegmentLen
		}
		segment := v[offset : offset+segmentLen]
		index := uint64(offset / maxSegmentLen)
		marshalUint64(subkey[:0], valueHash)
		marshalUint64(subkey[8:8], index)
		subkeyHash := xxhash.Sum64(subkey[:])
		c.buckets[subkeyHash%bucketsCount].setBig(subkey[:], segment, subkeyHash, xxhash.Sum64(segment), uint64(len(segment)), bigValueLenMarker)
		offset += segmentLen
	}
	b.setBig(k, nil, h, valueHash, uint64(len(v)), bigSegmentedMarker)
}

func (c *Cache) getBigSegmented(dst, meta []byte) []byte {
	if len(meta) != 16 {
		atomic.AddUint64(&c.bigStats.InvalidMetavalueErrors, 1)
		return dst
	}
	valueLen := unmarshalUint64(meta)
	valueHash := unmarshalUint64(meta[8:])
	segmentLen := uint64(maxBigSegmentLen(uint64(len(c.buckets[0].chunks)) * chunkSize))
	if segmentLen == 0 {
		atomic.AddUint64(&c.bigStats.InvalidMetavalueErrors, 1)
		return dst
	}

	dstLen := len(dst)
	if n := dstLen + int(valueLen) - cap(dst); n > 0 {
		dst = append(dst[:cap(dst)], make([]byte, n)...)
	}
	dst = dst[:dstLen]

	var subkey [16]byte
	var offset uint64
	for offset < valueLen {
		n := valueLen - offset
		if n > segmentLen {
			n = segmentLen
		}
		marshalUint64(subkey[:0], valueHash)
		marshalUint64(subkey[8:8], offset/segmentLen)
		subkeyHash := xxhash.Sum64(subkey[:])
		dstNew, status, _ := c.buckets[subkeyHash%bucketsCount].GetBig(dst, subkey[:], subkeyHash, false)
		if status != bigEntryFound || len(dstNew) != len(dst)+int(n) {
			atomic.AddUint64(&c.bigStats.InvalidValueLenErrors, 1)
			return dst[:dstLen]
		}
		dst = dstNew
		offset += n
	}

	if xxhash.Sum64(dst[dstLen:]) != valueHash {
		atomic.AddUint64(&c.bigStats.InvalidValueHashErrors, 1)
		return dst[:dstLen]
	}
	return dst
}

func maxBigSegmentLen(capacity uint64) int {
	const segmentKeyLen = 16
	segmentOverhead := uint64(bigEntryHeaderLen + segmentKeyLen)
	if capacity <= 2*segmentOverhead {
		return 0
	}
	return int((capacity - 2*segmentOverhead - 1) / 2)
}

func (c *Cache) getBigLegacy(dst, meta []byte) []byte {
	if len(meta) != 16 {
		atomic.AddUint64(&c.bigStats.InvalidMetavalueErrors, 1)
		return dst
	}
	valueHash := unmarshalUint64(meta)
	valueLen := unmarshalUint64(meta[8:])

	subkey := getSubkeyBuf()
	defer putSubkeyBuf(subkey)

	dstLen := len(dst)
	if n := dstLen + int(valueLen) - cap(dst); n > 0 {
		dst = append(dst[:cap(dst)], make([]byte, n)...)
	}
	dst = dst[:dstLen]
	var i uint64
	for uint64(len(dst)-dstLen) < valueLen {
		subkey.B = marshalUint64(subkey.B[:0], valueHash)
		subkey.B = marshalUint64(subkey.B, uint64(i))
		i++
		dstNew := c.Get(dst, subkey.B)
		if len(dstNew) == len(dst) {
			// Cannot find subvalue
			return dst[:dstLen]
		}
		dst = dstNew
	}

	// Verify the obtained value.
	v := dst[dstLen:]
	if uint64(len(v)) != valueLen {
		atomic.AddUint64(&c.bigStats.InvalidValueLenErrors, 1)
		return dst[:dstLen]
	}
	h := xxhash.Sum64(v)
	if h != valueHash {
		atomic.AddUint64(&c.bigStats.InvalidValueHashErrors, 1)
		return dst[:dstLen]
	}
	return dst
}

type bigEntryStatus uint8

const (
	bigEntryMiss bigEntryStatus = iota
	bigEntryFound
	bigEntryLegacy
	bigEntrySegmented
	bigEntryInvalidValueLen
	bigEntryInvalidValueHash
)

func (b *bucket) setBig(k, v []byte, h, valueHash, valueLen uint64, marker uint16) {
	atomic.AddUint64(&b.setCalls, 1)

	var header [bigEntryHeaderLen]byte
	header[0] = byte(uint16(len(k)) >> 8)
	header[1] = byte(len(k))
	header[2] = byte(marker >> 8)
	header[3] = byte(marker & 0xff)
	marshalUint64(header[4:4], valueLen)
	marshalUint64(header[12:12], valueHash)

	b.mu.Lock()
	chunks := b.chunks
	capacity := uint64(len(chunks)) * chunkSize
	recordLen := uint64(bigEntryHeaderLen) + uint64(len(k)) + uint64(len(v))
	if recordLen >= capacity {
		b.mu.Unlock()
		return
	}

	idx := b.idx
	needClean := false
	if idx+recordLen >= capacity {
		idx = 0
		b.gen++
		if b.gen&((1<<genSizeBits)-1) == 0 {
			b.gen++
		}
		needClean = true
	}
	b.writeBigLocked(idx, header[:])
	b.writeBigLocked(idx+uint64(len(header)), k)
	b.writeBigLocked(idx+uint64(len(header))+uint64(len(k)), v)
	b.m[h] = idx | (b.gen << bucketSizeBits)
	b.idx = idx + recordLen

	if b.idx%chunkSize == 0 && b.idx < capacity {
		chunkIdx := b.idx / chunkSize
		chunk := chunks[chunkIdx]
		if chunk == nil {
			chunk = getChunk()
		}
		chunks[chunkIdx] = chunk[:0]
	}
	if needClean {
		b.cleanLocked()
	}
	b.mu.Unlock()
}

func (b *bucket) writeBigLocked(idx uint64, p []byte) {
	chunks := b.chunks
	for len(p) > 0 {
		chunkIdx := idx / chunkSize
		offset := int(idx % chunkSize)
		chunk := chunks[chunkIdx]
		if chunk == nil {
			chunk = getChunk()
		}
		chunk = chunk[:offset]
		n := chunkSize - offset
		if n > len(p) {
			n = len(p)
		}
		chunk = append(chunk, p[:n]...)
		chunks[chunkIdx] = chunk
		idx += uint64(n)
		p = p[n:]
	}
}

func (b *bucket) GetBig(dst, k []byte, h uint64, verifyHash bool) ([]byte, bigEntryStatus, []byte) {
	b.mu.RLock()
	atomic.AddUint64(&b.getCalls, 1)

	status := bigEntryMiss
	var meta []byte
	chunks := b.chunks
	v := b.m[h]
	bGen := b.gen & ((1 << genSizeBits) - 1)
	if v > 0 {
		gen := v >> bucketSizeBits
		idx := v & ((1 << bucketSizeBits) - 1)
		if gen == bGen && idx < b.idx || gen+1 == bGen && idx >= b.idx || gen == maxGen && bGen == 1 && idx >= b.idx {
			var header [4]byte
			if !readChunksAt(chunks, idx, header[:]) {
				atomic.AddUint64(&b.corruptions, 1)
				goto end
			}
			keyLen := (uint64(header[0]) << 8) | uint64(header[1])
			valLen := (uint64(header[2]) << 8) | uint64(header[3])
			keyIdx := idx + uint64(len(header))

			if valLen == bigValueLenMarker || valLen == bigSegmentedMarker {
				var bigHeader [bigEntryHeaderLen - 4]byte
				if !readChunksAt(chunks, keyIdx, bigHeader[:]) {
					atomic.AddUint64(&b.corruptions, 1)
					goto end
				}
				valueLen := unmarshalUint64(bigHeader[:8])
				valueHash := unmarshalUint64(bigHeader[8:])
				if keyLen != uint64(len(k)) || !equalChunksAt(chunks, keyIdx+uint64(len(bigHeader)), k) {
					atomic.AddUint64(&b.collisions, 1)
					goto end
				}
				if valLen == bigSegmentedMarker {
					meta = make([]byte, 16)
					marshalUint64(meta[:0], valueLen)
					marshalUint64(meta[8:8], valueHash)
					status = bigEntrySegmented
					goto end
				}
				valueIdx := keyIdx + uint64(len(bigHeader)) + keyLen
				capacity := uint64(len(chunks)) * chunkSize
				if valueLen > capacity || valueIdx > capacity-valueLen {
					atomic.AddUint64(&b.corruptions, 1)
					status = bigEntryInvalidValueLen
					goto end
				}

				dstLen := len(dst)
				if n := dstLen + int(valueLen) - cap(dst); n > 0 {
					dst = append(dst[:cap(dst)], make([]byte, n)...)
				}
				dst = dst[:dstLen+int(valueLen)]
				if !readChunksAt(chunks, valueIdx, dst[dstLen:]) {
					dst = dst[:dstLen]
					atomic.AddUint64(&b.corruptions, 1)
					status = bigEntryInvalidValueLen
					goto end
				}
				if verifyHash && xxhash.Sum64(dst[dstLen:]) != valueHash {
					dst = dst[:dstLen]
					status = bigEntryInvalidValueHash
					goto end
				}
				status = bigEntryFound
				goto end
			}

			offset := int(idx % chunkSize)
			if offset+len(header) >= chunkSize || offset+len(header)+int(keyLen+valLen) >= chunkSize {
				atomic.AddUint64(&b.corruptions, 1)
				goto end
			}
			if keyLen != uint64(len(k)) || !equalChunksAt(chunks, keyIdx, k) {
				atomic.AddUint64(&b.collisions, 1)
				goto end
			}
			meta = make([]byte, int(valLen))
			if !readChunksAt(chunks, keyIdx+keyLen, meta) {
				atomic.AddUint64(&b.corruptions, 1)
				goto end
			}
			status = bigEntryLegacy
		}
	}

end:
	b.mu.RUnlock()
	if status == bigEntryMiss {
		atomic.AddUint64(&b.misses, 1)
	}
	return dst, status, meta
}

func readChunksAt(chunks [][]byte, idx uint64, p []byte) bool {
	for len(p) > 0 {
		chunkIdx := idx / chunkSize
		if chunkIdx >= uint64(len(chunks)) {
			return false
		}
		chunk := chunks[chunkIdx]
		offset := int(idx % chunkSize)
		if chunk == nil || offset >= len(chunk) {
			return false
		}
		n := len(chunk) - offset
		if n > len(p) {
			n = len(p)
		}
		copy(p[:n], chunk[offset:offset+n])
		idx += uint64(n)
		p = p[n:]
	}
	return true
}

func equalChunksAt(chunks [][]byte, idx uint64, p []byte) bool {
	for len(p) > 0 {
		chunkIdx := idx / chunkSize
		if chunkIdx >= uint64(len(chunks)) {
			return false
		}
		chunk := chunks[chunkIdx]
		offset := int(idx % chunkSize)
		if chunk == nil || offset >= len(chunk) {
			return false
		}
		n := len(chunk) - offset
		if n > len(p) {
			n = len(p)
		}
		for i := 0; i < n; i++ {
			if chunk[offset+i] != p[i] {
				return false
			}
		}
		idx += uint64(n)
		p = p[n:]
	}
	return true
}

func getSubkeyBuf() *bytesBuf {
	v := subkeyPool.Get()
	if v == nil {
		return &bytesBuf{}
	}
	return v.(*bytesBuf)
}

func putSubkeyBuf(bb *bytesBuf) {
	bb.B = bb.B[:0]
	subkeyPool.Put(bb)
}

var subkeyPool sync.Pool

type bytesBuf struct {
	B []byte
}

func marshalUint64(dst []byte, u uint64) []byte {
	return append(dst, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32), byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

func unmarshalUint64(src []byte) uint64 {
	_ = src[7]
	return uint64(src[0])<<56 | uint64(src[1])<<48 | uint64(src[2])<<40 | uint64(src[3])<<32 | uint64(src[4])<<24 | uint64(src[5])<<16 | uint64(src[6])<<8 | uint64(src[7])
}
