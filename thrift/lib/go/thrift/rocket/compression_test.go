/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rocket

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/thrift/rpcmetadata"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func compressDecompressTestHelper(t *testing.T, compress, decompress func([]byte) ([]byte, error)) {
	want := []byte{0x28, 0xb5, 0x2f, 0xfd}

	compressed, err := compress(want)
	require.NoError(t, err)
	require.NotEqual(t, want, compressed)

	got, err := decompress(compressed)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestZstd(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		compressDecompressTestHelper(t, compressZstd, decompressZstd)
	})
	t.Run("less", func(t *testing.T) {
		compressDecompressTestHelper(t, compressZstdLess, decompressZstd)
	})
	t.Run("more", func(t *testing.T) {
		compressDecompressTestHelper(t, compressZstdMore, decompressZstd)
	})
}

// Pooled encoders must produce the same bytes as a new encoder with the
// default window.
func TestZstdPooledEncoderOutputUnchanged(t *testing.T) {
	// Sizes repeat in another order, so most calls reuse an encoder that
	// already encoded a payload of another size. Above 128 KiB, zstd uses
	// more than one block and the encoder history buffer. Above
	// zstdPooledWindow, a new encoder with the default window is used.
	sizes := []int{
		0, 1, 50, 10 << 10, 128 << 10, 128<<10 + 1, 300 << 10, 10 << 10, 1, 300 << 10, 50, 0,
		zstdPooledWindow, zstdPooledWindow + 1, 1 << 20, zstdPooledWindow, 50,
	}
	for _, c := range []struct {
		name     string
		level    zstd.EncoderLevel
		compress func([]byte) ([]byte, error)
	}{
		{"default", zstd.SpeedDefault, compressZstd},
		{"less", zstd.SpeedFastest, compressZstdLess},
		{"more", zstd.SpeedBestCompression, compressZstdMore},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, size := range sizes {
				data := compressibleTestPayload(size)
				got, err := c.compress(data)
				require.NoError(t, err)
				fresh, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(c.level), zstd.WithEncoderConcurrency(1))
				require.NoError(t, err)
				require.True(t, bytes.Equal(fresh.EncodeAll(data, nil), got), "size %d", size)
			}
		})
	}
}

// A pooled encoder must keep giving the same bytes as a new encoder after many
// calls with payloads of mixed sizes and kinds. After about 4,400 calls, a
// pooled encoder resets its match table offsets.
func TestZstdPooledEncoderOutputUnchangedAfterLongReuse(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	sizes := []int{0, 50, 1 << 10, 10 << 10, 64 << 10, 200 << 10}
	for _, c := range []struct {
		name     string
		level    zstd.EncoderLevel
		compress func([]byte) ([]byte, error)
		// largeCalls is how many calls use all sizes; the others use the
		// three smallest.
		largeCalls int
	}{
		{"default", zstd.SpeedDefault, compressZstd, 5000},
		{"less", zstd.SpeedFastest, compressZstdLess, 5000},
		{"more", zstd.SpeedBestCompression, compressZstdMore, 200},
	} {
		t.Run(c.name, func(t *testing.T) {
			// Encoders reset between calls, so one reference encoder gives the
			// same bytes as a new one for every call.
			ref, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(c.level), zstd.WithEncoderConcurrency(1))
			require.NoError(t, err)
			for i := range 5000 {
				size := sizes[i%len(sizes)]
				if i >= c.largeCalls {
					size = sizes[i%3]
				}
				data := compressibleTestPayload(size)
				if i%2 == 1 {
					for j := range data {
						data[j] = byte(r.IntN(256))
					}
				}
				got, err := c.compress(data)
				require.NoError(t, err)
				require.True(t, bytes.Equal(ref.EncodeAll(data, nil), got), "call %d, size %d", i, size)
			}
		})
	}
}

// Calls that do not overlap must reuse one encoder and one decoder instead of
// creating new ones (about 18 allocations and 1.6 MB for an encoder).
func TestZstdReusesEncoderAndDecoder(t *testing.T) {
	data := compressibleTestPayload(50)
	compressed, err := compressZstd(data)
	require.NoError(t, err)
	_, err = decompressZstd(compressed)
	require.NoError(t, err)

	require.LessOrEqual(t, testing.AllocsPerRun(100, func() { _, _ = compressZstd(data) }), 4.0)
	require.LessOrEqual(t, testing.AllocsPerRun(100, func() { _, _ = decompressZstd(compressed) }), 4.0)
}

// A pooled decoder must not keep its last output or input alive.
func TestZstdDecoderDoesNotKeepPayloadAlive(t *testing.T) {
	compressed, err := compressZstd(compressibleTestPayload(300 << 10))
	require.NoError(t, err)
	out, err := decompressZstd(compressed)
	require.NoError(t, err)

	freed := make(chan struct{}, 2)
	runtime.AddCleanup(&out[0], func(ch chan struct{}) { ch <- struct{}{} }, freed)
	runtime.AddCleanup(&compressed[0], func(ch chan struct{}) { ch <- struct{}{} }, freed)
	out, compressed = nil, nil
	for range 2 {
		require.Eventually(t, func() bool {
			runtime.GC()
			select {
			case <-freed:
				return true
			default:
				return false
			}
		}, 10*time.Second, 10*time.Millisecond)
	}
}

// After a large decode, a pooled decoder must keep at most 128 KiB more than
// before, so the pool does not hold memory for the largest payload it decoded.
func TestZstdPooledDecoderKeepsLittleMemory(t *testing.T) {
	small, err := compressZstd(compressibleTestPayload(1 << 20))
	require.NoError(t, err)
	large, err := compressZstd(compressibleTestPayload(32 << 20))
	require.NoError(t, err)
	// The first decode creates the pooled decoder and grows its block buffers.
	_, err = decompressZstd(small)
	require.NoError(t, err)

	before := heapAllocAfterGC()
	func() {
		out, err := decompressZstd(large)
		require.NoError(t, err)
		require.Len(t, out, 32<<20)
	}()
	require.LessOrEqual(t, int64(heapAllocAfterGC())-int64(before), int64(128<<10))
}

func heapAllocAfterGC() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// zstdEmptyFrame is a frame with no content, as zstd.Encoder.EncodeAll makes
// it for empty input.
var zstdEmptyFrame = []byte{0x28, 0xb5, 0x2f, 0xfd, 0x20, 0x00, 0x01, 0x00, 0x00}

func TestZstdEmptyFrame(t *testing.T) {
	compressed, err := compressZstd(nil)
	require.NoError(t, err)
	require.Equal(t, zstdEmptyFrame, compressed)
	got, err := decompressZstd(zstdEmptyFrame)
	require.NoError(t, err)
	require.Empty(t, got)
}

// A pooled encoder keeps its history buffer for good, so the buffer must be
// small: 1 MB, not the 16 MB of the default window. A payload larger than the
// pooled window must not use the pool.
func TestZstdPooledEncoderKeepsSmallHistory(t *testing.T) {
	encoders := newZstdEncoderPool(zstd.SpeedDefault, true)
	data := compressibleTestPayload(zstdPooledWindow)
	var err error
	allocated := bytesAllocated(func() { _, err = compressZstdWithPool(data, encoders) })
	require.NoError(t, err)
	require.Less(t, allocated, uint64(8<<20))
	require.NotNil(t, encoders.hot.Load())

	encoders = newZstdEncoderPool(zstd.SpeedDefault, true)
	_, err = compressZstdWithPool(compressibleTestPayload(zstdPooledWindow+1), encoders)
	require.NoError(t, err)
	require.Nil(t, encoders.hot.Load())
}

// A best level encoder has 34 MB of match tables, so it must not become the
// hot item, which the GC never frees.
func TestZstdMoreEncoderIsNotHot(t *testing.T) {
	_, err := compressZstdMore(compressibleTestPayload(50))
	require.NoError(t, err)
	require.Nil(t, zstdEncodersMore.hot.Load())
}

func TestZstdConcurrentRoundTrip(t *testing.T) {
	algos := []rpcmetadata.CompressionAlgorithm{
		rpcmetadata.CompressionAlgorithm_ZSTD,
		rpcmetadata.CompressionAlgorithm_ZSTD_LESS,
		rpcmetadata.CompressionAlgorithm_ZSTD_MORE,
	}
	payloads := make([][]byte, 8)
	for i := range payloads {
		payloads[i] = compressibleTestPayload(1 + i*40_000)
	}
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Go(func() {
			for i := range 12 {
				algo := algos[(g+i)%len(algos)]
				want := payloads[(g*3+i)%len(payloads)]
				compressed, err := MaybeCompress(want, algo)
				if !assert.NoError(t, err) {
					return
				}
				got, err := MaybeDecompress(compressed, algo)
				if !assert.NoError(t, err) {
					return
				}
				assert.True(t, bytes.Equal(want, got), "%s round trip of %d bytes", algo, len(want))
			}
		})
	}
	wg.Wait()
}

// A pooled decoder must fail on the same inputs as a new decoder, and give the
// same output for the others. The buffers that it kept from the payload before
// can make it pick another decoding path than a new decoder, so the error text
// can differ.
func TestZstdWarmPooledDecoderMatchesNewDecoder(t *testing.T) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	require.NoError(t, err)
	r := rand.New(rand.NewPCG(11, 12))
	random := func(size int, letters int) []byte {
		data := make([]byte, size)
		for i := range data {
			if letters == 6 && r.IntN(3) == 0 {
				data[i] = byte(r.IntN(256))
			} else {
				data[i] = byte('a' + r.IntN(letters))
			}
		}
		return data
	}
	type input struct{ before, bad []byte }
	var inputs []input
	for range 120 {
		// The payload before is the bad one without a few bytes, so its
		// buffers are a little smaller.
		size := 2000 + r.IntN(50000)
		base := random(size, 6)
		tail := slices.Clone(base[size/2 : size/2+64])
		before := enc.EncodeAll(slices.Concat(base, tail), nil)
		frame := enc.EncodeAll(slices.Concat(base, random(1+r.IntN(15), 6), tail), nil)
		for range 8 {
			bad := slices.Clone(frame)
			bad[len(bad)-1-r.IntN(64)] ^= byte(1 << r.IntN(8))
			inputs = append(inputs, input{before, bad})
		}
		// A declared size a few bytes too small, without and with a window.
		if frame[4] != 0x64 { // single segment, 2-byte size, checksum
			continue
		}
		declared := binary.LittleEndian.Uint16(frame[5:])
		for d := range uint16(8) {
			small := slices.Clone(frame)
			binary.LittleEndian.PutUint16(small[5:], declared-d-1)
			windowed := []byte{0x28, 0xb5, 0x2f, 0xfd, 0x44, 0x38} // 128 KiB window
			windowed = binary.LittleEndian.AppendUint16(windowed, declared-d-1)
			inputs = append(inputs, input{before, small}, input{before, append(windowed, frame[7:]...)})
		}
	}
	// A large payload before, then a small frame and a changed frame without
	// a declared size.
	large := enc.EncodeAll(bytes.Repeat([]byte("0123456789abcdefghij"), 20000), nil)
	first := enc.EncodeAll([]byte("hi"), nil)
	for range 40 {
		frame := enc.EncodeAll(random(300+r.IntN(700), 4), nil)
		if frame[4] != 0x44 { // 2-byte size, checksum, window field
			continue
		}
		noSize := slices.Concat([]byte{0x28, 0xb5, 0x2f, 0xfd, 0x04, frame[5]}, frame[8:])
		for range 30 {
			bad := slices.Clone(noSize)
			bad[6+r.IntN(len(bad)-6)] ^= byte(1 << r.IntN(8))
			inputs = append(inputs, input{large, slices.Concat(first, bad)})
		}
	}
	for i, in := range inputs {
		_, err := decompressZstd(in.before)
		require.NoError(t, err)
		fresh, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		require.NoError(t, err)
		want, wantErr := fresh.DecodeAll(in.bad, nil)
		fresh.Close()
		got, gotErr := decompressZstd(in.bad)
		require.Equal(t, wantErr == nil, gotErr == nil, "input %d: %v vs %v", i, wantErr, gotErr)
		if wantErr == nil {
			require.True(t, bytes.Equal(want, got), "input %d: output differs", i)
		}
	}
}

// A pooled decoder must still decode correctly after it failed on bad input.
func TestZstdDecodeAfterBadInput(t *testing.T) {
	want := compressibleTestPayload(10 << 10)
	compressed, err := compressZstd(want)
	require.NoError(t, err)
	corrupt := bytes.Clone(compressed)
	corrupt[len(corrupt)/2] ^= 0xff
	for _, bad := range [][]byte{corrupt, compressed[:len(compressed)/2]} {
		_, err := decompressZstd(bad)
		require.Error(t, err)
		got, err := decompressZstd(compressed)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func bytesAllocated(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// The decoder must keep the default limit on the declared decompressed size.
func TestZstdRejectsOversizedFrame(t *testing.T) {
	// Magic number, a descriptor with an 8-byte content size, and the
	// smallest window.
	frame := []byte{0x28, 0xb5, 0x2f, 0xfd, 0xc0, 0x00}
	frame = binary.LittleEndian.AppendUint64(frame, 1<<40)
	_, err := decompressZstd(frame)
	require.ErrorIs(t, err, zstd.ErrDecoderSizeExceeded)
}

func TestZlib(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		compressDecompressTestHelper(t, compressZlib, decompressZlib)
	})
	t.Run("less", func(t *testing.T) {
		compressDecompressTestHelper(t, compressZlibLess, decompressZlib)
	})
	t.Run("more", func(t *testing.T) {
		compressDecompressTestHelper(t, compressZlibMore, decompressZlib)
	})
}

func TestLz4(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		compressDecompressTestHelper(t, compressLz4, decompressLz4)
	})
	t.Run("less", func(t *testing.T) {
		compressDecompressTestHelper(t, compressLz4Less, decompressLz4)
	})
	t.Run("more", func(t *testing.T) {
		compressDecompressTestHelper(t, compressLz4More, decompressLz4)
	})

	// Larger, highly compressible payload exercises real LZ4 block compression
	// (not just literal-block encoding) and verifies the varint size prefix
	// for sizes that span more than one byte.
	t.Run("compressible payload round-trips across all levels", func(t *testing.T) {
		want := bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog "), 256)
		for _, c := range []struct {
			name     string
			compress func([]byte) ([]byte, error)
		}{
			{"default", compressLz4},
			{"less", compressLz4Less},
			{"more", compressLz4More},
		} {
			t.Run(c.name, func(t *testing.T) {
				compressed, err := c.compress(want)
				require.NoError(t, err)
				require.Less(t, len(compressed), len(want), "compression should shrink repetitive data")
				got, err := decompressLz4(compressed)
				require.NoError(t, err)
				require.Equal(t, want, got)
			})
		}
	})
}

func TestMaybeCompressDecompressLz4(t *testing.T) {
	want := bytes.Repeat([]byte("hello lz4 world "), 64)
	for _, algo := range []rpcmetadata.CompressionAlgorithm{
		rpcmetadata.CompressionAlgorithm_LZ4,
		rpcmetadata.CompressionAlgorithm_LZ4_LESS,
		rpcmetadata.CompressionAlgorithm_LZ4_MORE,
	} {
		t.Run(algo.String(), func(t *testing.T) {
			compressed, err := MaybeCompress(want, algo)
			require.NoError(t, err)
			require.NotEqual(t, want, compressed)

			got, err := MaybeDecompress(compressed, algo)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

// Reject crafted inputs whose varint header declares an implausibly large
// uncompressed size, which would otherwise trigger an oversized allocation.
func TestDecompressLz4RejectsOversizedSizePrefix(t *testing.T) {
	var buf [binary.MaxVarintLen64]byte
	// Declare an absurd uncompressed size with an empty payload.
	n := binary.PutUvarint(buf[:], 1<<40)
	_, err := decompressLz4(buf[:n])
	require.Error(t, err)
	require.Contains(t, err.Error(), "plausible bound")
}

var benchmarkPayloadSizes = []int{50, 10 << 10, 20 << 10, 200 << 10}

func BenchmarkMaybeCompress(b *testing.B) {
	// Run: buck2 run @fbcode//mode/opt fbsource//xplat/thrift/lib/go/thrift/rocket:rocket_test-bench -- -test.bench=BenchmarkMaybe -test.run=^$
	for _, algo := range []rpcmetadata.CompressionAlgorithm{
		rpcmetadata.CompressionAlgorithm_ZSTD,
		rpcmetadata.CompressionAlgorithm_ZSTD_LESS,
		rpcmetadata.CompressionAlgorithm_ZSTD_MORE,
	} {
		for _, size := range benchmarkPayloadSizes {
			data := compressibleTestPayload(size)
			b.Run(fmt.Sprintf("%s/%d", algo, size), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(size))
				for b.Loop() {
					if _, err := MaybeCompress(data, algo); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// Calls that overlap cannot all use the hot encoder, so most take one from the
// sync.Pool.
func BenchmarkMaybeCompressParallel(b *testing.B) {
	for _, size := range benchmarkPayloadSizes {
		data := compressibleTestPayload(size)
		b.Run(fmt.Sprintf("%s/%d", rpcmetadata.CompressionAlgorithm_ZSTD, size), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := MaybeCompress(data, rpcmetadata.CompressionAlgorithm_ZSTD); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

func BenchmarkMaybeDecompress(b *testing.B) {
	for _, size := range benchmarkPayloadSizes {
		compressed, err := MaybeCompress(compressibleTestPayload(size), rpcmetadata.CompressionAlgorithm_ZSTD)
		require.NoError(b, err)
		b.Run(fmt.Sprintf("%s/%d", rpcmetadata.CompressionAlgorithm_ZSTD, size), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				if _, err := MaybeDecompress(compressed, rpcmetadata.CompressionAlgorithm_ZSTD); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// compressibleTestPayload returns size bytes of counter-like text that zstd
// compresses about 3x, similar to serialized telemetry.
func compressibleTestPayload(size int) []byte {
	r := rand.New(rand.NewPCG(1, 2))
	buf := make([]byte, 0, size+64)
	for len(buf) < size {
		buf = fmt.Appendf(buf, "dev%04d.counter_%02d=%d;", r.IntN(2000), r.IntN(64), r.Int64N(1_000_000))
	}
	return buf[:size]
}
