// Package compression provides high-performance compression support for Nebula
package compression

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/ajitpratap0/nebula/pkg/pool"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
)

// Algorithm represents a compression algorithm
type Algorithm string

const (
	// None represents no compression
	None Algorithm = "none"
	// Gzip represents gzip compression
	Gzip Algorithm = "gzip"
	// Snappy represents snappy compression
	Snappy Algorithm = "snappy"
	// LZ4 represents lz4 compression
	LZ4 Algorithm = "lz4"
	// Zstd represents zstandard compression
	Zstd Algorithm = "zstd"
	// S2 represents s2 compression (Snappy compatible)
	S2 Algorithm = "s2"
	// Deflate represents deflate compression
	Deflate Algorithm = "deflate"
)

// Level represents compression level
type Level int

const (
	// Fastest prioritizes speed over compression ratio
	Fastest Level = 1
	// Default balances speed and compression
	Default Level = 5
	// Better improves compression at cost of speed
	Better Level = 7
	// Best maximizes compression ratio
	Best Level = 9
)

// Compressor provides compression and decompression functionality
type Compressor interface {
	// Compress compresses data
	Compress(data []byte) ([]byte, error)
	// Decompress decompresses data
	Decompress(data []byte) ([]byte, error)
	// CompressStream compresses from reader to writer
	CompressStream(dst io.Writer, src io.Reader) error
	// DecompressStream decompresses from reader to writer
	DecompressStream(dst io.Writer, src io.Reader) error
	// Algorithm returns the compression algorithm
	Algorithm() Algorithm
	// Level returns the compression level
	Level() Level
}

// Config represents compressor configuration
type Config struct {
	Algorithm   Algorithm
	Level       Level
	BufferSize  int
	Concurrency int
}

// DefaultConfig returns default compression configuration
func DefaultConfig() *Config {
	return &Config{
		Algorithm:   Snappy,
		Level:       Default,
		BufferSize:  64 * 1024, // 64KB
		Concurrency: 4,
	}
}

// NewCompressor creates a new compressor based on configuration
func NewCompressor(config *Config) (Compressor, error) {
	if config == nil {
		config = DefaultConfig()
	}

	switch config.Algorithm {
	case None:
		return &noneCompressor{}, nil
	case Gzip:
		return newGzipCompressor(config)
	case Snappy:
		return newSnappyCompressor(config)
	case LZ4:
		return newLZ4Compressor(config)
	case Zstd:
		return newZstdCompressor(config)
	case S2:
		return newS2Compressor(config)
	case Deflate:
		return newDeflateCompressor(config)
	default:
		return nil, fmt.Errorf("unsupported compression algorithm: %s", config.Algorithm)
	}
}

// CompressorPool provides pooled compressors for better performance
type CompressorPool struct {
	pool    sync.Pool
	config  *Config
	newFunc func() (Compressor, error)
}

// NewCompressorPool creates a new compressor pool
func NewCompressorPool(config *Config) *CompressorPool {
	if config == nil {
		config = DefaultConfig()
	}

	cp := &CompressorPool{
		config: config,
		newFunc: func() (Compressor, error) {
			return NewCompressor(config)
		},
	}

	cp.pool.New = func() interface{} {
		comp, _ := cp.newFunc()
		return comp
	}

	return cp
}

// Get gets a compressor from pool
func (cp *CompressorPool) Get() Compressor {
	return cp.pool.Get().(Compressor)
}

// Put returns compressor to pool
func (cp *CompressorPool) Put(c Compressor) {
	cp.pool.Put(c)
}

// Compress compresses data using a pooled compressor
func (cp *CompressorPool) Compress(data []byte) ([]byte, error) {
	c := cp.Get()
	defer cp.Put(c)
	return c.Compress(data)
}

// Decompress decompresses data using a pooled compressor
func (cp *CompressorPool) Decompress(data []byte) ([]byte, error) {
	c := cp.Get()
	defer cp.Put(c)
	return c.Decompress(data)
}

// Base compressor implementation
type baseCompressor struct {
	algorithm  Algorithm
	level      Level
	bufferSize int
}

// Algorithm returns the compression algorithm
func (bc *baseCompressor) Algorithm() Algorithm {
	return bc.algorithm
}

// Level returns the compression level
func (bc *baseCompressor) Level() Level {
	return bc.level
}

// None compressor (no compression)
type noneCompressor struct {
	baseCompressor
}

func (nc *noneCompressor) Compress(data []byte) ([]byte, error) {
	return data, nil
}

func (nc *noneCompressor) Decompress(data []byte) ([]byte, error) {
	return data, nil
}

func (nc *noneCompressor) CompressStream(dst io.Writer, src io.Reader) error {
	_, err := io.Copy(dst, src)
	return err
}

func (nc *noneCompressor) DecompressStream(dst io.Writer, src io.Reader) error {
	_, err := io.Copy(dst, src)
	return err
}

// Gzip compressor
type gzipCompressor struct {
	baseCompressor
	writerPool sync.Pool
	readerPool sync.Pool
}

func newGzipCompressor(config *Config) (*gzipCompressor, error) {
	level := mapGzipLevel(config.Level)

	gc := &gzipCompressor{
		baseCompressor: baseCompressor{
			algorithm:  Gzip,
			level:      config.Level,
			bufferSize: config.BufferSize,
		},
	}

	gc.writerPool.New = func() interface{} {
		w, _ := gzip.NewWriterLevel(nil, level)
		return w
	}

	gc.readerPool.New = func() interface{} {
		return new(gzip.Reader)
	}

	return gc, nil
}

func (gc *gzipCompressor) Compress(data []byte) ([]byte, error) {
	// Use pooled builder for compression buffer
	builder := stringpool.GetBuilder(stringpool.Medium)
	defer stringpool.PutBuilder(builder, stringpool.Medium)
	
	w := gc.writerPool.Get().(*gzip.Writer)
	defer gc.writerPool.Put(w)

	w.Reset(builder)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}

	// Copy data from pooled builder to result
	result := pool.GetByteSlice()
	if cap(result) < builder.Len() {
		result = make([]byte, builder.Len())
	}
	defer pool.PutByteSlice(result)
	copy(result, builder.Bytes())
	return result, nil
}

func (gc *gzipCompressor) Decompress(data []byte) ([]byte, error) {
	r := gc.readerPool.Get().(*gzip.Reader)
	defer gc.readerPool.Put(r)

	if err := r.Reset(bytes.NewReader(data)); err != nil {
		return nil, err
	}

	// Use pooled builder for decompression buffer
	builder := stringpool.GetBuilder(stringpool.Medium)
	defer stringpool.PutBuilder(builder, stringpool.Medium)
	
	if _, err := io.Copy(builder, r); err != nil {
		return nil, err
	}

	// Copy data from pooled builder to result
	result := pool.GetByteSlice()
	if cap(result) < builder.Len() {
		result = make([]byte, builder.Len())
	}
	defer pool.PutByteSlice(result)
	copy(result, builder.Bytes())
	return result, nil
}

func (gc *gzipCompressor) CompressStream(dst io.Writer, src io.Reader) error {
	w := gc.writerPool.Get().(*gzip.Writer)
	defer gc.writerPool.Put(w)

	w.Reset(dst)
	if _, err := io.Copy(w, src); err != nil {
		return err
	}
	return w.Close()
}

func (gc *gzipCompressor) DecompressStream(dst io.Writer, src io.Reader) error {
	r := gc.readerPool.Get().(*gzip.Reader)
	defer gc.readerPool.Put(r)

	if err := r.Reset(src); err != nil {
		return err
	}

	_, err := io.Copy(dst, r)
	return err
}

// Snappy compressor
type snappyCompressor struct {
	baseCompressor
}

func newSnappyCompressor(config *Config) (*snappyCompressor, error) {
	return &snappyCompressor{
		baseCompressor: baseCompressor{
			algorithm:  Snappy,
			level:      config.Level,
			bufferSize: config.BufferSize,
		},
	}, nil
}

func (sc *snappyCompressor) Compress(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

func (sc *snappyCompressor) Decompress(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}

func (sc *snappyCompressor) CompressStream(dst io.Writer, src io.Reader) error {
	w := snappy.NewBufferedWriter(dst)
	_, err := io.Copy(w, src)
	if err != nil {
		return err
	}
	return w.Close()
}

func (sc *snappyCompressor) DecompressStream(dst io.Writer, src io.Reader) error {
	r := snappy.NewReader(src)
	_, err := io.Copy(dst, r)
	return err
}

// LZ4 compressor
type lz4Compressor struct {
	baseCompressor
	compressionLevel lz4.CompressionLevel
}

func newLZ4Compressor(config *Config) (*lz4Compressor, error) {
	level := mapLZ4Level(config.Level)

	return &lz4Compressor{
		baseCompressor: baseCompressor{
			algorithm:  LZ4,
			level:      config.Level,
			bufferSize: config.BufferSize,
		},
		compressionLevel: level,
	}, nil
}

func (lc *lz4Compressor) Compress(data []byte) ([]byte, error) {
	// Use pooled builder for compression buffer
	builder := stringpool.GetBuilder(stringpool.Medium)
	defer stringpool.PutBuilder(builder, stringpool.Medium)
	
	w := lz4.NewWriter(builder)

	// Apply compression level using the v4 API
	if err := w.Apply(lz4.CompressionLevelOption(lc.compressionLevel)); err != nil {
		return nil, err
	}

	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}

	// Copy data from pooled builder to result
	result := pool.GetByteSlice()
	if cap(result) < builder.Len() {
		result = make([]byte, builder.Len())
	}
	defer pool.PutByteSlice(result)
	copy(result, builder.Bytes())
	return result, nil
}

func (lc *lz4Compressor) Decompress(data []byte) ([]byte, error) {
	r := lz4.NewReader(bytes.NewReader(data))
	
	// Use pooled builder for decompression buffer
	builder := stringpool.GetBuilder(stringpool.Medium)
	defer stringpool.PutBuilder(builder, stringpool.Medium)
	
	if _, err := io.Copy(builder, r); err != nil {
		return nil, err
	}
	
	// Copy data from pooled builder to result
	result := pool.GetByteSlice()
	if cap(result) < builder.Len() {
		result = make([]byte, builder.Len())
	}
	defer pool.PutByteSlice(result)
	copy(result, builder.Bytes())
	return result, nil
}

func (lc *lz4Compressor) CompressStream(dst io.Writer, src io.Reader) error {
	w := lz4.NewWriter(dst)

	// Apply compression level using the v4 API
	if err := w.Apply(lz4.CompressionLevelOption(lc.compressionLevel)); err != nil {
		return err
	}

	if _, err := io.Copy(w, src); err != nil {
		return err
	}
	return w.Close()
}

func (lc *lz4Compressor) DecompressStream(dst io.Writer, src io.Reader) error {
	r := lz4.NewReader(src)
	_, err := io.Copy(dst, r)
	return err
}

// Zstd compressor
type zstdCompressor struct {
	baseCompressor
	encoderPool sync.Pool
	decoderPool sync.Pool
}

func newZstdCompressor(config *Config) (*zstdCompressor, error) {
	level := mapZstdLevel(config.Level)

	zc := &zstdCompressor{
		baseCompressor: baseCompressor{
			algorithm:  Zstd,
			level:      config.Level,
			bufferSize: config.BufferSize,
		},
	}

	zc.encoderPool.New = func() interface{} {
		enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
		return enc
	}

	zc.decoderPool.New = func() interface{} {
		dec, _ := zstd.NewReader(nil)
		return dec
	}

	return zc, nil
}

func (zc *zstdCompressor) Compress(data []byte) ([]byte, error) {
	enc := zc.encoderPool.Get().(*zstd.Encoder)
	defer zc.encoderPool.Put(enc)

	return enc.EncodeAll(data, nil), nil
}

func (zc *zstdCompressor) Decompress(data []byte) ([]byte, error) {
	dec := zc.decoderPool.Get().(*zstd.Decoder)
	defer zc.decoderPool.Put(dec)

	return dec.DecodeAll(data, nil)
}

func (zc *zstdCompressor) CompressStream(dst io.Writer, src io.Reader) error {
	enc := zc.encoderPool.Get().(*zstd.Encoder)
	defer zc.encoderPool.Put(enc)

	enc.Reset(dst)
	_, err := io.Copy(enc, src)
	if err != nil {
		return err
	}
	return enc.Close()
}

func (zc *zstdCompressor) DecompressStream(dst io.Writer, src io.Reader) error {
	dec := zc.decoderPool.Get().(*zstd.Decoder)
	defer zc.decoderPool.Put(dec)

	err := dec.Reset(src)
	if err != nil {
		return err
	}

	_, err = io.Copy(dst, dec)
	return err
}

// S2 compressor (Snappy-compatible but better compression)
type s2Compressor struct {
	baseCompressor
}

func newS2Compressor(config *Config) (*s2Compressor, error) {
	return &s2Compressor{
		baseCompressor: baseCompressor{
			algorithm:  S2,
			level:      config.Level,
			bufferSize: config.BufferSize,
		},
	}, nil
}

func (sc *s2Compressor) Compress(data []byte) ([]byte, error) {
	return s2.Encode(nil, data), nil
}

func (sc *s2Compressor) Decompress(data []byte) ([]byte, error) {
	return s2.Decode(nil, data)
}

func (sc *s2Compressor) CompressStream(dst io.Writer, src io.Reader) error {
	w := s2.NewWriter(dst)
	_, err := io.Copy(w, src)
	if err != nil {
		return err
	}
	return w.Close()
}

func (sc *s2Compressor) DecompressStream(dst io.Writer, src io.Reader) error {
	r := s2.NewReader(src)
	_, err := io.Copy(dst, r)
	return err
}

// Deflate compressor
type deflateCompressor struct {
	baseCompressor
	level int
}

func newDeflateCompressor(config *Config) (*deflateCompressor, error) {
	level := mapDeflateLevel(config.Level)

	return &deflateCompressor{
		baseCompressor: baseCompressor{
			algorithm:  Deflate,
			level:      config.Level,
			bufferSize: config.BufferSize,
		},
		level: level,
	}, nil
}

func (dc *deflateCompressor) Compress(data []byte) ([]byte, error) {
	// Use pooled builder for compression buffer
	builder := stringpool.GetBuilder(stringpool.Medium)
	defer stringpool.PutBuilder(builder, stringpool.Medium)
	
	w, err := flate.NewWriter(builder, dc.level)
	if err != nil {
		return nil, err
	}

	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}

	// Copy data from pooled builder to result
	result := pool.GetByteSlice()
	if cap(result) < builder.Len() {
		result = make([]byte, builder.Len())
	}
	defer pool.PutByteSlice(result)
	copy(result, builder.Bytes())
	return result, nil
}

func (dc *deflateCompressor) Decompress(data []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(data))
	defer r.Close()

	// Use pooled builder for decompression buffer
	builder := stringpool.GetBuilder(stringpool.Medium)
	defer stringpool.PutBuilder(builder, stringpool.Medium)
	
	if _, err := io.Copy(builder, r); err != nil {
		return nil, err
	}

	// Copy data from pooled builder to result
	result := pool.GetByteSlice()
	if cap(result) < builder.Len() {
		result = make([]byte, builder.Len())
	}
	defer pool.PutByteSlice(result)
	copy(result, builder.Bytes())
	return result, nil
}

func (dc *deflateCompressor) CompressStream(dst io.Writer, src io.Reader) error {
	w, err := flate.NewWriter(dst, dc.level)
	if err != nil {
		return err
	}

	if _, err := io.Copy(w, src); err != nil {
		return err
	}
	return w.Close()
}

func (dc *deflateCompressor) DecompressStream(dst io.Writer, src io.Reader) error {
	r := flate.NewReader(src)
	defer r.Close()

	_, err := io.Copy(dst, r)
	return err
}

// Helper functions to map compression levels

func mapGzipLevel(level Level) int {
	switch level {
	case Fastest:
		return gzip.BestSpeed
	case Best:
		return gzip.BestCompression
	default:
		return gzip.DefaultCompression
	}
}

func mapLZ4Level(level Level) lz4.CompressionLevel {
	switch level {
	case Fastest:
		return lz4.Fast
	case Best:
		return lz4.Level9
	default:
		return lz4.Level5
	}
}

func mapZstdLevel(level Level) zstd.EncoderLevel {
	switch level {
	case Fastest:
		return zstd.SpeedFastest
	case Better:
		return zstd.SpeedBetterCompression
	case Best:
		return zstd.SpeedBestCompression
	default:
		return zstd.SpeedDefault
	}
}

func mapDeflateLevel(level Level) int {
	switch level {
	case Fastest:
		return flate.BestSpeed
	case Best:
		return flate.BestCompression
	default:
		return flate.DefaultCompression
	}
}
