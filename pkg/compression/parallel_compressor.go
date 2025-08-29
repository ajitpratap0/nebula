package compression

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/ajitpratap0/nebula/pkg/errors"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"go.uber.org/zap"
)

// ParallelCompressor implements parallel compression for large data
type ParallelCompressor struct {
	logger     *zap.Logger
	algorithm  Algorithm
	level      Level
	numWorkers int
	chunkSize  int

	// Metrics
	bytesProcessed  int64
	chunksProcessed int64

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// CompressedChunk represents a compressed data chunk
type CompressedChunk struct {
	ID         int
	Original   []byte
	Compressed []byte
	Error      error
}

// ParallelConfig configures parallel compression
type ParallelConfig struct {
	Algorithm  Algorithm
	Level      Level
	NumWorkers int // 0 = auto (NumCPU)
	ChunkSize  int // Size of each chunk in bytes
}

// NewParallelCompressor creates a new parallel compressor
func NewParallelCompressor(config ParallelConfig, logger *zap.Logger) *ParallelCompressor {
	if config.NumWorkers == 0 {
		config.NumWorkers = runtime.NumCPU()
	}
	if config.ChunkSize == 0 {
		config.ChunkSize = 1024 * 1024 // 1MB default chunk size
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &ParallelCompressor{
		logger:     logger,
		algorithm:  config.Algorithm,
		level:      config.Level,
		numWorkers: config.NumWorkers,
		chunkSize:  config.ChunkSize,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// CompressData compresses data in parallel
func (pc *ParallelCompressor) CompressData(data []byte) ([]byte, error) {
	if len(data) <= pc.chunkSize {
		// Small data, compress directly
		return pc.compressChunk(data)
	}

	// Split into chunks
	chunks := pc.splitIntoChunks(data)
	numChunks := len(chunks)

	// Create channels for work distribution
	chunkChan := make(chan struct {
		id   int
		data []byte
	}, pc.numWorkers)
	resultChan := make(chan CompressedChunk, numChunks)

	// Start workers
	for i := 0; i < pc.numWorkers; i++ {
		pc.wg.Add(1)
		go pc.compressionWorker(chunkChan, resultChan)
	}

	// Distribute work
	go func() {
		for i, chunk := range chunks {
			select {
			case chunkChan <- struct {
				id   int
				data []byte
			}{i, chunk}:
			case <-pc.ctx.Done():
				close(chunkChan)
				return
			}
		}
		close(chunkChan)
	}()

	// Collect results
	results := make(map[int]CompressedChunk)
	for i := 0; i < numChunks; i++ {
		result := <-resultChan
		if result.Error != nil {
			pc.cancel() // Cancel all workers
			pc.wg.Wait()
			return nil, result.Error
		}
		results[result.ID] = result
	}

	// Wait for workers to finish
	pc.wg.Wait()

	// Combine compressed chunks in order
	estimatedSize := len(data) + numChunks*8 // Estimate with some overhead
	bufData := pool.GlobalBufferPool.Get(estimatedSize)
	defer pool.GlobalBufferPool.Put(bufData)

	// Create a bytes.Buffer wrapper around the pooled slice
	output := bytes.NewBuffer(bufData[:0])

	// Write header with chunk information
	header := pc.createHeader(numChunks, len(data))
	output.Write(header)

	// Write compressed chunks
	for i := 0; i < numChunks; i++ {
		chunk := results[i]
		// Write chunk size (4 bytes)
		output.Write([]byte{
			byte(len(chunk.Compressed) >> 24),
			byte(len(chunk.Compressed) >> 16),
			byte(len(chunk.Compressed) >> 8),
			byte(len(chunk.Compressed)),
		})
		// Write compressed data
		output.Write(chunk.Compressed)

		atomic.AddInt64(&pc.bytesProcessed, int64(len(chunk.Original)))
		atomic.AddInt64(&pc.chunksProcessed, 1)
	}

	return output.Bytes(), nil
}

// DecompressData decompresses data in parallel
func (pc *ParallelCompressor) DecompressData(data []byte) ([]byte, error) {
	if len(data) < 12 { // Minimum header size
		return pc.decompressChunk(data)
	}

	// Read header
	numChunks, originalSize, headerSize := pc.readHeader(data)
	if numChunks <= 1 {
		// Single chunk, decompress directly
		return pc.decompressChunk(data[headerSize:])
	}

	// Parse compressed chunks
	offset := headerSize
	chunks := make([]struct {
		id   int
		data []byte
	}, 0, numChunks)

	for i := 0; i < numChunks; i++ {
		if offset+4 > len(data) {
			return nil, errors.New(errors.ErrorTypeData, "corrupted compressed data")
		}

		// Read chunk size
		chunkSize := int(data[offset])<<24 | int(data[offset+1])<<16 |
			int(data[offset+2])<<8 | int(data[offset+3])
		offset += 4

		if offset+chunkSize > len(data) {
			return nil, errors.New(errors.ErrorTypeData, "corrupted compressed data")
		}

		chunks = append(chunks, struct {
			id   int
			data []byte
		}{
			id:   i,
			data: data[offset : offset+chunkSize],
		})
		offset += chunkSize
	}

	// Create channels for parallel decompression
	chunkChan := make(chan struct {
		id   int
		data []byte
	}, pc.numWorkers)
	resultChan := make(chan CompressedChunk, numChunks)

	// Start workers
	for i := 0; i < pc.numWorkers; i++ {
		pc.wg.Add(1)
		go pc.decompressionWorker(chunkChan, resultChan)
	}

	// Distribute work
	go func() {
		for _, chunk := range chunks {
			select {
			case chunkChan <- chunk:
			case <-pc.ctx.Done():
				close(chunkChan)
				return
			}
		}
		close(chunkChan)
	}()

	// Collect results
	results := make(map[int][]byte)
	for i := 0; i < numChunks; i++ {
		result := <-resultChan
		if result.Error != nil {
			pc.cancel()
			pc.wg.Wait()
			return nil, result.Error
		}
		results[result.ID] = result.Original // Decompressed data is in Original field
	}

	// Wait for workers to finish
	pc.wg.Wait()

	// Combine decompressed chunks
	output := pool.GetByteSlice()
	if cap(output) < originalSize {
		output = make([]byte, 0, originalSize)
	}
	defer pool.PutByteSlice(output)
	for i := 0; i < numChunks; i++ {
		output = append(output, results[i]...)
	}

	return output, nil
}

// compressionWorker processes compression tasks
func (pc *ParallelCompressor) compressionWorker(
	chunkChan <-chan struct {
		id   int
		data []byte
	},
	resultChan chan<- CompressedChunk) {

	defer pc.wg.Done()

	for chunk := range chunkChan {
		compressed, err := pc.compressChunk(chunk.data)
		resultChan <- CompressedChunk{
			ID:         chunk.id,
			Original:   chunk.data,
			Compressed: compressed,
			Error:      err,
		}
	}
}

// decompressionWorker processes decompression tasks
func (pc *ParallelCompressor) decompressionWorker(
	chunkChan <-chan struct {
		id   int
		data []byte
	},
	resultChan chan<- CompressedChunk) {

	defer pc.wg.Done()

	for chunk := range chunkChan {
		decompressed, err := pc.decompressChunk(chunk.data)
		resultChan <- CompressedChunk{
			ID:       chunk.id,
			Original: decompressed, // Store decompressed in Original
			Error:    err,
		}
	}
}

// compressChunk compresses a single chunk
func (pc *ParallelCompressor) compressChunk(data []byte) ([]byte, error) {
	buffer := pool.GlobalBufferPool.Get(len(data))
	defer pool.GlobalBufferPool.Put(buffer)

	buf := bytes.NewBuffer(buffer[:0])

	switch pc.algorithm {
	case Gzip:
		w, _ := gzip.NewWriterLevel(buf, pc.getGzipLevel())
		_, err := w.Write(data)
		w.Close()
		return buf.Bytes(), err

	case Snappy:
		return snappy.Encode(nil, data), nil

	case LZ4:
		w := lz4.NewWriter(buf)
		w.Apply(lz4.CompressionLevelOption(pc.getLZ4Level()))
		_, err := w.Write(data)
		w.Close()
		return buf.Bytes(), err

	case Zstd:
		encoder, _ := zstd.NewWriter(buf,
			zstd.WithEncoderLevel(pc.getZstdLevel()))
		_, err := encoder.Write(data)
		encoder.Close()
		return buf.Bytes(), err

	case S2:
		return s2.EncodeSnappy(nil, data), nil

	default:
		return nil, errors.New(errors.ErrorTypeConfig,
			"unsupported compression algorithm for parallel compression")
	}
}

// decompressChunk decompresses a single chunk
func (pc *ParallelCompressor) decompressChunk(data []byte) ([]byte, error) {
	switch pc.algorithm {
	case Gzip:
		r, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)

	case Snappy:
		return snappy.Decode(nil, data)

	case LZ4:
		r := lz4.NewReader(bytes.NewReader(data))
		return io.ReadAll(r)

	case Zstd:
		decoder, _ := zstd.NewReader(bytes.NewReader(data))
		defer decoder.Close()
		return io.ReadAll(decoder)

	case S2:
		return s2.Decode(nil, data)

	default:
		return nil, errors.New(errors.ErrorTypeConfig,
			"unsupported compression algorithm for parallel decompression")
	}
}

// splitIntoChunks splits data into chunks for parallel processing
func (pc *ParallelCompressor) splitIntoChunks(data []byte) [][]byte {
	var chunks [][]byte

	for i := 0; i < len(data); i += pc.chunkSize {
		end := i + pc.chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunks = append(chunks, data[i:end])
	}

	return chunks
}

// createHeader creates a header for parallel compressed data
func (pc *ParallelCompressor) createHeader(numChunks, originalSize int) []byte {
	// Header format:
	// - Magic bytes (4): "PCMP" (Parallel CoMPression)
	// - Version (1): 0x01
	// - Algorithm (1): compression type
	// - NumChunks (2): number of chunks
	// - OriginalSize (4): original data size
	header := pool.GetByteSlice()

	if cap(header) < 12 {

		header = make([]byte, 12)

	}

	defer pool.PutByteSlice(header)
	copy(header[0:4], []byte("PCMP"))
	header[4] = 0x01 // Version
	// Map algorithm to byte
	var algByte byte
	switch pc.algorithm {
	case Gzip:
		algByte = 1
	case Snappy:
		algByte = 2
	case LZ4:
		algByte = 3
	case Zstd:
		algByte = 4
	case S2:
		algByte = 5
	default:
		algByte = 0
	}
	header[5] = algByte
	header[6] = byte(numChunks >> 8)
	header[7] = byte(numChunks)
	header[8] = byte(originalSize >> 24)
	header[9] = byte(originalSize >> 16)
	header[10] = byte(originalSize >> 8)
	header[11] = byte(originalSize)

	return header
}

// readHeader reads the parallel compression header
func (pc *ParallelCompressor) readHeader(data []byte) (numChunks, originalSize, headerSize int) {
	if len(data) < 12 || string(data[0:4]) != "PCMP" {
		return 1, len(data), 0 // Not parallel compressed
	}

	// version := data[4] // For future use
	// algorithm := CompressionType(data[5]) // For validation
	numChunks = int(data[6])<<8 | int(data[7])
	originalSize = int(data[8])<<24 | int(data[9])<<16 | int(data[10])<<8 | int(data[11])

	return numChunks, originalSize, 12
}

// Helper methods to get compression levels
func (pc *ParallelCompressor) getGzipLevel() int {
	switch pc.level {
	case Fastest:
		return gzip.BestSpeed
	case Best:
		return gzip.BestCompression
	default:
		return gzip.DefaultCompression
	}
}

func (pc *ParallelCompressor) getLZ4Level() lz4.CompressionLevel {
	switch pc.level {
	case Fastest:
		return lz4.Fast
	case Best:
		return lz4.Level9
	default:
		return lz4.Level4
	}
}

func (pc *ParallelCompressor) getZstdLevel() zstd.EncoderLevel {
	switch pc.level {
	case Fastest:
		return zstd.SpeedFastest
	case Best:
		return zstd.SpeedBestCompression
	default:
		return zstd.SpeedDefault
	}
}

// GetMetrics returns compression metrics
func (pc *ParallelCompressor) GetMetrics() (bytesProcessed, chunksProcessed int64) {
	return atomic.LoadInt64(&pc.bytesProcessed), atomic.LoadInt64(&pc.chunksProcessed)
}

// Stop stops the parallel compressor
func (pc *ParallelCompressor) Stop() {
	pc.cancel()
	pc.wg.Wait()
}
