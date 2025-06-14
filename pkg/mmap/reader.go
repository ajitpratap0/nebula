// Package mmap provides memory-mapped file I/O for zero-copy high-performance reading
package mmap

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"unsafe"
)

// Reader provides memory-mapped file reading with zero-copy performance
type Reader struct {
	file     *os.File
	data     []byte
	fileSize int64
	pageSize int

	// Prefetch control
	prefetch         bool
	prefetchDistance int

	// Parallel processing
	numWorkers int
	chunkSize  int64

	// Stats
	bytesRead int64
	pagesRead int64

	mu sync.RWMutex
}

// NewReader creates a new memory-mapped file reader
func NewReader(filename string) (*Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	fileSize := stat.Size()
	if fileSize == 0 {
		file.Close()
		return nil, fmt.Errorf("file is empty")
	}

	// Memory map the file
	data, err := mmap(int(file.Fd()), 0, int(fileSize),
		PROT_READ, MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to mmap file: %w", err)
	}

	// Advise kernel about access pattern
	err = madvise(data, MADV_SEQUENTIAL)
	if err != nil {
		// Non-fatal, just log
		fmt.Printf("madvise failed: %v\n", err)
	}

	pageSize := os.Getpagesize()

	return &Reader{
		file:             file,
		data:             data,
		fileSize:         fileSize,
		pageSize:         pageSize,
		prefetch:         true,
		prefetchDistance: 16 * pageSize, // Prefetch 16 pages ahead
		numWorkers:       runtime.NumCPU(),
		chunkSize:        1024 * 1024, // 1MB chunks
	}, nil
}

// ReadAll returns the entire memory-mapped file data
func (r *Reader) ReadAll() []byte {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Trigger prefetch for entire file
	if r.prefetch {
		r.prefetchRange(0, r.fileSize)
	}

	r.bytesRead = r.fileSize
	r.pagesRead = (r.fileSize + int64(r.pageSize) - 1) / int64(r.pageSize)

	return r.data
}

// ReadRange reads a specific range from the memory-mapped file
func (r *Reader) ReadRange(offset, length int64) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if offset < 0 || offset >= r.fileSize {
		return nil, fmt.Errorf("offset %d out of range [0, %d)", offset, r.fileSize)
	}

	end := offset + length
	if end > r.fileSize {
		end = r.fileSize
	}

	// Prefetch if enabled
	if r.prefetch {
		r.prefetchRange(offset, end)
	}

	r.bytesRead += end - offset
	r.pagesRead += ((end - offset) + int64(r.pageSize) - 1) / int64(r.pageSize)

	return r.data[offset:end], nil
}

// ProcessParallel processes the file in parallel chunks
func (r *Reader) ProcessParallel(processor func(chunk []byte, offset int64) error) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Create worker pool
	type work struct {
		offset int64
		length int64
	}

	workChan := make(chan work, r.numWorkers*2)
	errChan := make(chan error, r.numWorkers)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < r.numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for w := range workChan {
				chunk := r.data[w.offset : w.offset+w.length]

				// Prefetch next chunk
				if r.prefetch && w.offset+w.length < r.fileSize {
					nextOffset := w.offset + w.length
					nextEnd := nextOffset + r.chunkSize
					if nextEnd > r.fileSize {
						nextEnd = r.fileSize
					}
					r.prefetchRange(nextOffset, nextEnd)
				}

				if err := processor(chunk, w.offset); err != nil {
					select {
					case errChan <- err:
					default:
					}
					return
				}
			}
		}()
	}

	// Distribute work
	for offset := int64(0); offset < r.fileSize; offset += r.chunkSize {
		length := r.chunkSize
		if offset+length > r.fileSize {
			length = r.fileSize - offset
		}

		workChan <- work{offset: offset, length: length}
	}

	close(workChan)
	wg.Wait()

	// Check for errors
	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

// prefetchRange advises kernel to prefetch a range of pages
func (r *Reader) prefetchRange(start, end int64) {
	// Align to page boundaries
	startPage := (start / int64(r.pageSize)) * int64(r.pageSize)
	endPage := ((end + int64(r.pageSize) - 1) / int64(r.pageSize)) * int64(r.pageSize)

	if endPage > r.fileSize {
		endPage = r.fileSize
	}

	length := endPage - startPage
	if length <= 0 {
		return
	}

	// Advise kernel to prefetch
	_ = madvise(r.data[startPage:endPage], MADV_WILLNEED)
}

// Close unmaps the file and closes it
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var err error

	// Unmap the file
	if r.data != nil {
		err = munmap(r.data)
		r.data = nil
	}

	// Close the file
	if r.file != nil {
		if closeErr := r.file.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		r.file = nil
	}

	return err
}

// Stats returns reading statistics
func (r *Reader) Stats() (bytesRead, pagesRead int64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.bytesRead, r.pagesRead
}

// LineReader provides line-oriented reading over memory-mapped data
type LineReader struct {
	reader    *Reader
	data      []byte
	offset    int64
	batchSize int
}

// NewLineReader creates a new line reader over memory-mapped file
func NewLineReader(filename string, batchSize int) (*LineReader, error) {
	reader, err := NewReader(filename)
	if err != nil {
		return nil, err
	}

	return &LineReader{
		reader:    reader,
		data:      reader.ReadAll(),
		offset:    0,
		batchSize: batchSize,
	}, nil
}

// ReadBatch reads a batch of lines
func (lr *LineReader) ReadBatch() ([][]byte, error) {
	if lr.data == nil || lr.offset >= int64(len(lr.data)) {
		return nil, nil // EOF
	}

	lines := make([][]byte, 0, lr.batchSize)

	for i := 0; i < lr.batchSize && lr.offset < int64(len(lr.data)); i++ {
		// Find next newline
		start := lr.offset
		end := lr.offset

		// Ensure we don't go out of bounds
		for end < int64(len(lr.data)) && lr.data[end] != '\n' {
			end++
		}

		if end < int64(len(lr.data)) {
			// Include the newline
			end++
		}

		// Ensure slice is within bounds
		if start > int64(len(lr.data)) || end > int64(len(lr.data)) || start > end {
			break
		}

		// Zero-copy line slice
		line := lr.data[start:end]
		lines = append(lines, line)

		lr.offset = end
	}

	return lines, nil
}

// Close closes the line reader
func (lr *LineReader) Close() error {
	if lr.reader != nil {
		return lr.reader.Close()
	}
	return nil
}

// ParallelCSVReader reads CSV files using memory mapping and parallel processing
type ParallelCSVReader struct {
	reader     *LineReader
	delimiter  byte
	numWorkers int
}

// GetReader returns the underlying LineReader
func (pcr *ParallelCSVReader) GetReader() *LineReader {
	return pcr.reader
}

// NewParallelCSVReader creates a new parallel CSV reader
func NewParallelCSVReader(filename string, delimiter byte, numWorkers int) (*ParallelCSVReader, error) {
	lineReader, err := NewLineReader(filename, 1000) // 1000 lines per batch
	if err != nil {
		return nil, err
	}

	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	return &ParallelCSVReader{
		reader:     lineReader,
		delimiter:  delimiter,
		numWorkers: numWorkers,
	}, nil
}

// ReadAll reads all CSV records in parallel
func (pcr *ParallelCSVReader) ReadAll() ([][]string, error) {
	type result struct {
		index   int
		records [][]string
	}

	resultChan := make(chan result, pcr.numWorkers*2)
	workChan := make(chan struct {
		index int
		lines [][]byte
	}, pcr.numWorkers*2)

	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < pcr.numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range workChan {
				records := make([][]string, len(work.lines))
				for i, line := range work.lines {
					records[i] = pcr.parseLine(line)
				}
				resultChan <- result{index: work.index, records: records}
			}
		}()
	}

	// Read and distribute work
	go func() {
		index := 0
		for {
			lines, err := pcr.reader.ReadBatch()
			if err != nil || len(lines) == 0 {
				break
			}

			workChan <- struct {
				index int
				lines [][]byte
			}{index: index, lines: lines}

			index++
		}
		close(workChan)
	}()

	// Wait for workers to finish
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results in order
	results := make(map[int][][]string)
	maxIndex := -1

	for res := range resultChan {
		results[res.index] = res.records
		if res.index > maxIndex {
			maxIndex = res.index
		}
	}

	// Flatten results
	var allRecords [][]string
	for i := 0; i <= maxIndex; i++ {
		if records, ok := results[i]; ok {
			allRecords = append(allRecords, records...)
		}
	}

	return allRecords, nil
}

// parseLine parses a CSV line (zero-copy where possible)
func (pcr *ParallelCSVReader) parseLine(line []byte) []string {
	// Remove trailing newline if present
	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}

	// Fast path for simple cases
	fields := make([]string, 0, 16)
	start := 0

	for i := 0; i < len(line); i++ {
		if line[i] == pcr.delimiter {
			// Use unsafe string conversion for zero-copy
			fields = append(fields, unsafeString(line[start:i]))
			start = i + 1
		}
	}

	// Add last field
	if start <= len(line) {
		fields = append(fields, unsafeString(line[start:]))
	}

	return fields
}

// unsafeString converts bytes to string without copying
func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// Close closes the parallel CSV reader
func (pcr *ParallelCSVReader) Close() error {
	return pcr.reader.Close()
}
