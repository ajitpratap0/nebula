package csv

import (
	"bufio"
	"context"
	"encoding/csv"
	"io"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ajitpratap0/nebula/pkg/nebulaerrors"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/pool"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
	"go.uber.org/zap"
)

// ParallelCSVParser implements parallel CSV parsing for improved performance
type ParallelCSVParser struct {
	logger     *zap.Logger
	numWorkers int
	chunkSize  int
	headers    []string
	skipHeader bool
	delimiter  rune

	// Parsing state
	parseFunc func([]string) (*models.Record, error)

	// Metrics
	rowsParsed int64
	errors     int64

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// CSVChunk represents a chunk of CSV data to be processed
type CSVChunk struct {
	ID       int
	Lines    []string
	StartRow int
}

// ParallelCSVConfig configures the parallel CSV parser
type ParallelCSVConfig struct {
	NumWorkers int // Number of parallel workers (0 = auto)
	ChunkSize  int // Lines per chunk
	Headers    []string
	SkipHeader bool
	Delimiter  rune
	ParseFunc  func([]string) (*models.Record, error)
}

// NewParallelCSVParser creates a new parallel CSV parser
func NewParallelCSVParser(config ParallelCSVConfig, logger *zap.Logger) *ParallelCSVParser {
	if config.NumWorkers == 0 {
		config.NumWorkers = runtime.NumCPU()
	}
	if config.ChunkSize == 0 {
		config.ChunkSize = 1000 // Default chunk size
	}
	if config.Delimiter == 0 {
		config.Delimiter = ','
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &ParallelCSVParser{
		logger:     logger,
		numWorkers: config.NumWorkers,
		chunkSize:  config.ChunkSize,
		headers:    config.Headers,
		skipHeader: config.SkipHeader,
		delimiter:  config.Delimiter,
		parseFunc:  config.ParseFunc,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// ParseFile parses a CSV file in parallel
func (p *ParallelCSVParser) ParseFile(reader io.Reader) (<-chan *models.Record, <-chan error) {
	recordChan := make(chan *models.Record, p.numWorkers*p.chunkSize)
	errorChan := make(chan error, p.numWorkers)

	// Create chunk channel
	chunkChan := make(chan *CSVChunk, p.numWorkers*2)

	// Start chunk reader
	p.wg.Add(1)
	go p.readChunks(reader, chunkChan)

	// Start worker pool
	for i := 0; i < p.numWorkers; i++ {
		p.wg.Add(1)
		go p.parseWorker(i, chunkChan, recordChan, errorChan)
	}

	// Close channels when done
	go func() {
		p.wg.Wait()
		close(recordChan)
		close(errorChan)
	}()

	return recordChan, errorChan
}

// readChunks reads the input and creates chunks for parallel processing
func (p *ParallelCSVParser) readChunks(reader io.Reader, chunkChan chan<- *CSVChunk) {
	defer p.wg.Done()
	defer close(chunkChan)

	scanner := bufio.NewScanner(reader)
	scanner.Buffer(pool.GlobalBufferPool.Get(64 * 1024)[:0], 1024*1024) // Use pooled buffer

	chunkID := 0
	currentChunk := &CSVChunk{
		ID:       chunkID,
		Lines:    make([]string, 0, p.chunkSize),
		StartRow: 0,
	}

	rowNum := 0

	// Skip header if needed
	if p.skipHeader && scanner.Scan() {
		rowNum++
	}

	for scanner.Scan() {
		select {
		case <-p.ctx.Done():
			return
		default:
			line := scanner.Text()

			// Add line to current chunk
			currentChunk.Lines = append(currentChunk.Lines, line)

			// Send chunk if full
			if len(currentChunk.Lines) >= p.chunkSize {
				select {
				case chunkChan <- currentChunk:
					chunkID++
					currentChunk = &CSVChunk{
						ID:       chunkID,
						Lines:    make([]string, 0, p.chunkSize),
						StartRow: rowNum + 1,
					}
				case <-p.ctx.Done():
					return
				}
			}

			rowNum++
		}
	}

	// Send final chunk if not empty
	if len(currentChunk.Lines) > 0 {
		select {
		case chunkChan <- currentChunk:
		case <-p.ctx.Done():
			return
		}
	}

	if err := scanner.Err(); err != nil {
		p.logger.Error("error reading CSV", zap.Error(err))
	}
}

// parseWorker processes CSV chunks in parallel
func (p *ParallelCSVParser) parseWorker(workerID int, chunkChan <-chan *CSVChunk, recordChan chan<- *models.Record, errorChan chan<- error) {
	defer p.wg.Done()

	// Create a CSV reader for this worker
	for chunk := range chunkChan {
		select {
		case <-p.ctx.Done():
			return
		default:
			p.processChunk(workerID, chunk, recordChan, errorChan)
		}
	}
}

// processChunk processes a single chunk of CSV data
func (p *ParallelCSVParser) processChunk(workerID int, chunk *CSVChunk, recordChan chan<- *models.Record, errorChan chan<- error) {
	for i, line := range chunk.Lines {
		// Parse CSV line
		reader := csv.NewReader(strings.NewReader(line))
		reader.Comma = p.delimiter
		reader.FieldsPerRecord = -1 // Allow variable fields

		fields, err := reader.Read()
		if err != nil {
			atomic.AddInt64(&p.errors, 1)
			select {
			case errorChan <- nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeData,
				stringpool.Sprintf("failed to parse line %d", chunk.StartRow+i+1)):
			case <-p.ctx.Done():
				return
			}
			continue
		}

		// Convert to record using the provided parse function
		var record *models.Record
		if p.parseFunc != nil {
			record, err = p.parseFunc(fields)
		} else {
			record = p.defaultParseRecord(fields, chunk.StartRow+i+1)
		}

		if err != nil {
			atomic.AddInt64(&p.errors, 1)
			select {
			case errorChan <- err:
			case <-p.ctx.Done():
				return
			}
			continue
		}

		atomic.AddInt64(&p.rowsParsed, 1)

		// Send record
		select {
		case recordChan <- record:
		case <-p.ctx.Done():
			return
		}
	}
}

// defaultParseRecord creates a record from CSV fields
func (p *ParallelCSVParser) defaultParseRecord(fields []string, rowNum int) *models.Record {
	record := pool.GetRecord()
	record.SetMetadata("row_number", rowNum)

	// Map fields to headers
	for i, value := range fields {
		if i < len(p.headers) {
			fieldName := p.headers[i]
			// Simple type inference
			typedValue := inferFieldType(value)
			record.SetData(fieldName, typedValue)
		} else {
			// Extra fields
			fieldName := stringpool.Sprintf("field_%d", i+1)
			record.SetData(fieldName, value)
		}
	}

	return record
}

// GetMetrics returns parsing metrics
func (p *ParallelCSVParser) GetMetrics() (rowsParsed, errors int64) {
	return atomic.LoadInt64(&p.rowsParsed), atomic.LoadInt64(&p.errors)
}

// Stop stops the parallel parser
func (p *ParallelCSVParser) Stop() {
	p.cancel()
	p.wg.Wait()
}

// inferFieldType performs simple type inference on a string value
func inferFieldType(value string) interface{} {
	value = strings.TrimSpace(value)

	if value == "" {
		return nil
	}

	// Try to parse as integer
	if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
		return intVal
	}

	// Try to parse as float
	if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
		return floatVal
	}

	// Try to parse as boolean
	if boolVal, err := strconv.ParseBool(value); err == nil {
		return boolVal
	}

	// Return as string
	return value
}

// ParallelCSVTransform applies transformations to CSV records in parallel
type ParallelCSVTransform struct {
	transform  func(*models.Record) (*models.Record, error)
	numWorkers int
	logger     *zap.Logger
}

// NewParallelCSVTransform creates a transform that processes records in parallel
func NewParallelCSVTransform(transform func(*models.Record) (*models.Record, error), numWorkers int, logger *zap.Logger) *ParallelCSVTransform {
	if numWorkers == 0 {
		numWorkers = runtime.NumCPU()
	}

	return &ParallelCSVTransform{
		transform:  transform,
		numWorkers: numWorkers,
		logger:     logger,
	}
}

// Process processes records in parallel while maintaining order
func (t *ParallelCSVTransform) Process(ctx context.Context, input <-chan *models.Record) <-chan *models.Record {
	output := make(chan *models.Record, t.numWorkers*100)

	// Create worker pool
	type result struct {
		index  int
		record *models.Record
		err    error
	}

	workChan := make(chan struct {
		index  int
		record *models.Record
	}, t.numWorkers*2)
	resultChan := make(chan result, t.numWorkers*2)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < t.numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for work := range workChan {
				transformed, err := t.transform(work.record)
				resultChan <- result{
					index:  work.index,
					record: transformed,
					err:    err,
				}
			}
		}(i)
	}

	// Input distributor
	go func() {
		index := 0
		for record := range input {
			select {
			case workChan <- struct {
				index  int
				record *models.Record
			}{index, record}:
				index++
			case <-ctx.Done():
				close(workChan)
				return
			}
		}
		close(workChan)
	}()

	// Result collector (maintains order)
	go func() {
		defer close(output)

		// Wait for all workers to finish
		go func() {
			wg.Wait()
			close(resultChan)
		}()

		// Collect results in order
		results := make(map[int]result)
		nextIndex := 0

		for res := range resultChan {
			if res.err != nil {
				t.logger.Error("transform error",
					zap.Int("index", res.index),
					zap.Error(res.err))
				continue
			}

			results[res.index] = res

			// Send all consecutive results
			for {
				if r, ok := results[nextIndex]; ok {
					select {
					case output <- r.record:
						delete(results, nextIndex)
						nextIndex++
					case <-ctx.Done():
						return
					}
				} else {
					break
				}
			}
		}

		// Send any remaining results
		for i := nextIndex; ; i++ {
			if r, ok := results[i]; ok {
				select {
				case output <- r.record:
				case <-ctx.Done():
					return
				}
			} else if len(results) == 0 {
				break
			}
		}
	}()

	return output
}
