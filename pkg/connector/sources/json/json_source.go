package json

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	gojson "github.com/goccy/go-json"
	nebulaConfig "github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/pool"
)

// JSONFormat represents the JSON file format
type JSONFormat string

const (
	// JSONArray represents a file containing a JSON array of objects
	JSONArray JSONFormat = "array"
	// JSONLines represents line-delimited JSON (JSONL/NDJSON)
	JSONLines JSONFormat = "lines"
)

// JSONSource reads data from JSON files
type JSONSource struct {
	config       *nebulaConfig.BaseConfig
	file         *os.File
	reader       io.Reader
	decoder      *gojson.Decoder
	scanner      *bufio.Scanner
	recordsRead  int64
	bytesRead    int64
	currentState core.State
	schema       *core.Schema
	isArray      bool
	arrayStarted bool
	position     int64
	filePath     string
	format       JSONFormat
	bufferSize   int
	scanBuffer   []byte // Buffer for scanner, returned to pool on close
}

// NewJSONSource creates a new JSON source factory function
func NewJSONSource(config *nebulaConfig.BaseConfig) (core.Source, error) {
	return &JSONSource{
		config:       config,
		currentState: pool.GetMap(), // Use pooled map for state
	}, nil
}

// Initialize initializes the JSON source
func (s *JSONSource) Initialize(ctx context.Context, config *nebulaConfig.BaseConfig) error {
	s.config = config

	// Get file path from credentials
	if config.Security.Credentials == nil || config.Security.Credentials["file_path"] == "" {
		return fmt.Errorf("missing required file path in security.credentials")
	}
	s.filePath = config.Security.Credentials["file_path"]

	// Get format from credentials or use default
	format := JSONLines // default
	if f, ok := config.Security.Credentials["format"]; ok && f != "" {
		format = JSONFormat(f)
	}
	s.format = format
	s.isArray = format == JSONArray

	// Use buffer size from config
	s.bufferSize = config.Performance.BufferSize
	if s.bufferSize <= 0 {
		s.bufferSize = 64 * 1024 // default 64KB
	}

	// Open file
	file, err := os.Open(s.filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	s.file = file

	// Create buffered reader
	s.reader = bufio.NewReaderSize(file, s.bufferSize)

	// Setup reader based on format
	if s.isArray {
		s.decoder = jsonpool.GetDecoder(s.reader)
	} else {
		s.scanner = bufio.NewScanner(s.reader)
		// Use pooled buffer for scanner
		buffer := pool.GlobalBufferPool.Get(s.bufferSize)
		s.scanner.Buffer(buffer[:0], s.bufferSize)
		// Note: We can't return this buffer to the pool while scanner is active
		// Store it for cleanup in Close()
		s.scanBuffer = buffer
	}

	// Update file info in state
	fileInfo, err := file.Stat()
	if err == nil {
		s.currentState["file_size"] = fileInfo.Size()
		s.currentState["file_name"] = fileInfo.Name()
		s.currentState["file_modified"] = fileInfo.ModTime()
	}

	// Create basic schema
	s.schema = &core.Schema{
		Name:        "json_schema",
		Description: "JSON file schema",
		Fields:      []core.Field{}, // Will be populated on first read
	}

	return nil
}

// Discover discovers the schema of the JSON file
func (s *JSONSource) Discover(ctx context.Context) (*core.Schema, error) {
	if s.schema != nil {
		return s.schema, nil
	}
	return nil, fmt.Errorf("no schema available")
}

// Read reads records from the JSON file
func (s *JSONSource) Read(ctx context.Context) (*core.RecordStream, error) {
	recordChan := make(chan *models.Record, 100)
	errorChan := make(chan error, 10)

	stream := &core.RecordStream{
		Records: recordChan,
		Errors:  errorChan,
	}

	// Start reading in background
	go s.readRecords(ctx, recordChan, errorChan)

	return stream, nil
}

// readRecords reads records based on format
func (s *JSONSource) readRecords(ctx context.Context, recordChan chan<- *models.Record, errorChan chan<- error) {
	defer close(recordChan)

	// No logging here to avoid undefined logger

	if s.isArray {
		s.readArrayFormat(ctx, recordChan, errorChan)
	} else {
		s.readLinesFormat(ctx, recordChan, errorChan)
	}

	// Records read: atomic.LoadInt64(&s.recordsRead)
}

// readArrayFormat reads JSON array format
func (s *JSONSource) readArrayFormat(ctx context.Context, recordChan chan<- *models.Record, errorChan chan<- error) {
	// Read opening bracket
	token, err := s.decoder.Token()
	if err != nil {
		errorChan <- fmt.Errorf("failed to read JSON array start: %w", err)
		return
	}

	if delim, ok := token.(gojson.Delim); !ok || delim != '[' {
		errorChan <- fmt.Errorf("expected JSON array, got %v", token)
		return
	}

	// Read array elements
	for s.decoder.More() {
		select {
		case <-ctx.Done():
			return
		default:
			record := models.NewRecordFromPool("json")
			if err := s.decoder.Decode(&record.Data); err != nil {
				record.Release() // Important: release on error
				errorChan <- fmt.Errorf("failed to decode JSON object: %w", err)
				continue
			}
			s.setRecordMetadata(record)

			select {
			case recordChan <- record:
				atomic.AddInt64(&s.recordsRead, 1)
			case <-ctx.Done():
				return
			}
		}
	}
}

// readLinesFormat reads line-delimited JSON format
func (s *JSONSource) readLinesFormat(ctx context.Context, recordChan chan<- *models.Record, errorChan chan<- error) {
	lineNum := 0

	for s.scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
			lineNum++
			line := s.scanner.Bytes()

			// Skip empty lines
			if len(line) == 0 {
				continue
			}

			atomic.AddInt64(&s.bytesRead, int64(len(line)))

			record := models.NewRecordFromPool("json")
			if err := jsonpool.Unmarshal(line, &record.Data); err != nil {
				record.Release() // Important: release on error
				errorChan <- fmt.Errorf("failed to parse JSON on line %d: %w", lineNum, err)
				continue
			}
			s.setRecordMetadata(record)
			record.Metadata.Custom["line_number"] = lineNum

			select {
			case recordChan <- record:
				atomic.AddInt64(&s.recordsRead, 1)
			case <-ctx.Done():
				return
			}
		}
	}

	if err := s.scanner.Err(); err != nil {
		errorChan <- fmt.Errorf("scanner error: %w", err)
	}
}

// setRecordMetadata sets metadata for records using the new structured metadata
func (s *JSONSource) setRecordMetadata(record *models.Record) {
	record.Metadata.Source = "json"
	record.Metadata.Timestamp = time.Now()
	if record.Metadata.Custom == nil {
		record.Metadata.Custom = pool.GetMap()
	}
	record.Metadata.Custom["file_path"] = s.filePath
	record.Metadata.Custom["format"] = string(s.format)
	record.Metadata.Custom["record_num"] = atomic.LoadInt64(&s.recordsRead) + 1
}

// GetState returns the current state
func (s *JSONSource) GetState() core.State {
	state := make(core.State)

	// Copy current state
	for k, v := range s.currentState {
		state[k] = v
	}

	// Add read progress
	state["records_read"] = atomic.LoadInt64(&s.recordsRead)
	state["bytes_read"] = atomic.LoadInt64(&s.bytesRead)

	// Get current file position
	if s.file != nil {
		pos, err := s.file.Seek(0, io.SeekCurrent)
		if err == nil {
			state["file_position"] = pos
			s.position = pos
		}
	}

	return state
}

// SetState sets the connector state (for resuming)
func (s *JSONSource) SetState(state core.State) error {
	// For resuming, we would need to seek to the saved position
	if pos, ok := state["file_position"].(int64); ok && s.file != nil {
		if _, err := s.file.Seek(pos, io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek to position %d: %w", pos, err)
		}

		// Recreate reader/scanner from new position
		s.reader = bufio.NewReaderSize(s.file, s.bufferSize)
		if s.isArray {
			s.decoder = jsonpool.GetDecoder(s.reader)
		} else {
			s.scanner = bufio.NewScanner(s.reader)
			// Get buffer from pool
			s.scanBuffer = pool.GlobalBufferPool.Get(s.bufferSize)
			s.scanner.Buffer(s.scanBuffer[:0], s.bufferSize)
		}
	}

	// Restore counters
	if recordsRead, ok := state["records_read"].(int64); ok {
		atomic.StoreInt64(&s.recordsRead, recordsRead)
	}
	if bytesRead, ok := state["bytes_read"].(int64); ok {
		atomic.StoreInt64(&s.bytesRead, bytesRead)
	}

	s.currentState = state
	return nil
}

// Close closes the JSON file
func (s *JSONSource) Close(ctx context.Context) error {
	// Return decoder to pool if allocated
	if s.decoder != nil {
		jsonpool.PutDecoder(s.decoder)
		s.decoder = nil
	}
	
	// Return buffer to pool if allocated
	if s.scanBuffer != nil {
		pool.GlobalBufferPool.Put(s.scanBuffer)
		s.scanBuffer = nil
	}
	
	if s.file != nil {
		if err := s.file.Close(); err != nil {
			return err
		}
		s.file = nil
	}
	return nil
}

// ReadBatch reads records in batches
func (s *JSONSource) ReadBatch(ctx context.Context, batchSize int) (*core.BatchStream, error) {
	batchChan := make(chan []*models.Record, 10)
	errorChan := make(chan error, 1)

	go func() {
		defer close(batchChan)
		defer close(errorChan)

		if s.isArray {
			s.readArrayBatch(ctx, batchSize, batchChan, errorChan)
		} else {
			s.readLinesBatch(ctx, batchSize, batchChan, errorChan)
		}
	}()

	return &core.BatchStream{
		Batches: batchChan,
		Errors:  errorChan,
	}, nil
}

// readArrayBatch reads batches from array format
func (s *JSONSource) readArrayBatch(ctx context.Context, batchSize int, batchChan chan<- []*models.Record, errorChan chan<- error) {
	// Read opening bracket if not started
	if !s.arrayStarted {
		token, err := s.decoder.Token()
		if err != nil {
			errorChan <- fmt.Errorf("failed to read JSON array start: %w", err)
			return
		}

		if delim, ok := token.(gojson.Delim); !ok || delim != '[' {
			errorChan <- fmt.Errorf("expected JSON array, got %v", token)
			return
		}
		s.arrayStarted = true
	}

	batch := pool.GetBatchSlice(batchSize)


	defer pool.PutBatchSlice(batch)

	for s.decoder.More() {
		select {
		case <-ctx.Done():
			return
		default:
			record := models.NewRecordFromPool("json")
			if err := s.decoder.Decode(&record.Data); err != nil {
				record.Release() // Important: release on error
				errorChan <- fmt.Errorf("failed to decode JSON object: %w", err)
				continue
			}
			s.setRecordMetadata(record)

			batch = append(batch, record)
			atomic.AddInt64(&s.recordsRead, 1)

			if len(batch) >= batchSize {
				select {
				case batchChan <- batch:
					batch = pool.GetBatchSlice(batchSize)
				case <-ctx.Done():
					return
				}
			}
		}
	}

	// Send final batch
	if len(batch) > 0 {
		select {
		case batchChan <- batch:
		case <-ctx.Done():
		}
	}
}

// readLinesBatch reads batches from lines format
func (s *JSONSource) readLinesBatch(ctx context.Context, batchSize int, batchChan chan<- []*models.Record, errorChan chan<- error) {
	batch := pool.GetBatchSlice(batchSize)

	defer pool.PutBatchSlice(batch)
	lineNum := 0

	for s.scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
			lineNum++
			line := s.scanner.Bytes()

			// Skip empty lines
			if len(line) == 0 {
				continue
			}

			atomic.AddInt64(&s.bytesRead, int64(len(line)))

			record := models.NewRecordFromPool("json")
			if err := jsonpool.Unmarshal(line, &record.Data); err != nil {
				record.Release() // Important: release on error
				errorChan <- fmt.Errorf("failed to parse JSON on line %d: %w", lineNum, err)
				continue
			}
			s.setRecordMetadata(record)
			record.Metadata.Custom["line_number"] = lineNum

			batch = append(batch, record)
			atomic.AddInt64(&s.recordsRead, 1)

			if len(batch) >= batchSize {
				select {
				case batchChan <- batch:
					batch = pool.GetBatchSlice(batchSize)
				case <-ctx.Done():
					return
				}
			}
		}
	}

	// Send final batch
	if len(batch) > 0 {
		select {
		case batchChan <- batch:
		case <-ctx.Done():
		}
	}

	if err := s.scanner.Err(); err != nil {
		errorChan <- fmt.Errorf("scanner error: %w", err)
	}
}

// GetPosition returns the current position
func (s *JSONSource) GetPosition() core.Position {
	return &simplePosition{value: fmt.Sprintf("%d", s.position)}
}

// SetPosition sets the current position
func (s *JSONSource) SetPosition(position core.Position) error {
	// Not implemented for simple file reading
	return nil
}

// SupportsIncremental returns true if the source supports incremental sync
func (s *JSONSource) SupportsIncremental() bool {
	return false
}

// SupportsRealtime returns true if the source supports real-time sync
func (s *JSONSource) SupportsRealtime() bool {
	return false
}

// SupportsBatch returns true if the source supports batch reading
func (s *JSONSource) SupportsBatch() bool {
	return true
}

// Subscribe implements CDC subscription (not supported for JSON files)
func (s *JSONSource) Subscribe(ctx context.Context, tables []string) (*core.ChangeStream, error) {
	return nil, fmt.Errorf("CDC not supported for JSON files")
}

// Health performs a health check
func (s *JSONSource) Health(ctx context.Context) error {
	if s.file == nil {
		return fmt.Errorf("file not open")
	}
	return nil
}

// Metrics returns current metrics
func (s *JSONSource) Metrics() map[string]interface{} {
	metrics := pool.GetMap()

	defer pool.PutMap(metrics)

	// Add JSON-specific metrics
	metrics["records_read"] = atomic.LoadInt64(&s.recordsRead)
	metrics["bytes_read"] = atomic.LoadInt64(&s.bytesRead)
	metrics["format"] = string(s.format)
	metrics["file_path"] = s.filePath

	return metrics
}

// simplePosition implements Position interface
type simplePosition struct {
	value string
}

func (p *simplePosition) String() string {
	return p.value
}

func (p *simplePosition) Compare(other core.Position) int {
	// Simple string comparison
	otherStr := other.String()
	if p.value < otherStr {
		return -1
	} else if p.value > otherStr {
		return 1
	}
	return 0
}
