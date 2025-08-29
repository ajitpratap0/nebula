// Package sources provides source connectors for data ingestion
package sources

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/pipeline"
	"github.com/ajitpratap0/nebula/pkg/pool"
)

// CSVHybridSource demonstrates hybrid storage mode support
type CSVHybridSource struct {
	config   *config.BaseConfig
	file     *os.File
	reader   *csv.Reader
	storage  *pipeline.StorageAdapter
	headers  []string
	filePath string
}

// NewCSVHybridSource creates a new hybrid CSV source
func NewCSVHybridSource(cfg *config.BaseConfig, filePath string) (core.Source, error) {
	// Validate file path
	if filePath == "" {
		return nil, fmt.Errorf("file path is required")
	}

	// Determine storage mode from configuration
	var storageMode pipeline.StorageMode
	switch cfg.Advanced.StorageMode {
	case "row":
		storageMode = pipeline.StorageModeRow
	case "columnar":
		storageMode = pipeline.StorageModeColumnar
	case "hybrid":
		storageMode = pipeline.StorageModeHybrid
	default:
		storageMode = pipeline.StorageModeHybrid
	}

	// Create storage adapter
	storage := pipeline.NewStorageAdapter(storageMode, cfg)

	return &CSVHybridSource{
		config:   cfg,
		filePath: filePath,
		storage:  storage,
	}, nil
}

// Connect opens the CSV file (internal method)
func (s *CSVHybridSource) Connect(ctx context.Context) error {
	file, err := os.Open(s.filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	s.file = file
	s.reader = csv.NewReader(file)
	s.reader.ReuseRecord = true // Memory optimization

	// Read headers
	headers, err := s.reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read headers: %w", err)
	}
	s.headers = headers

	return nil
}

// Initialize initializes the CSV source
func (s *CSVHybridSource) Initialize(ctx context.Context, cfg *config.BaseConfig) error {
	s.config = cfg
	return nil
}

// Discover discovers the schema
func (s *CSVHybridSource) Discover(ctx context.Context) (*core.Schema, error) {
	if s.headers == nil {
		return nil, fmt.Errorf("headers not available, call Connect first")
	}

	fields := make([]core.Field, len(s.headers))
	for i, header := range s.headers {
		fields[i] = core.Field{
			Name:     header,
			Type:     core.FieldTypeString, // Default to string
			Nullable: true,
		}
	}

	return &core.Schema{
		Name:      "csv_schema",
		Fields:    fields,
		Version:   1,
		CreatedAt: time.Now(),
	}, nil
}

// Read processes the CSV file based on storage mode
func (s *CSVHybridSource) Read(ctx context.Context) (*core.RecordStream, error) {
	if err := s.Connect(ctx); err != nil {
		return nil, err
	}

	recordCh := make(chan *pool.Record, s.config.Performance.BufferSize)
	errorCh := make(chan error, 1)

	go func() {
		defer close(recordCh)
		defer close(errorCh)

		// For columnar mode, we can use direct CSV conversion
		if s.storage.GetStorageMode() == pipeline.StorageModeColumnar {
			s.readColumnar(ctx, recordCh, errorCh)
			return
		}

		// For row or hybrid mode, use traditional record-by-record
		s.readRow(ctx, recordCh, errorCh)
	}()

	return &core.RecordStream{
		Records: (<-chan *pool.Record)(recordCh),
		Errors:  (<-chan error)(errorCh),
	}, nil
}

// ReadBatch reads records in batches
func (s *CSVHybridSource) ReadBatch(ctx context.Context, batchSize int) (*core.BatchStream, error) {
	stream, err := s.Read(ctx)
	if err != nil {
		return nil, err
	}

	batchCh := make(chan []*pool.Record, 10)
	errorCh := make(chan error, 1)

	go func() {
		defer close(batchCh)
		defer close(errorCh)

		batch := make([]*pool.Record, 0, batchSize)

		for {
			select {
			case record, ok := <-stream.Records:
				if !ok {
					if len(batch) > 0 {
						batchCh <- batch
					}
					return
				}
				batch = append(batch, record)
				if len(batch) >= batchSize {
					batchCh <- batch
					batch = make([]*pool.Record, 0, batchSize)
				}
			case err := <-stream.Errors:
				errorCh <- err
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	return &core.BatchStream{
		Batches: (<-chan []*pool.Record)(batchCh),
		Errors:  (<-chan error)(errorCh),
	}, nil
}

// Position management
func (s *CSVHybridSource) GetPosition() core.Position               { return nil }
func (s *CSVHybridSource) SetPosition(position core.Position) error { return nil }
func (s *CSVHybridSource) GetState() core.State                     { return nil }
func (s *CSVHybridSource) SetState(state core.State) error          { return nil }

// Capabilities
func (s *CSVHybridSource) SupportsIncremental() bool { return false }
func (s *CSVHybridSource) SupportsRealtime() bool    { return false }
func (s *CSVHybridSource) SupportsBatch() bool       { return true }

// Subscribe for CDC (not supported)
func (s *CSVHybridSource) Subscribe(ctx context.Context, tables []string) (*core.ChangeStream, error) {
	return nil, fmt.Errorf("CDC not supported for CSV source")
}

// Health check
func (s *CSVHybridSource) Health(ctx context.Context) error {
	if s.file == nil {
		return fmt.Errorf("file not open")
	}
	return nil
}

// Metrics
func (s *CSVHybridSource) Metrics() map[string]interface{} {
	metrics := map[string]interface{}{
		"storage_mode": string(s.storage.GetStorageMode()),
		"record_count": s.storage.GetRecordCount(),
	}
	if s.storage != nil {
		metrics["memory_usage"] = s.storage.GetMemoryUsage()
		metrics["memory_per_record"] = s.storage.GetMemoryPerRecord()
	}
	return metrics
}

// readRow processes in row-based mode
func (s *CSVHybridSource) readRow(ctx context.Context, recordCh chan<- *pool.Record, errorCh chan<- error) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			row, err := s.reader.Read()
			if err == io.EOF {
				return
			}
			if err != nil {
				errorCh <- fmt.Errorf("failed to read row: %w", err)
				return
			}

			// Create record using pool
			record := pool.GetRecord()
			record.Metadata.Source = "csv"

			// Populate data
			for i, value := range row {
				if i < len(s.headers) {
					record.Data[s.headers[i]] = value
				}
			}

			// Add to storage adapter
			if err := s.storage.AddRecord(record); err != nil {
				errorCh <- fmt.Errorf("failed to add record: %w", err)
				record.Release()
				return
			}

			// Send to channel for downstream processing
			select {
			case recordCh <- record:
			case <-ctx.Done():
				record.Release()
				return
			}
		}
	}
}

// readColumnar processes in columnar mode for better memory efficiency
func (s *CSVHybridSource) readColumnar(ctx context.Context, recordCh chan<- *pool.Record, errorCh chan<- error) {
	// In columnar mode, we batch process for efficiency
	batchSize := s.config.Performance.BatchSize
	rowCount := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
			row, err := s.reader.Read()
			if err == io.EOF {
				// Flush any remaining data
				s.storage.Flush()
				return
			}
			if err != nil {
				errorCh <- fmt.Errorf("failed to read row: %w", err)
				return
			}

			// Create temporary record for columnar storage
			record := pool.GetRecord()
			record.Metadata.Source = "csv"

			for i, value := range row {
				if i < len(s.headers) {
					record.Data[s.headers[i]] = value
				}
			}

			// Add to columnar storage
			if err := s.storage.AddRecord(record); err != nil {
				errorCh <- fmt.Errorf("failed to add to columnar: %w", err)
				record.Release()
				return
			}

			record.Release()
			rowCount++

			// Periodically optimize columnar storage
			if rowCount%batchSize == 0 {
				s.storage.OptimizeStorage()
			}
		}
	}
}

// Close closes the file and storage
func (s *CSVHybridSource) Close(ctx context.Context) error {
	if s.file != nil {
		s.file.Close()
	}
	if s.storage != nil {
		s.storage.Close()
	}
	return nil
}

// GetStorageAdapter returns the storage adapter for external access
func (s *CSVHybridSource) GetStorageAdapter() *pipeline.StorageAdapter {
	return s.storage
}

// GetMemoryUsage returns current memory usage
func (s *CSVHybridSource) GetMemoryUsage() int64 {
	return s.storage.GetMemoryUsage()
}

// GetMemoryPerRecord returns average memory per record
func (s *CSVHybridSource) GetMemoryPerRecord() float64 {
	return s.storage.GetMemoryPerRecord()
}

// Example configuration for hybrid CSV source
/*
name: csv_hybrid_source
type: source
performance:
  batch_size: 10000
  buffer_size: 100000
  streaming_mode: false  # Set to true for streaming
advanced:
  storage_mode: columnar  # Options: row, columnar, hybrid
  enable_compression: true
  compression_algorithm: zstd
*/
