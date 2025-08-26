package csv

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"os"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
	"github.com/ajitpratap0/nebula/pkg/models"
)

func init() {
	// Register the V2 CSV source connector as primary
	_ = registry.RegisterSource("csv", NewCSVSource)

	// Register the simple CSV source connector as legacy (deprecated)
	_ = registry.RegisterSource("csv-legacy", NewSimpleCSVSource)

	// Register primary connector info (V2 promoted)
	_ = registry.RegisterConnectorInfo(&registry.ConnectorInfo{
		Name:        "csv",
		Type:        "source",
		Description: "Production-ready CSV file source connector with BaseConnector features",
		Version:     "2.0.0",
		Author:      "Nebula Team",
		Capabilities: []string{
			"streaming",
			"batch",
			"incremental",
			"circuit_breaker",
			"rate_limiting",
			"health_checks",
			"metrics",
			"error_handling",
			"progress_reporting",
			"schema_discovery",
		},
		ConfigSchema: map[string]interface{}{
			"path": map[string]interface{}{
				"type":        "string",
				"required":    true,
				"description": "Path to the CSV file",
			},
			"has_header": map[string]interface{}{
				"type":        "bool",
				"required":    false,
				"default":     true,
				"description": "Whether the file has headers",
			},
		},
	})

	// Register legacy connector info (deprecated)
	_ = registry.RegisterConnectorInfo(&registry.ConnectorInfo{
		Name:        "csv-legacy",
		Type:        "source",
		Description: "DEPRECATED: Simple CSV file source connector (use 'csv' instead)",
		Version:     "1.0.0",
		Author:      "Nebula Team",
		Capabilities: []string{
			"streaming",
			"batch",
		},
		ConfigSchema: map[string]interface{}{
			"path": map[string]interface{}{
				"type":        "string",
				"required":    true,
				"description": "Path to the CSV file",
			},
			"has_header": map[string]interface{}{
				"type":        "bool",
				"required":    false,
				"default":     true,
				"description": "Whether the file has headers",
			},
		},
	})
}

// SimpleCSVSource is a minimal CSV source implementation
type SimpleCSVSource struct {
	config  *config.BaseConfig
	file    *os.File
	reader  *csv.Reader
	headers []string
	schema  *core.Schema
}

// NewSimpleCSVSource creates a new simple CSV source
// DEPRECATED: Use NewCSVSource instead. This will be removed in v3.0.0
func NewSimpleCSVSource(config *config.BaseConfig) (core.Source, error) {
	// Log deprecation warning
	fmt.Printf("WARNING: 'csv-legacy' connector is deprecated. Please use 'csv' connector instead. This legacy connector will be removed in v3.0.0\n")

	return &SimpleCSVSource{
		config: config,
	}, nil
}

// Initialize initializes the CSV source
func (s *SimpleCSVSource) Initialize(ctx context.Context, config *config.BaseConfig) error {
	s.config = config

	// For SimpleCSVSource, file path would need to come from environment or other config
	// This is a simplified implementation - in practice, would need proper config handling
	path := "test.csv" // placeholder - would need proper implementation

	// Open file
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	s.file = file

	// Create CSV reader
	s.reader = csv.NewReader(file)

	// Read headers if present
	hasHeader := true // default for simplified implementation

	if hasHeader {
		headers, err := s.reader.Read()
		if err != nil {
			return fmt.Errorf("failed to read headers: %w", err)
		}
		s.headers = headers

		// Create schema from headers
		fields := make([]core.Field, len(headers))
		for i, header := range headers {
			fields[i] = core.Field{
				Name: header,
				Type: core.FieldTypeString,
			}
		}
		s.schema = &core.Schema{
			Name:   "csv_schema",
			Fields: fields,
		}
	}

	return nil
}

// DiscoverSchema discovers the schema of the CSV file
func (s *SimpleCSVSource) DiscoverSchema(ctx context.Context) (*core.Schema, error) {
	if s.schema != nil {
		return s.schema, nil
	}
	return nil, fmt.Errorf("no schema available")
}

// Read reads records from the CSV file
func (s *SimpleCSVSource) Read(ctx context.Context) (*core.RecordStream, error) {
	recordChan := pool.GetRecordChannel(100)
	defer pool.PutRecordChannel(recordChan)
	errorChan := make(chan error, 1)

	go func() {
		defer close(recordChan)
		defer close(errorChan)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				row, err := s.reader.Read()
				if err != nil {
					if err.Error() != "EOF" {
						errorChan <- err
					}
					return
				}

				// Create record using pooled resources
				record := models.NewRecordFromPool("csv")
				if s.headers != nil {
					for i, header := range s.headers {
						if i < len(row) {
							record.SetData(header, row[i])
						}
					}
				} else {
					for i, value := range row {
						record.SetData(fmt.Sprintf("column_%d", i), value)
					}
				}
				record.SetTimestamp(time.Now())

				select {
				case recordChan <- record:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return &core.RecordStream{
		Records: recordChan,
		Errors:  errorChan,
	}, nil
}

// ReadBatch reads records in batches
func (s *SimpleCSVSource) ReadBatch(ctx context.Context, batchSize int) (*core.BatchStream, error) {
	batchChan := pool.GetBatchChannel()
	defer pool.PutBatchChannel(batchChan)
	errorChan := make(chan error, 1)

	go func() {
		defer close(batchChan)
		defer close(errorChan)

		batch := pool.GetBatchSlice(batchSize)
		defer pool.PutBatchSlice(batch)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				row, err := s.reader.Read()
				if err != nil {
					if err.Error() != "EOF" {
						errorChan <- err
					}
					// Send final batch if any
					if len(batch) > 0 {
						select {
						case batchChan <- batch:
						case <-ctx.Done():
						}
					}
					return
				}

				// Create record using pooled resources
				record := models.NewRecordFromPool("csv")
				if s.headers != nil {
					for i, header := range s.headers {
						if i < len(row) {
							record.SetData(header, row[i])
						}
					}
				} else {
					for i, value := range row {
						record.SetData(fmt.Sprintf("column_%d", i), value)
					}
				}
				record.SetTimestamp(time.Now())

				batch = append(batch, record)

				if len(batch) >= batchSize {
					select {
					case batchChan <- batch:
						// Reset batch - no need to allocate new one
						batch = batch[:0]
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return &core.BatchStream{
		Batches: batchChan,
		Errors:  errorChan,
	}, nil
}

// SaveState saves the current state
func (s *SimpleCSVSource) SaveState(ctx context.Context, state core.State) error {
	return nil // Not implemented for simple version
}

// LoadState loads the saved state
func (s *SimpleCSVSource) LoadState(ctx context.Context) (core.State, error) {
	return nil, nil // Not implemented for simple version
}

// Close closes the CSV source
func (s *SimpleCSVSource) Close(ctx context.Context) error {
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

// SupportsIncremental returns whether the source supports incremental sync
func (s *SimpleCSVSource) SupportsIncremental() bool {
	return false
}

// SupportsBatch returns whether the source supports batch reading
func (s *SimpleCSVSource) SupportsBatch() bool {
	return true
}

// SupportsStreaming returns whether the source supports streaming
func (s *SimpleCSVSource) SupportsStreaming() bool {
	return true
}

// Health checks the health of the source
func (s *SimpleCSVSource) Health(ctx context.Context) error {
	if s.file == nil {
		return fmt.Errorf("file not opened")
	}
	return nil
}

// Metrics returns metrics for the source
func (s *SimpleCSVSource) Metrics() map[string]interface{} {
	return map[string]interface{}{
		"type": "csv",
	}
}

// Discover discovers available streams/tables
func (s *SimpleCSVSource) Discover(ctx context.Context) (*core.Schema, error) {
	// For CSV, return the schema if we have it
	if s.schema != nil {
		return s.schema, nil
	}
	return nil, fmt.Errorf("no schema discovered yet - call Initialize first")
}

// GetPosition returns the current position
func (s *SimpleCSVSource) GetPosition() core.Position {
	return nil // Not implemented for simple version
}

// SetPosition sets the current position
func (s *SimpleCSVSource) SetPosition(position core.Position) error {
	return nil // Not implemented for simple version
}

// GetState returns the current state
func (s *SimpleCSVSource) GetState() core.State {
	return nil // Not implemented for simple version
}

// SetState sets the current state
func (s *SimpleCSVSource) SetState(state core.State) error {
	return nil // Not implemented for simple version
}

// SupportsRealtime returns whether the source supports real-time updates
func (s *SimpleCSVSource) SupportsRealtime() bool {
	return false
}

// Subscribe subscribes to real-time changes
func (s *SimpleCSVSource) Subscribe(ctx context.Context, tables []string) (*core.ChangeStream, error) {
	return nil, fmt.Errorf("real-time subscription not supported for CSV")
}
