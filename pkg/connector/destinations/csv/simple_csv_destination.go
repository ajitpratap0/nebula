package csv

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/models"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
)

// SimpleCSVDestination is a minimal CSV destination implementation
type SimpleCSVDestination struct {
	config            *config.BaseConfig
	file              *os.File
	writer            *csv.Writer
	headers           []string
	schema            *core.Schema
	hasWrittenHeaders bool
}

// NewSimpleCSVDestination creates a new simple CSV destination
// Deprecated: Use NewCSVDestination instead. This will be removed in v3.0.0
func NewSimpleCSVDestination(config *config.BaseConfig) (core.Destination, error) {
	// Log deprecation warning
	fmt.Printf("WARNING: 'csv-legacy' connector is deprecated. Please use 'csv' connector instead. This legacy connector will be removed in v3.0.0\n")

	return &SimpleCSVDestination{
		config: config,
	}, nil
}

// Initialize initializes the CSV destination
func (d *SimpleCSVDestination) Initialize(ctx context.Context, config *config.BaseConfig) error {
	d.config = config

	// For SimpleCSVDestination, file path would need to come from environment or other config
	// This is a simplified implementation - in practice, would need proper config handling
	path := "output.csv" // placeholder - would need proper implementation

	// Create/open file
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	d.file = file

	// Create CSV writer
	d.writer = csv.NewWriter(file)

	return nil
}

// CreateSchema creates a schema in the destination
func (d *SimpleCSVDestination) CreateSchema(ctx context.Context, schema *core.Schema) error {
	d.schema = schema

	// Extract headers from schema
	d.headers = make([]string, len(schema.Fields))
	for i, field := range schema.Fields {
		d.headers[i] = field.Name
	}

	return nil
}

// Write writes records to the CSV file
func (d *SimpleCSVDestination) Write(ctx context.Context, stream *core.RecordStream) error {
	for {
		select {
		case record, ok := <-stream.Records:
			if !ok {
				d.writer.Flush()
				return nil
			}

			// Write headers if not written
			if !d.hasWrittenHeaders && len(d.headers) == 0 {
				// Extract headers from first record
				for key := range record.Data {
					d.headers = append(d.headers, key)
				}
			}

			if !d.hasWrittenHeaders && len(d.headers) > 0 {
				if err := d.writer.Write(d.headers); err != nil {
					return fmt.Errorf("failed to write headers: %w", err)
				}
				d.hasWrittenHeaders = true
			}

			// Write record
			row := make([]string, len(d.headers))
			for i, header := range d.headers {
				if val, ok := record.Data[header]; ok {
					row[i] = stringpool.ValueToString(val)
				}
			}

			if err := d.writer.Write(row); err != nil {
				return fmt.Errorf("failed to write row: %w", err)
			}

		case err := <-stream.Errors:
			return err

		case <-ctx.Done():
			d.writer.Flush()
			return ctx.Err()
		}
	}
}

// WriteBatch writes batches of records to the CSV file
func (d *SimpleCSVDestination) WriteBatch(ctx context.Context, stream *core.BatchStream) error {
	for {
		select {
		case batch, ok := <-stream.Batches:
			if !ok {
				d.writer.Flush()
				return nil
			}

			for _, record := range batch {
				// Write headers if not written
				if !d.hasWrittenHeaders && len(d.headers) == 0 {
					// Extract headers from first record
					for key := range record.Data {
						d.headers = append(d.headers, key)
					}
				}

				if !d.hasWrittenHeaders && len(d.headers) > 0 {
					if err := d.writer.Write(d.headers); err != nil {
						return fmt.Errorf("failed to write headers: %w", err)
					}
					d.hasWrittenHeaders = true
				}

				// Write record
				row := make([]string, len(d.headers))
				for i, header := range d.headers {
					if val, ok := record.Data[header]; ok {
						row[i] = stringpool.ValueToString(val)
					}
				}

				if err := d.writer.Write(row); err != nil {
					return fmt.Errorf("failed to write row: %w", err)
				}
			}

			d.writer.Flush()

		case err := <-stream.Errors:
			return err

		case <-ctx.Done():
			d.writer.Flush()
			return ctx.Err()
		}
	}
}

// Close closes the CSV destination
func (d *SimpleCSVDestination) Close(ctx context.Context) error {
	if d.writer != nil {
		d.writer.Flush()
	}
	if d.file != nil {
		return d.file.Close()
	}
	return nil
}

// SupportsBulkLoad returns whether the destination supports bulk loading
func (d *SimpleCSVDestination) SupportsBulkLoad() bool {
	return false
}

// SupportsTransactions returns whether the destination supports transactions
func (d *SimpleCSVDestination) SupportsTransactions() bool {
	return false
}

// SupportsUpsert returns whether the destination supports upsert
func (d *SimpleCSVDestination) SupportsUpsert() bool {
	return false
}

// SupportsBatch returns whether the destination supports batch writing
func (d *SimpleCSVDestination) SupportsBatch() bool {
	return true
}

// SupportsStreaming returns whether the destination supports streaming
func (d *SimpleCSVDestination) SupportsStreaming() bool {
	return true
}

// BulkLoad performs bulk loading
func (d *SimpleCSVDestination) BulkLoad(ctx context.Context, reader interface{}, format string) error {
	return fmt.Errorf("bulk load not supported")
}

// BeginTransaction begins a transaction
func (d *SimpleCSVDestination) BeginTransaction(ctx context.Context) (core.Transaction, error) {
	return nil, fmt.Errorf("transactions not supported")
}

// Upsert performs an upsert operation
func (d *SimpleCSVDestination) Upsert(ctx context.Context, records []*models.Record, keys []string) error {
	return fmt.Errorf("upsert not supported")
}

// AlterSchema alters the schema
func (d *SimpleCSVDestination) AlterSchema(ctx context.Context, oldSchema, newSchema *core.Schema) error {
	return fmt.Errorf("schema alteration not supported")
}

// DropSchema drops a schema
func (d *SimpleCSVDestination) DropSchema(ctx context.Context, schema *core.Schema) error {
	return fmt.Errorf("schema drop not supported")
}

// Health checks the health of the destination
func (d *SimpleCSVDestination) Health(ctx context.Context) error {
	if d.file == nil {
		return fmt.Errorf("file not opened")
	}
	return nil
}

// Metrics returns metrics for the destination
func (d *SimpleCSVDestination) Metrics() map[string]interface{} {
	return map[string]interface{}{
		"type": "csv",
	}
}
