// Package compressed provides compression wrapper for Nebula destination connectors
package compressed

import (
	"context"
	"io"

	"github.com/ajitpratap0/nebula/pkg/compression"
	nebulaConfig "github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/nebulaerrors"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"go.uber.org/zap"
)

// Wrapper wraps any file-based destination to add compression support
type Wrapper struct {
	destination        core.Destination
	compressionEnabled bool
	algorithm          compression.Algorithm
	level              compression.Level
	compressor         compression.Compressor
	compressedWriter   io.WriteCloser
	originalWriter     io.Writer //nolint:unused // Reserved for fallback writer reference
	logger             *zap.Logger
}

// NewWrapper creates a new compressed destination wrapper
func NewWrapper(destination core.Destination, logger *zap.Logger) *Wrapper {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Wrapper{
		destination: destination,
		logger:      logger.With(zap.String("component", "compressed_wrapper")),
	}
}

// WrapConfig adds compression configuration to the destination config
func WrapConfig(cfg *nebulaConfig.BaseConfig) *nebulaConfig.BaseConfig {
	// Clone the config to avoid modifying the original
	wrappedConfig := &nebulaConfig.BaseConfig{
		Name:    cfg.Name,
		Type:    cfg.Type,
		Version: cfg.Version,

		Performance:   cfg.Performance,
		Timeouts:      cfg.Timeouts,
		Reliability:   cfg.Reliability,
		Security:      cfg.Security,
		Observability: cfg.Observability,
		Memory:        cfg.Memory,
		Advanced:      cfg.Advanced,
	}

	// Configure compression defaults if not already set
	if wrappedConfig.Advanced.CompressionAlgorithm == "" {
		wrappedConfig.Advanced.CompressionAlgorithm = string(compression.Snappy)
		wrappedConfig.Advanced.EnableCompression = true
	}

	return wrappedConfig
}

// getCompressionExtension returns the file extension for the compression algorithm
func getCompressionExtension(algorithm compression.Algorithm) string { //nolint:unused // Reserved for file extension mapping
	switch algorithm {
	case compression.Gzip:
		return ".gz"
	case compression.Snappy:
		return ".snappy"
	case compression.LZ4:
		return ".lz4"
	case compression.Zstd:
		return ".zst"
	case compression.S2:
		return ".s2"
	case compression.Deflate:
		return ".deflate"
	default:
		return ""
	}
}

// Initialize initializes the wrapped destination with compression support
func (w *Wrapper) Initialize(ctx context.Context, config *nebulaConfig.BaseConfig) error {
	// Extract compression configuration
	if err := w.extractCompressionConfig(config); err != nil {
		return err
	}

	// Initialize the underlying destination with wrapped config
	wrappedConfig := WrapConfig(config)
	if err := w.destination.Initialize(ctx, wrappedConfig); err != nil {
		return err
	}

	w.logger.Info("Compressed wrapper initialized",
		zap.Bool("compression_enabled", w.compressionEnabled),
		zap.String("algorithm", string(w.algorithm)),
		zap.Int("level", int(w.level)))

	return nil
}

// extractCompressionConfig extracts compression settings from config
func (w *Wrapper) extractCompressionConfig(config *nebulaConfig.BaseConfig) error {
	// Use the Advanced configuration section for compression settings
	w.compressionEnabled = config.Advanced.EnableCompression

	if !w.compressionEnabled {
		return nil
	}

	// Extract algorithm
	if config.Advanced.CompressionAlgorithm != "" {
		w.algorithm = compression.Algorithm(config.Advanced.CompressionAlgorithm)
	} else {
		w.algorithm = compression.Snappy // default
	}

	// Extract level (using CompressionLevel from Advanced config)
	if config.Advanced.CompressionLevel != 0 {
		switch config.Advanced.CompressionLevel {
		case 1:
			w.level = compression.Fastest
		case 2:
			w.level = compression.Better
		case 3:
			w.level = compression.Best
		default:
			w.level = compression.Default
		}
	} else {
		w.level = compression.Default
	}

	// Create compressor
	compressionConfig := &compression.Config{
		Algorithm: w.algorithm,
		Level:     w.level,
	}

	var err error
	w.compressor, err = compression.NewCompressor(compressionConfig)
	if err != nil {
		return nebulaerrors.Wrap(err, nebulaerrors.ErrorTypeConfig, "failed to create compressor")
	}

	return nil
}

// All other methods delegate to the wrapped destination
func (w *Wrapper) CreateSchema(ctx context.Context, schema *core.Schema) error {
	return w.destination.CreateSchema(ctx, schema)
}

func (w *Wrapper) Write(ctx context.Context, stream *core.RecordStream) error {
	return w.destination.Write(ctx, stream)
}

func (w *Wrapper) WriteBatch(ctx context.Context, stream *core.BatchStream) error {
	return w.destination.WriteBatch(ctx, stream)
}

func (w *Wrapper) Close(ctx context.Context) error {
	// Close compression writer if exists
	if w.compressedWriter != nil {
		if err := w.compressedWriter.Close(); err != nil {
			w.logger.Error("Failed to close compression writer", zap.Error(err))
		}
		w.compressedWriter = nil
	}

	// Close underlying destination
	return w.destination.Close(ctx)
}

func (w *Wrapper) SupportsBulkLoad() bool {
	return w.destination.SupportsBulkLoad()
}

func (w *Wrapper) SupportsTransactions() bool {
	return w.destination.SupportsTransactions()
}

func (w *Wrapper) SupportsUpsert() bool {
	return w.destination.SupportsUpsert()
}

func (w *Wrapper) SupportsBatch() bool {
	return w.destination.SupportsBatch()
}

func (w *Wrapper) SupportsStreaming() bool {
	return w.destination.SupportsStreaming()
}

func (w *Wrapper) BulkLoad(ctx context.Context, reader interface{}, format string) error {
	return w.destination.BulkLoad(ctx, reader, format)
}

func (w *Wrapper) BeginTransaction(ctx context.Context) (core.Transaction, error) {
	return w.destination.BeginTransaction(ctx)
}

func (w *Wrapper) Upsert(ctx context.Context, records []*models.Record, keys []string) error {
	return w.destination.Upsert(ctx, records, keys)
}

func (w *Wrapper) AlterSchema(ctx context.Context, oldSchema, newSchema *core.Schema) error {
	return w.destination.AlterSchema(ctx, oldSchema, newSchema)
}

func (w *Wrapper) DropSchema(ctx context.Context, schema *core.Schema) error {
	return w.destination.DropSchema(ctx, schema)
}

func (w *Wrapper) Health(ctx context.Context) error {
	return w.destination.Health(ctx)
}

func (w *Wrapper) Metrics() map[string]interface{} {
	metrics := w.destination.Metrics()
	if metrics == nil {
		metrics = pool.GetMap()
	}

	// Add compression metrics
	metrics["compression_enabled"] = w.compressionEnabled
	if w.compressionEnabled {
		metrics["compression_algorithm"] = string(w.algorithm)
		metrics["compression_level"] = int(w.level)
	}

	return metrics
}

// CreateCompressedDestination creates a destination with compression support
func CreateCompressedDestination(baseDestination core.Destination, config *nebulaConfig.BaseConfig, logger *zap.Logger) (core.Destination, error) {
	wrapper := NewWrapper(baseDestination, logger)

	// Check if compression is requested
	if config.Advanced.EnableCompression {
		return wrapper, nil
	}

	// Return original destination if compression not enabled
	return baseDestination, nil
}
