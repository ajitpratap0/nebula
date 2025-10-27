package iceberg

import (
	"fmt"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	icebergGo "github.com/shubham-tomar/iceberg-go"
	"go.uber.org/zap"
)

// BufferConfig contains configurable constants for Arrow buffer sizing
type BufferConfig struct {
	StringDataMultiplier  int // Default: 32 - estimated chars per string field
	ListElementMultiplier int // Default: 5 - estimated elements per list field
}

// SimpleSchemaValidator validates schema once at initialization
type SimpleSchemaValidator struct {
	icebergSchema *icebergGo.Schema
	arrowSchema   *arrow.Schema
	validated     bool
}

// IcebergDestination is a minimal Iceberg destination connector
type IcebergDestination struct {
	catalogProvider CatalogProvider
	schemaValidator *SimpleSchemaValidator
	builderPool     *ArrowBuilderPool
	bufferConfig    BufferConfig

	// Configuration
	catalogURI  string
	catalogName string
	warehouse   string
	branch      string
	database    string
	tableName   string

	// S3/MinIO configuration
	region     string
	s3Endpoint string
	accessKey  string
	secretKey  string
	properties map[string]string

	logger *zap.Logger
}

type TableResponse struct {
	Metadata struct {
		Schemas []struct {
			SchemaID int `json:"schema-id"`
			Fields   []struct {
				ID       int    `json:"id"`
				Name     string `json:"name"`
				Required bool   `json:"required"`
				Type     string `json:"type"`
			} `json:"fields"`
		} `json:"schemas"`
		CurrentSchemaID int `json:"current-schema-id"`
	} `json:"metadata"`
}

func NewIcebergDestination(config *config.BaseConfig) (core.Destination, error) {
	if config == nil {
		return nil, fmt.Errorf("configuration cannot be nil")
	}

	// Validate required fields
	if config.Security.Credentials == nil {
		return nil, fmt.Errorf("missing required field: credentials")
	}

	catalogURI, exists := config.Security.Credentials["catalog_uri"]
	if !exists || catalogURI == "" {
		return nil, fmt.Errorf("missing required field: catalog_uri")
	}

	// Check for warehouse field
	warehouse, exists := config.Security.Credentials["warehouse"]
	if !exists || warehouse == "" {
		return nil, fmt.Errorf("missing required field: warehouse")
	}

	logger, _ := zap.NewProduction()

	// Create Arrow builder pool for efficient memory reuse
	allocator := memory.NewGoAllocator()
	builderPool := NewArrowBuilderPool(allocator, logger)

	logger.Debug("Created IcebergDestination with builder pool",
		zap.Bool("builder_pool_nil", builderPool == nil))

	return &IcebergDestination{
		properties:      make(map[string]string),
		logger:          logger,
		schemaValidator: &SimpleSchemaValidator{},
		builderPool:     builderPool,
	}, nil
}
