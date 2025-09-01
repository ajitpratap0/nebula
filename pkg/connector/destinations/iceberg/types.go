package iceberg

import (
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	icebergGo "github.com/shubham-tomar/iceberg-go"
	"go.uber.org/zap"
)

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
	logger, _ := zap.NewProduction()

	// Create Arrow builder pool for efficient memory reuse
	allocator := memory.NewGoAllocator()
	builderPool := NewArrowBuilderPool(allocator, logger)

	return &IcebergDestination{
		properties:      make(map[string]string),
		logger:          logger,
		schemaValidator: &SimpleSchemaValidator{},
		builderPool:     builderPool,
	}, nil
}
