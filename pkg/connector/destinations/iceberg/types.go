package iceberg

import (
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"go.uber.org/zap"
)

// IcebergDestination is a minimal Iceberg destination connector
type IcebergDestination struct {
	catalogProvider CatalogProvider

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

	return &IcebergDestination{
		properties: make(map[string]string),
		logger:     logger,
	}, nil
}
