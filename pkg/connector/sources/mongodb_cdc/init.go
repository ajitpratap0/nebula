// Package mongodbcdc provides MongoDB Change Data Capture (CDC) source connector for Nebula
package mongodbcdc

import (
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
	// Register MongoDB CDC source with the registry
	_ = registry.GetRegistry().RegisterSource("mongodb-cdc", NewMongoDBCDCSource) // Ignore registration error - will fail later if needed
}
