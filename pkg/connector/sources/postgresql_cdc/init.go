package postgresqlcdc

import (
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
	// Register PostgreSQL CDC source connector
	_ = registry.GetRegistry().RegisterSource("postgresql-cdc", NewPostgreSQLCDCSource) // Ignore registration error - will fail later if needed
}
