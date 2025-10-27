package mysqlcdc

import (
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
	// Register MySQL CDC source connector
	_ = registry.GetRegistry().RegisterSource("mysql-cdc", NewMySQLCDCSource) // Ignore registration error - will fail later if needed
}
