// Package sources provides factory functions for all source connectors
package sources

import (
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"

	// Import all source connectors to trigger init() registration
	_ "github.com/ajitpratap0/nebula/pkg/connector/sources/csv"
	_ "github.com/ajitpratap0/nebula/pkg/connector/sources/google_ads"
	_ "github.com/ajitpratap0/nebula/pkg/connector/sources/json"
	_ "github.com/ajitpratap0/nebula/pkg/connector/sources/meta_ads"
	_ "github.com/ajitpratap0/nebula/pkg/connector/sources/mongodb_cdc"
	_ "github.com/ajitpratap0/nebula/pkg/connector/sources/mysql_cdc"
	_ "github.com/ajitpratap0/nebula/pkg/connector/sources/postgresql"
	_ "github.com/ajitpratap0/nebula/pkg/connector/sources/postgresql_cdc"

	"github.com/ajitpratap0/nebula/pkg/connector/sources/google_ads"
	"github.com/ajitpratap0/nebula/pkg/connector/sources/meta_ads"
)

// NewGoogleAdsSource creates a new Google Ads source connector
// This function is expected by the benchmark tests
func NewGoogleAdsSource(name string, config *config.BaseConfig) (core.Source, error) {
	return google_ads.NewGoogleAdsSource(name, config)
}

// NewMetaAdsSource creates a new Meta Ads source connector
// This function is expected by the benchmark tests
func NewMetaAdsSource(name string, config *config.BaseConfig) (core.Source, error) {
	return meta_ads.NewMetaAdsSource(name, config)
}
