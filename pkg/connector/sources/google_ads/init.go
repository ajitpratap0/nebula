package google_ads

import (
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
	// Register Google Ads source connector in the global registry
	registry.RegisterSource("google_ads", func(config *config.BaseConfig) (core.Source, error) {
		return NewGoogleAdsSource("google_ads", config)
	})
}
