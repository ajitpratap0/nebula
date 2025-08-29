package googleads

import (
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
	// Register Google Ads source connector in the global registry
	_ = registry.RegisterSource("google_ads", func(config *config.BaseConfig) (core.Source, error) {
		return NewGoogleAdsSource("google_ads", config)
	})
}
