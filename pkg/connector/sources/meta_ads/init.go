// Package metaads provides Meta Ads (Facebook Ads) source connector functionality.
package metaads

import (
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
)

func init() {
	// Register Meta Ads source connector in the global registry
	_ = registry.RegisterSource("meta_ads", func(config *config.BaseConfig) (core.Source, error) {
		return NewMetaAdsSource("meta_ads", config)
	})
}
