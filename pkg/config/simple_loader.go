// Package config provides simple configuration loading
package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Load loads a configuration from a YAML file
func Load(filePath string, config interface{}) error {
	data, err := os.ReadFile(filePath) //nolint:gosec // G304: File path is controlled by caller and validated
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	// Substitute environment variables
	content := string(data)
	content = substituteEnvVars(content)

	if err := yaml.Unmarshal([]byte(content), config); err != nil {
		return fmt.Errorf("failed to parse YAML: %w", err)
	}

	return nil
}

// Save saves a configuration to a YAML file
func Save(filePath string, config interface{}) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal YAML: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil { //nolint:gosec
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// substituteEnvVars replaces ${VAR_NAME} with environment variable values
func substituteEnvVars(content string) string {
	for {
		start := strings.Index(content, "${")
		if start == -1 {
			break
		}
		end := strings.Index(content[start:], "}")
		if end == -1 {
			break
		}
		end += start

		varName := content[start+2 : end]
		envValue := os.Getenv(varName)
		content = content[:start] + envValue + content[end+1:]
	}
	return content
}
