package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/ajitpratap0/nebula/internal/pipeline"
	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/registry"
	"github.com/ajitpratap0/nebula/pkg/logger"

	// Import all available connectors to register them
	_ "github.com/ajitpratap0/nebula/pkg/connector/destinations/csv"
	_ "github.com/ajitpratap0/nebula/pkg/connector/destinations/iceberg"
	_ "github.com/ajitpratap0/nebula/pkg/connector/destinations/json"
	_ "github.com/ajitpratap0/nebula/pkg/connector/sources/csv"
	_ "github.com/ajitpratap0/nebula/pkg/connector/sources/google_ads"
	_ "github.com/ajitpratap0/nebula/pkg/connector/sources/iceberg"
	_ "github.com/ajitpratap0/nebula/pkg/connector/sources/json"
)

var version = "0.1.0"

// SystemFlags contains optional system-level configuration
type SystemFlags struct {
	BatchSize      int           `json:"batch_size"`
	Workers        int           `json:"workers"`
	MaxConcurrency int           `json:"max_concurrency"`
	FlushInterval  time.Duration `json:"flush_interval"`
	Timeout        time.Duration `json:"timeout"`
	LogLevel       string        `json:"log_level"`
	EnableMetrics  bool          `json:"enable_metrics"`
}

// DefaultSystemFlags returns sensible defaults for system configuration
func DefaultSystemFlags() *SystemFlags {
	return &SystemFlags{
		BatchSize:      1000,
		Workers:        runtime.NumCPU(),
		MaxConcurrency: runtime.NumCPU() * 2,
		FlushInterval:  10 * time.Second,
		Timeout:        30 * time.Minute,
		LogLevel:       "info",
		EnableMetrics:  true,
	}
}

func main() {
	// Load .env file if it exists
	_ = godotenv.Load() // Ignore error if .env doesn't exist

	root := &cobra.Command{
		Use:   "nebula",
		Short: "Nebula - High-performance data integration platform",
		Long: `Nebula is a high-performance, cloud-native Extract & Load (EL) data integration platform.
It provides ultra-fast data transfer between various sources and destinations with minimal configuration.`,
	}

	// Version command
	root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Show version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("Nebula v%s\n", version)
			fmt.Printf("Go version: %s\n", runtime.Version())
			fmt.Printf("OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
		},
	})

	// List command to show available connectors
	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List available connectors",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Available Source Connectors:")
			for _, source := range registry.ListSources() {
				fmt.Printf("  - %s\n", source)
			}
			fmt.Println("\nAvailable Destination Connectors:")
			for _, dest := range registry.ListDestinations() {
				fmt.Printf("  - %s\n", dest)
			}
		},
	}
	root.AddCommand(listCmd)

	// Main run command
	var sourceConfigFile, destConfigFile, systemFlagsFile string
	var batchSize, workers, maxConcurrency int
	var timeout, flushInterval time.Duration
	var logLevel string
	var enableMetrics bool

	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Run a data pipeline",
		Long: `Run a data pipeline with the specified source and destination configurations.
Configuration files should be in JSON format containing the connector settings.

Example:
  nebula run --source src-cnf.json --destination dest-cnf.json --system-flags`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPipeline(sourceConfigFile, destConfigFile, systemFlagsFile, &SystemFlags{
				BatchSize:      batchSize,
				Workers:        workers,
				MaxConcurrency: maxConcurrency,
				FlushInterval:  flushInterval,
				Timeout:        timeout,
				LogLevel:       logLevel,
				EnableMetrics:  enableMetrics,
			})
		},
	}

	// Required flags
	runCmd.Flags().StringVarP(&sourceConfigFile, "source", "s", "", "Path to source configuration JSON file (required)")
	runCmd.Flags().StringVarP(&destConfigFile, "destination", "d", "", "Path to destination configuration JSON file (required)")
	_ = runCmd.MarkFlagRequired("source")
	_ = runCmd.MarkFlagRequired("destination")

	// Optional system flags
	runCmd.Flags().StringVar(&systemFlagsFile, "system-flags", "", "Path to system configuration JSON file (optional)")
	runCmd.Flags().IntVar(&batchSize, "batch-size", 1000, "Number of records per batch. Higher values improve throughput but increase memory usage and latency")
	runCmd.Flags().IntVar(&workers, "workers", runtime.NumCPU(), "Number of worker threads for parallel processing. Increase for CPU-bound workloads")
	runCmd.Flags().IntVar(&maxConcurrency, "max-concurrency", runtime.NumCPU()*2, "Maximum concurrent I/O operations. Increase for I/O-bound workloads")
	runCmd.Flags().DurationVar(&timeout, "timeout", 30*time.Minute, "Pipeline timeout")
	runCmd.Flags().DurationVar(&flushInterval, "flush-interval", 10*time.Second, "Time interval for periodic batch flushing (e.g., 1s, 30s, 2m). Lower values reduce latency but may impact throughput")
	runCmd.Flags().StringVar(&logLevel, "log-level", "error", "Log level (debug, info, warn, error)")
	runCmd.Flags().BoolVar(&enableMetrics, "enable-metrics", false, "Enable metrics collection")

	root.AddCommand(runCmd)

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// loadConfigFromFile loads a BaseConfig from a JSON file
func loadConfigFromFile(filename string) (*config.BaseConfig, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", filename, err)
	}

	var cfg config.BaseConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", filename, err)
	}

	return &cfg, nil
}

// loadSystemFlags loads system flags from a JSON file or uses command line flags
func loadSystemFlags(filename string, cmdFlags *SystemFlags) (*SystemFlags, error) {
	if filename == "" {
		return cmdFlags, nil
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read system config file %s: %w", filename, err)
	}

	var sysFlags SystemFlags
	if err := json.Unmarshal(data, &sysFlags); err != nil {
		return nil, fmt.Errorf("failed to parse system config file %s: %w", filename, err)
	}

	// Override with command line flags if they were explicitly set
	if cmdFlags.BatchSize != 1000 {
		sysFlags.BatchSize = cmdFlags.BatchSize
	}
	if cmdFlags.Workers != runtime.NumCPU() {
		sysFlags.Workers = cmdFlags.Workers
	}
	if cmdFlags.MaxConcurrency != runtime.NumCPU()*2 {
		sysFlags.MaxConcurrency = cmdFlags.MaxConcurrency
	}
	if cmdFlags.Timeout != 30*time.Minute {
		sysFlags.Timeout = cmdFlags.Timeout
	}
	if cmdFlags.LogLevel != "info" {
		sysFlags.LogLevel = cmdFlags.LogLevel
	}
	if !cmdFlags.EnableMetrics {
		sysFlags.EnableMetrics = cmdFlags.EnableMetrics
	}

	return &sysFlags, nil
}

// runPipeline executes the data pipeline with the given configurations
func runPipeline(sourceConfigFile, destConfigFile, systemConfigFile string, cmdSystemFlags *SystemFlags) error {
	// Load configurations
	sourceConfig, err := loadConfigFromFile(sourceConfigFile)
	if err != nil {
		return fmt.Errorf("source configuration error: %w", err)
	}

	destConfig, err := loadConfigFromFile(destConfigFile)
	if err != nil {
		return fmt.Errorf("destination configuration error: %w", err)
	}

	systemFlags, err := loadSystemFlags(systemConfigFile, cmdSystemFlags)
	if err != nil {
		return fmt.Errorf("system configuration error: %w", err)
	}

	// Initialize logger
	log := logger.Get().With(
		zap.String("component", "nebula-cli"),
		zap.String("source", sourceConfig.Type),
		zap.String("destination", destConfig.Type),
	)

	log.Info("starting pipeline",
		zap.String("source_config", sourceConfigFile),
		zap.String("dest_config", destConfigFile),
		zap.Int("batch_size", systemFlags.BatchSize),
		zap.Int("workers", systemFlags.Workers),
		zap.Duration("flush_interval", systemFlags.FlushInterval))

	// Apply system flags to connector configurations
	applySystemFlags(sourceConfig, systemFlags)
	applySystemFlags(destConfig, systemFlags)

	// Create source connector
	source, err := registry.CreateSource(sourceConfig.Type, sourceConfig)
	if err != nil {
		return fmt.Errorf("failed to create source connector '%s': %w", sourceConfig.Type, err)
	}

	// Create destination connector
	destination, err := registry.CreateDestination(destConfig.Type, destConfig)
	if err != nil {
		return fmt.Errorf("failed to create destination connector '%s': %w", destConfig.Type, err)
	}

	// Initialize connectors
	ctx, cancel := context.WithTimeout(context.Background(), systemFlags.Timeout)
	defer cancel()

	if err := source.Initialize(ctx, sourceConfig); err != nil {
		return fmt.Errorf("failed to initialize source: %w", err)
	}

	if err := destination.Initialize(ctx, destConfig); err != nil {
		return fmt.Errorf("failed to initialize destination: %w", err)
	}

	// Create pipeline config
	pipelineConfig := &pipeline.PipelineConfig{
		BatchSize:     systemFlags.BatchSize,
		WorkerCount:   systemFlags.Workers,
		FlushInterval: systemFlags.FlushInterval,
	}

	simplePipeline := pipeline.NewSimplePipeline(source, destination, pipelineConfig, log)

	// Run pipeline
	log.Info("executing pipeline")
	startTime := time.Now()

	if err := simplePipeline.Run(ctx); err != nil {
		return fmt.Errorf("pipeline execution failed: %w", err)
	}

	duration := time.Since(startTime)
	metrics := simplePipeline.Metrics()
	recordsProcessed := metrics["records_processed"].(int64)

	log.Info("pipeline completed successfully",
		zap.Duration("duration", duration),
		zap.Int64("records_processed", recordsProcessed),
		zap.Float64("records_per_second", float64(recordsProcessed)/duration.Seconds()))

	// Clean up
	if err := source.Close(ctx); err != nil {
		log.Warn("failed to close source", zap.Error(err))
	}

	if err := destination.Close(ctx); err != nil {
		log.Warn("failed to close destination", zap.Error(err))
	}

	return nil
}

// applySystemFlags applies system-level configuration to connector configs
func applySystemFlags(cfg *config.BaseConfig, flags *SystemFlags) {
	if flags.BatchSize > 0 {
		cfg.Performance.BatchSize = flags.BatchSize
	}
	if flags.Workers > 0 {
		cfg.Performance.Workers = flags.Workers
	}
	if flags.MaxConcurrency > 0 {
		cfg.Performance.MaxConcurrency = flags.MaxConcurrency
	}
	if flags.EnableMetrics {
		cfg.Observability.EnableMetrics = false
	}
}
