package observability

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// initTracing initializes the tracing provider
func initTracing(config TracingConfig) error {
	// Create resource
	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(config.ServiceName),
			semconv.ServiceVersionKey.String(config.ServiceVersion),
			semconv.DeploymentEnvironmentKey.String(config.Environment),
		),
	)
	if err != nil {
		return fmt.Errorf("failed to create resource: %w", err)
	}

	// Create exporter based on type
	var exporter sdktrace.SpanExporter
	switch config.ExporterType {
	case "stdout":
		exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return fmt.Errorf("failed to create stdout exporter: %w", err)
		}
	default:
		// Default to stdout for development
		exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return fmt.Errorf("failed to create stdout exporter: %w", err)
		}
	}

	// Configure sampling
	var sampler sdktrace.Sampler
	if config.SamplingRate <= 0 {
		sampler = sdktrace.NeverSample()
	} else if config.SamplingRate >= 1.0 {
		sampler = sdktrace.AlwaysSample()
	} else {
		sampler = sdktrace.TraceIDRatioBased(config.SamplingRate)
	}

	// Create trace provider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(config.BatchTimeout),
			sdktrace.WithMaxExportBatchSize(config.MaxExportBatch),
			sdktrace.WithMaxQueueSize(config.MaxQueueSize),
		),
	)

	// Set global tracer provider
	otel.SetTracerProvider(tp)

	// Create global tracer
	tracer = tp.Tracer(config.ServiceName)

	return nil
}

// initMetrics initializes the metrics provider
func initMetrics(config MetricsConfig) error {
	// For now, we'll use a no-op meter since we already have Prometheus metrics
	// In the future, this could be enhanced with OpenTelemetry metrics
	meter = otel.Meter(config.Namespace)
	return nil
}

// initLogging initializes the structured logging
func initLogging(config LoggingConfig) error {
	// Build logger config
	logConfig := zap.Config{
		Level:       zap.NewAtomicLevelAt(config.Level),
		Development: config.Development,
		Sampling:    config.Sampling,
		Encoding:    config.Format,
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "timestamp",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			FunctionKey:    zapcore.OmitKey,
			MessageKey:     "message",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:      config.OutputPaths,
		ErrorOutputPaths: config.ErrorPaths,
	}

	// Set defaults if not provided
	if len(logConfig.OutputPaths) == 0 {
		logConfig.OutputPaths = []string{"stdout"}
	}
	if len(logConfig.ErrorOutputPaths) == 0 {
		logConfig.ErrorOutputPaths = []string{"stderr"}
	}

	// Build logger
	var err error
	logger, err = logConfig.Build()
	if err != nil {
		return fmt.Errorf("failed to build logger: %w", err)
	}

	// Replace global logger
	zap.ReplaceGlobals(logger)

	return nil
}

// DefaultConfig returns a default observability configuration
func DefaultConfig() ObservabilityConfig {
	return ObservabilityConfig{
		Tracing: TracingConfig{
			ServiceName:    "nebula",
			ServiceVersion: "1.0.0",
			Environment:    getEnv("ENVIRONMENT", "development"),
			SamplingRate:   0.1, // 10% sampling
			ExporterType:   getEnv("TRACING_EXPORTER", "stdout"),
			ExporterURL:    getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
			BatchTimeout:   5 * time.Second,
			MaxExportBatch: 512,
			MaxQueueSize:   2048,
		},
		Metrics: MetricsConfig{
			Namespace:       "nebula",
			Subsystem:       "core",
			PrometheusPush:  false,
			PushGateway:     getEnv("PROMETHEUS_PUSHGATEWAY", ""),
			PushInterval:    30 * time.Second,
			HistogramBounds: []float64{0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0},
		},
		Logging: LoggingConfig{
			Level:       getLogLevel(getEnv("LOG_LEVEL", "info")),
			Format:      getEnv("LOG_FORMAT", "json"),
			OutputPaths: []string{"stdout"},
			ErrorPaths:  []string{"stderr"},
			Development: getEnv("ENVIRONMENT", "development") == "development",
		},
	}
}

// getEnv gets environment variable with default
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getLogLevel converts string to zap log level
func getLogLevel(level string) zapcore.Level {
	switch level {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	case "fatal":
		return zapcore.FatalLevel
	default:
		return zapcore.InfoLevel
	}
}

// Shutdown gracefully shuts down all observability components
func Shutdown(ctx context.Context) error {
	var errors []error

	// Shutdown tracer
	if tp, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); ok {
		if err := tp.Shutdown(ctx); err != nil {
			errors = append(errors, fmt.Errorf("failed to shutdown tracer: %w", err))
		}
	}

	// Sync logger
	if logger != nil {
		if err := logger.Sync(); err != nil {
			// Ignore sync errors for stdout/stderr/stdin
			// These are common in tests and when output is redirected
			// See: https://github.com/uber-go/zap/issues/328
			errStr := err.Error()
			if !strings.Contains(errStr, "bad file descriptor") &&
				!strings.Contains(errStr, "invalid argument") &&
				!strings.Contains(errStr, "/dev/stdout") &&
				!strings.Contains(errStr, "/dev/stderr") {
				errors = append(errors, fmt.Errorf("failed to sync logger: %w", err))
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("shutdown errors: %v", errors)
	}

	return nil
}
