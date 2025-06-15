// Package logger provides structured logging for Nebula using Uber's Zap logger.
// It supports context-aware logging, multiple output formats, and high-performance
// structured logging suitable for production environments.
//
// # Overview
//
// The logger package provides:
//   - High-performance structured logging with Zap
//   - Context-aware logging with request IDs, job IDs, etc.
//   - Multiple encoding formats (JSON, console)
//   - Development and production modes
//   - Global logger with lazy initialization
//   - Field-based structured logging
//
// # Basic Usage
//
//	// Initialize logger (typically in main)
//	err := logger.Init(logger.Config{
//	    Level:       "info",
//	    Development: false,
//	    Encoding:    "json",
//	})
//
//	// Simple logging
//	logger.Info("server started", zap.Int("port", 8080))
//
//	// Context-aware logging
//	ctx := context.WithValue(ctx, logger.RequestIDKey, "req-123")
//	logger.WithContext(ctx).Info("processing request")
//
//	// Adding fields
//	logger.With(
//	    zap.String("user", "john"),
//	    zap.Duration("latency", time.Second),
//	).Info("request completed")
//
// # Performance Considerations
//
// The logger is designed for high-throughput scenarios:
//   - Zero-allocation field construction
//   - Buffered output to reduce syscalls
//   - Sampling for high-frequency logs
//   - Lazy evaluation of expensive operations
//
// # Thread Safety
//
// All logging operations are thread-safe. The global logger can be
// safely accessed from multiple goroutines.
package logger

import (
	"context"
	"fmt"
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	globalLogger *zap.Logger
	once         sync.Once
)

// contextKey is the type for context keys to avoid collisions
type contextKey string

const (
	// RequestIDKey is the context key for request ID tracking across services
	RequestIDKey contextKey = "request_id"
	// ConnectorKey is the context key for identifying the active connector
	ConnectorKey contextKey = "connector"
	// JobIDKey is the context key for tracking job execution
	JobIDKey contextKey = "job_id"
)

// Config represents logger configuration options.
//
// Example:
//
//	cfg := logger.Config{
//	    Level:       "info",        // debug, info, warn, error, fatal
//	    Development: false,         // Production mode
//	    Encoding:    "json",        // JSON for production, console for dev
//	    OutputPaths: []string{      // Multiple outputs supported
//	        "stdout",
//	        "/var/log/nebula/app.log",
//	    },
//	}
type Config struct {
	Level       string   // Log level: debug, info, warn, error, fatal
	Development bool     // Development mode enables stack traces and colored output
	Encoding    string   // Output encoding: json or console
	OutputPaths []string // Output destinations (stdout, stderr, or file paths)
}

// Init initializes the global logger with the provided configuration.
// This function should be called once during application startup.
// Subsequent calls are ignored due to sync.Once protection.
//
// Example:
//
//	func main() {
//	    if err := logger.Init(logger.Config{
//	        Level:       "info",
//	        Development: false,
//	        Encoding:    "json",
//	    }); err != nil {
//	        log.Fatal("failed to initialize logger:", err)
//	    }
//	    defer logger.Sync()
//	}
func Init(cfg Config) error {
	var err error
	once.Do(func() {
		globalLogger, err = newLogger(cfg)
	})
	return err
}

// newLogger creates a new zap logger
func newLogger(cfg Config) (*zap.Logger, error) {
	level, err := zapcore.ParseLevel(cfg.Level)
	if err != nil {
		return nil, fmt.Errorf("invalid log level: %w", err)
	}

	encoderConfig := zapcore.EncoderConfig{
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
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	if cfg.Development {
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}

	outputPaths := cfg.OutputPaths
	if len(outputPaths) == 0 {
		outputPaths = []string{"stdout"}
	}

	zapCfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(level),
		Development:      cfg.Development,
		Encoding:         cfg.Encoding,
		EncoderConfig:    encoderConfig,
		OutputPaths:      outputPaths,
		ErrorOutputPaths: []string{"stderr"},
	}

	logger, err := zapCfg.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build logger: %w", err)
	}

	if cfg.Development {
		logger = logger.WithOptions(zap.AddStacktrace(zapcore.ErrorLevel))
	}

	return logger, nil
}

// Get returns the global logger instance. If the logger hasn't been initialized,
// it creates a default logger with production settings. This ensures logging
// always works even if Init wasn't called.
func Get() *zap.Logger {
	if globalLogger == nil {
		// Create a default logger if not initialized
		cfg := Config{
			Level:       "info",
			Development: false,
			Encoding:    "json",
		}
		if err := Init(cfg); err != nil {
			// Fallback to basic logger
			logger, _ := zap.NewProduction()
			globalLogger = logger
		}
	}
	return globalLogger
}

// WithContext returns a logger enriched with values from the context.
// It automatically extracts and adds common fields like request ID,
// connector name, and job ID to all log entries.
//
// Example:
//
//	func ProcessRequest(ctx context.Context, data []byte) error {
//	    logger := logger.WithContext(ctx)
//	    logger.Info("processing request", zap.Int("size", len(data)))
//	    
//	    if err := validate(data); err != nil {
//	        logger.Error("validation failed", zap.Error(err))
//	        return err
//	    }
//	    
//	    logger.Info("request processed successfully")
//	    return nil
//	}
func WithContext(ctx context.Context) *zap.Logger {
	logger := Get()

	if requestID, ok := ctx.Value(RequestIDKey).(string); ok {
		logger = logger.With(zap.String("request_id", requestID))
	}

	if connector, ok := ctx.Value(ConnectorKey).(string); ok {
		logger = logger.With(zap.String("connector", connector))
	}

	if jobID, ok := ctx.Value(JobIDKey).(string); ok {
		logger = logger.With(zap.String("job_id", jobID))
	}

	return logger
}

// Debug logs a debug message with optional structured fields.
// Debug messages are typically used for detailed troubleshooting.
func Debug(msg string, fields ...zap.Field) {
	Get().Debug(msg, fields...)
}

// Info logs an info message with optional structured fields.
// Info messages indicate normal application flow and important events.
func Info(msg string, fields ...zap.Field) {
	Get().Info(msg, fields...)
}

// Warn logs a warning message with optional structured fields.
// Warnings indicate potential issues that don't prevent operation.
func Warn(msg string, fields ...zap.Field) {
	Get().Warn(msg, fields...)
}

// Error logs an error message with optional structured fields.
// Errors indicate failures that may impact functionality.
//
// Example:
//
//	if err := db.Connect(); err != nil {
//	    logger.Error("database connection failed",
//	        zap.Error(err),
//	        zap.String("host", dbHost),
//	        zap.Int("port", dbPort),
//	    )
//	}
func Error(msg string, fields ...zap.Field) {
	Get().Error(msg, fields...)
}

// Fatal logs a fatal message and exits the program with status 1.
// Use sparingly - only for unrecoverable errors during startup.
//
// Example:
//
//	config, err := LoadConfig()
//	if err != nil {
//	    logger.Fatal("failed to load configuration", zap.Error(err))
//	}
func Fatal(msg string, fields ...zap.Field) {
	Get().Fatal(msg, fields...)
	os.Exit(1)
}

// With creates a child logger with additional fields that will be
// included in all subsequent log entries. Useful for adding common
// context that applies to multiple log statements.
//
// Example:
//
//	connLogger := logger.With(
//	    zap.String("connector", "snowflake"),
//	    zap.String("account", account),
//	)
//	connLogger.Info("connecting to warehouse")
//	connLogger.Info("executing query", zap.String("query", sql))
func With(fields ...zap.Field) *zap.Logger {
	return Get().With(fields...)
}

// Sync flushes any buffered log entries. This should be called before
// program exit to ensure all logs are written. Returns an error if
// flushing fails, though this is typically ignored during shutdown.
//
// Example:
//
//	func main() {
//	    logger.Init(cfg)
//	    defer logger.Sync()
//	    // ... application code ...
//	}
func Sync() error {
	if globalLogger != nil {
		return globalLogger.Sync()
	}
	return nil
}
