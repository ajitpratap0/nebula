// Package logger provides structured logging for Nebula
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

// contextKey is the type for context keys
type contextKey string

const (
	// RequestIDKey is the context key for request ID
	RequestIDKey contextKey = "request_id"
	// ConnectorKey is the context key for connector name
	ConnectorKey contextKey = "connector"
	// JobIDKey is the context key for job ID
	JobIDKey contextKey = "job_id"
)

// Config represents logger configuration
type Config struct {
	Level       string
	Development bool
	Encoding    string // json or console
	OutputPaths []string
}

// Init initializes the global logger
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

// Get returns the global logger
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

// WithContext returns a logger with context values
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

// Debug logs a debug message
func Debug(msg string, fields ...zap.Field) {
	Get().Debug(msg, fields...)
}

// Info logs an info message
func Info(msg string, fields ...zap.Field) {
	Get().Info(msg, fields...)
}

// Warn logs a warning message
func Warn(msg string, fields ...zap.Field) {
	Get().Warn(msg, fields...)
}

// Error logs an error message
func Error(msg string, fields ...zap.Field) {
	Get().Error(msg, fields...)
}

// Fatal logs a fatal message and exits
func Fatal(msg string, fields ...zap.Field) {
	Get().Fatal(msg, fields...)
	os.Exit(1)
}

// With creates a child logger with additional fields
func With(fields ...zap.Field) *zap.Logger {
	return Get().With(fields...)
}

// Sync flushes any buffered log entries
func Sync() error {
	if globalLogger != nil {
		return globalLogger.Sync()
	}
	return nil
}
