// Package observability provides comprehensive monitoring, tracing, and logging for Nebula
package observability

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// Global tracer instance
	tracer trace.Tracer

	// Global meter instance
	meter metric.Meter

	// Global logger instance
	logger *zap.Logger

	// Initialization lock
	initOnce sync.Once
)

// TracingConfig contains tracing configuration
type TracingConfig struct {
	ServiceName    string
	ServiceVersion string
	Environment    string
	SamplingRate   float64
	ExporterType   string // "jaeger", "zipkin", "otlp"
	ExporterURL    string
	BatchTimeout   time.Duration
	MaxExportBatch int
	MaxQueueSize   int
}

// MetricsConfig contains metrics configuration
type MetricsConfig struct {
	Namespace       string
	Subsystem       string
	PrometheusPush  bool
	PushGateway     string
	PushInterval    time.Duration
	HistogramBounds []float64
}

// LoggingConfig contains logging configuration
type LoggingConfig struct {
	Level       zapcore.Level
	Format      string // "json", "console"
	OutputPaths []string
	ErrorPaths  []string
	Sampling    *zap.SamplingConfig
	Development bool
}

// ObservabilityConfig contains all observability configuration
type ObservabilityConfig struct {
	Tracing TracingConfig
	Metrics MetricsConfig
	Logging LoggingConfig
}

// Initialize sets up the observability framework
func Initialize(config ObservabilityConfig) error {
	var err error

	initOnce.Do(func() {
		// Initialize tracing
		err = initTracing(config.Tracing)
		if err != nil {
			return
		}

		// Initialize metrics
		err = initMetrics(config.Metrics)
		if err != nil {
			return
		}

		// Initialize logging
		err = initLogging(config.Logging)
		if err != nil {
			return
		}

		// Set up global propagators
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
	})

	return err
}

// GetTracer returns the global tracer
func GetTracer() trace.Tracer {
	return tracer
}

// GetMeter returns the global meter
func GetMeter() metric.Meter {
	return meter
}

// GetLogger returns the global logger
func GetLogger() *zap.Logger {
	return logger
}

// Span represents a tracing span with performance optimizations
type Span struct {
	span       trace.Span
	startTime  time.Time
	attributes []attribute.KeyValue
}

// NewSpan creates a new optimized span
func NewSpan(ctx context.Context, operationName string) (context.Context, *Span) {
	ctx, span := tracer.Start(ctx, operationName)

	return ctx, &Span{
		span:      span,
		startTime: time.Now(),
	}
}

// SetAttribute adds an attribute to the span (batched for performance)
func (s *Span) SetAttribute(key string, value interface{}) {
	var attr attribute.KeyValue

	switch v := value.(type) {
	case string:
		attr = attribute.String(key, v)
	case int:
		attr = attribute.Int(key, v)
	case int64:
		attr = attribute.Int64(key, v)
	case float64:
		attr = attribute.Float64(key, v)
	case bool:
		attr = attribute.Bool(key, v)
	default:
		attr = attribute.String(key, fmt.Sprintf("%v", v))
	}

	s.attributes = append(s.attributes, attr)
}

// AddEvent adds an event to the span
func (s *Span) AddEvent(name string, attrs ...attribute.KeyValue) {
	s.span.AddEvent(name, trace.WithAttributes(attrs...))
}

// SetStatus sets the span status
func (s *Span) SetStatus(code codes.Code, description string) {
	s.span.SetStatus(code, description)
}

// End ends the span and records metrics
func (s *Span) End() {
	// Batch set attributes for performance
	if len(s.attributes) > 0 {
		s.span.SetAttributes(s.attributes...)
	}

	// Record duration metric
	duration := time.Since(s.startTime)
	RecordDuration("span_duration", duration, map[string]string{
		"operation": s.span.SpanContext().SpanID().String(),
	})

	s.span.End()
}

// ConnectorTracer provides connector-specific tracing utilities
type ConnectorTracer struct {
	connectorType string
	connectorName string
	tracer        trace.Tracer
}

// NewConnectorTracer creates a new connector tracer
func NewConnectorTracer(connectorType, connectorName string) *ConnectorTracer {
	return &ConnectorTracer{
		connectorType: connectorType,
		connectorName: connectorName,
		tracer:        tracer,
	}
}

// StartSpan starts a connector-specific span
func (ct *ConnectorTracer) StartSpan(ctx context.Context, operation string) (context.Context, *Span) {
	operationName := fmt.Sprintf("%s.%s.%s", ct.connectorType, ct.connectorName, operation)
	ctx, span := NewSpan(ctx, operationName)

	// Add connector-specific attributes
	span.SetAttribute("connector.type", ct.connectorType)
	span.SetAttribute("connector.name", ct.connectorName)
	span.SetAttribute("connector.operation", operation)

	return ctx, span
}

// TraceRecord traces a record processing operation
func (ct *ConnectorTracer) TraceRecord(ctx context.Context, recordID string, operation string, fn func() error) error {
	ctx, span := ct.StartSpan(ctx, operation)
	defer span.End()

	span.SetAttribute("record.id", recordID)
	span.SetAttribute("record.operation", operation)

	start := time.Now()
	err := fn()
	duration := time.Since(start)

	// Record metrics
	RecordDuration("record_processing_duration", duration, map[string]string{
		"connector_type": ct.connectorType,
		"connector_name": ct.connectorName,
		"operation":      operation,
		"status":         getStatus(err),
	})

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.SetAttribute("error", true)
		span.SetAttribute("error.message", err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return err
}

// TraceBatch traces a batch processing operation
func (ct *ConnectorTracer) TraceBatch(ctx context.Context, batchSize int, operation string, fn func() error) error {
	ctx, span := ct.StartSpan(ctx, operation)
	defer span.End()

	span.SetAttribute("batch.size", batchSize)
	span.SetAttribute("batch.operation", operation)

	start := time.Now()
	err := fn()
	duration := time.Since(start)

	// Calculate throughput
	throughput := float64(batchSize) / duration.Seconds()

	// Record metrics
	RecordDuration("batch_processing_duration", duration, map[string]string{
		"connector_type": ct.connectorType,
		"connector_name": ct.connectorName,
		"operation":      operation,
		"status":         getStatus(err),
	})

	RecordGauge("batch_throughput", throughput, map[string]string{
		"connector_type": ct.connectorType,
		"connector_name": ct.connectorName,
		"operation":      operation,
	})

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.SetAttribute("error", true)
		span.SetAttribute("error.message", err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
		span.SetAttribute("batch.throughput", throughput)
	}

	return err
}

// DistributedTracer handles cross-service tracing
type DistributedTracer struct {
	propagator propagation.TextMapPropagator
}

// NewDistributedTracer creates a new distributed tracer
func NewDistributedTracer() *DistributedTracer {
	return &DistributedTracer{
		propagator: otel.GetTextMapPropagator(),
	}
}

// InjectContext injects tracing context into headers
func (dt *DistributedTracer) InjectContext(ctx context.Context, headers map[string]string) {
	dt.propagator.Inject(ctx, propagation.MapCarrier(headers))
}

// ExtractContext extracts tracing context from headers
func (dt *DistributedTracer) ExtractContext(ctx context.Context, headers map[string]string) context.Context {
	return dt.propagator.Extract(ctx, propagation.MapCarrier(headers))
}

// getStatus returns status string for metrics
func getStatus(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}

// TracingMiddleware provides HTTP middleware for tracing
func TracingMiddleware(serviceName string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			// Extract trace context from headers
			ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(r.Header))

			// Start span
			operationName := fmt.Sprintf("%s %s", r.Method, r.URL.Path)
			ctx, span := tracer.Start(ctx, operationName)
			defer span.End()

			// Add HTTP-specific attributes
			span.SetAttributes(
				attribute.String("http.method", r.Method),
				attribute.String("http.url", r.URL.String()),
				attribute.String("http.user_agent", r.UserAgent()),
				attribute.String("service.name", serviceName),
			)

			// Inject trace context into response headers
			otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(w.Header()))

			// Call next handler
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
