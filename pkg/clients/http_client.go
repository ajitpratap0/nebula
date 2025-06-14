// Package clients provides high-performance HTTP client implementations
package clients

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajitpratap0/nebula/pkg/pool"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
)

// HTTPClient provides a high-performance HTTP client with connection pooling
type HTTPClient struct {
	config     *HTTPConfig
	logger     *zap.Logger
	httpClient *http.Client
	transport  *http.Transport

	// Connection pool management
	connectionPool *ConnectionPool
	activeConns    int64
	totalRequests  int64
	failedRequests int64

	// Request/Response pooling
	requestPool  sync.Pool
	responsePool sync.Pool

	// Metrics
	metrics *HTTPMetrics

	// Circuit breaker
	circuitBreaker *HTTPCircuitBreaker

	// Rate limiting
	rateLimiter RateLimiter

	mu sync.RWMutex
}

// HTTPConfig configures the HTTP client
type HTTPConfig struct {
	// Connection settings
	MaxIdleConns        int           `json:"max_idle_conns"`
	MaxIdleConnsPerHost int           `json:"max_idle_conns_per_host"`
	MaxConnsPerHost     int           `json:"max_conns_per_host"`
	IdleConnTimeout     time.Duration `json:"idle_conn_timeout"`
	DisableKeepAlives   bool          `json:"disable_keep_alives"`
	DisableCompression  bool          `json:"disable_compression"`

	// HTTP/2 settings
	EnableHTTP2          bool `json:"enable_http2"`
	MaxConcurrentStreams int  `json:"max_concurrent_streams"`

	// Timeouts
	DialTimeout           time.Duration `json:"dial_timeout"`
	TLSHandshakeTimeout   time.Duration `json:"tls_handshake_timeout"`
	ResponseHeaderTimeout time.Duration `json:"response_header_timeout"`
	RequestTimeout        time.Duration `json:"request_timeout"`
	KeepAlive             time.Duration `json:"keep_alive"`

	// TLS settings
	InsecureSkipVerify bool   `json:"insecure_skip_verify"`
	TLSMinVersion      uint16 `json:"tls_min_version"`

	// Performance settings
	EnableConnectionReuse bool `json:"enable_connection_reuse"`
	EnableRequestPooling  bool `json:"enable_request_pooling"`
	RequestPoolSize       int  `json:"request_pool_size"`

	// Rate limiting
	RateLimit float64 `json:"rate_limit"`
	RateBurst int     `json:"rate_burst"`

	// Circuit breaker
	CircuitBreakerEnabled bool          `json:"circuit_breaker_enabled"`
	FailureThreshold      int           `json:"failure_threshold"`
	SuccessThreshold      int           `json:"success_threshold"`
	Timeout               time.Duration `json:"timeout"`
}

// DefaultHTTPConfig returns optimized default configuration
func DefaultHTTPConfig() *HTTPConfig {
	return &HTTPConfig{
		MaxIdleConns:          1000,
		MaxIdleConnsPerHost:   100,
		MaxConnsPerHost:       100,
		IdleConnTimeout:       90 * time.Second,
		DisableKeepAlives:     false,
		DisableCompression:    false,
		EnableHTTP2:           true,
		MaxConcurrentStreams:  100,
		DialTimeout:           30 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		RequestTimeout:        30 * time.Second,
		KeepAlive:             30 * time.Second,
		InsecureSkipVerify:    false,
		TLSMinVersion:         tls.VersionTLS12,
		EnableConnectionReuse: true,
		EnableRequestPooling:  true,
		RequestPoolSize:       1000,
		RateLimit:             1000.0, // requests per second
		RateBurst:             100,
		CircuitBreakerEnabled: true,
		FailureThreshold:      5,
		SuccessThreshold:      3,
		Timeout:               30 * time.Second,
	}
}

// NewHTTPClient creates a new high-performance HTTP client
func NewHTTPClient(config *HTTPConfig, logger *zap.Logger) *HTTPClient {
	if config == nil {
		config = DefaultHTTPConfig()
	}

	client := &HTTPClient{
		config:  config,
		logger:  logger.With(zap.String("component", "http_client")),
		metrics: NewHTTPMetrics(),
	}

	// Create custom transport with optimizations
	client.transport = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   config.DialTimeout,
			KeepAlive: config.KeepAlive,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          config.MaxIdleConns,
		MaxIdleConnsPerHost:   config.MaxIdleConnsPerHost,
		MaxConnsPerHost:       config.MaxConnsPerHost,
		IdleConnTimeout:       config.IdleConnTimeout,
		DisableKeepAlives:     config.DisableKeepAlives,
		DisableCompression:    config.DisableCompression,
		TLSHandshakeTimeout:   config.TLSHandshakeTimeout,
		ResponseHeaderTimeout: config.ResponseHeaderTimeout,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: config.InsecureSkipVerify,
			MinVersion:         config.TLSMinVersion,
		},
	}

	// Enable HTTP/2 if configured
	if config.EnableHTTP2 {
		if err := http2.ConfigureTransport(client.transport); err != nil {
			logger.Warn("failed to configure HTTP/2", zap.Error(err))
		} else {
			logger.Info("HTTP/2 enabled")
		}
	}

	// Create HTTP client
	client.httpClient = &http.Client{
		Transport: client.transport,
		Timeout:   config.RequestTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}

	// Initialize connection pool
	if config.EnableConnectionReuse {
		client.connectionPool = NewConnectionPool(config, logger)
	}

	// Initialize request/response pools
	if config.EnableRequestPooling {
		client.requestPool = sync.Pool{
			New: func() interface{} {
				return &PooledRequest{
					Headers: make(http.Header),
				}
			},
		}

		client.responsePool = sync.Pool{
			New: func() interface{} {
				return &PooledResponse{
					buffer: pool.GlobalBufferPool.Get(4096)[:0],
				}
			},
		}
	}

	// Initialize rate limiter
	if config.RateLimit > 0 {
		client.rateLimiter = NewTokenBucketRateLimiter(config.RateLimit, config.RateBurst)
	}

	// Initialize circuit breaker
	if config.CircuitBreakerEnabled {
		client.circuitBreaker = NewHTTPCircuitBreaker(config, logger)
	}

	return client
}

// Get performs an HTTP GET request
func (c *HTTPClient) Get(ctx context.Context, url string, headers map[string]string) (*http.Response, error) {
	req, err := c.newRequest(ctx, http.MethodGet, url, nil, headers)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

// Post performs an HTTP POST request
func (c *HTTPClient) Post(ctx context.Context, url string, body io.Reader, headers map[string]string) (*http.Response, error) {
	req, err := c.newRequest(ctx, http.MethodPost, url, body, headers)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

// Put performs an HTTP PUT request
func (c *HTTPClient) Put(ctx context.Context, url string, body io.Reader, headers map[string]string) (*http.Response, error) {
	req, err := c.newRequest(ctx, http.MethodPut, url, body, headers)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

// Delete performs an HTTP DELETE request
func (c *HTTPClient) Delete(ctx context.Context, url string, headers map[string]string) (*http.Response, error) {
	req, err := c.newRequest(ctx, http.MethodDelete, url, nil, headers)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

// Do performs an HTTP request with all optimizations
func (c *HTTPClient) Do(req *http.Request) (*http.Response, error) {
	// Apply rate limiting
	if c.rateLimiter != nil {
		if err := c.rateLimiter.Wait(req.Context()); err != nil {
			atomic.AddInt64(&c.failedRequests, 1)
			return nil, fmt.Errorf("rate limit exceeded: %w", err)
		}
	}

	// Check circuit breaker
	if c.circuitBreaker != nil && !c.circuitBreaker.Allow() {
		atomic.AddInt64(&c.failedRequests, 1)
		return nil, fmt.Errorf("circuit breaker open")
	}

	// Track request
	atomic.AddInt64(&c.totalRequests, 1)
	start := time.Now()

	// Perform request
	resp, err := c.httpClient.Do(req)

	// Update metrics
	duration := time.Since(start)
	c.metrics.RecordRequest(req.Method, req.URL.Host, duration, err)

	if err != nil {
		atomic.AddInt64(&c.failedRequests, 1)
		if c.circuitBreaker != nil {
			c.circuitBreaker.RecordFailure()
		}
		return nil, err
	}

	// Record success
	if c.circuitBreaker != nil {
		c.circuitBreaker.RecordSuccess()
	}

	// Update connection metrics
	if resp != nil && resp.TLS != nil {
		c.metrics.RecordConnectionReuse(resp.TLS.DidResume)
	}

	return resp, nil
}

// newRequest creates a new HTTP request with pooling support
func (c *HTTPClient) newRequest(ctx context.Context, method, url string, body io.Reader, headers map[string]string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}

	// Apply headers
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	// Set default headers for performance
	if req.Header.Get("Accept-Encoding") == "" && !c.config.DisableCompression {
		req.Header.Set("Accept-Encoding", "gzip, deflate")
	}

	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "Nebula-HTTPClient/1.0")
	}

	// Enable keep-alive
	if !c.config.DisableKeepAlives {
		req.Header.Set("Connection", "keep-alive")
	}

	return req, nil
}

// GetStats returns current client statistics
func (c *HTTPClient) GetStats() HTTPStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	activeConns := atomic.LoadInt64(&c.activeConns)
	totalRequests := atomic.LoadInt64(&c.totalRequests)
	failedRequests := atomic.LoadInt64(&c.failedRequests)

	stats := HTTPStats{
		ActiveConnections: activeConns,
		TotalRequests:     totalRequests,
		FailedRequests:    failedRequests,
		SuccessRate:       0.0,
		ConnectionReuse:   0.0,
		AverageLatency:    c.metrics.GetAverageLatency(),
		P95Latency:        c.metrics.GetP95Latency(),
		P99Latency:        c.metrics.GetP99Latency(),
	}

	if totalRequests > 0 {
		stats.SuccessRate = float64(totalRequests-failedRequests) / float64(totalRequests) * 100
	}

	if c.connectionPool != nil {
		poolStats := c.connectionPool.GetStats()
		stats.ConnectionReuse = poolStats.ReuseRate
		stats.IdleConnections = poolStats.IdleConnections
		stats.TotalConnections = poolStats.TotalConnections
	}

	return stats
}

// Close closes the HTTP client and releases resources
func (c *HTTPClient) Close() error {
	c.logger.Info("closing HTTP client")

	if c.connectionPool != nil {
		c.connectionPool.Close()
	}

	c.transport.CloseIdleConnections()

	return nil
}

// HTTPStats represents HTTP client statistics
type HTTPStats struct {
	ActiveConnections int64         `json:"active_connections"`
	IdleConnections   int64         `json:"idle_connections"`
	TotalConnections  int64         `json:"total_connections"`
	TotalRequests     int64         `json:"total_requests"`
	FailedRequests    int64         `json:"failed_requests"`
	SuccessRate       float64       `json:"success_rate"`
	ConnectionReuse   float64       `json:"connection_reuse_rate"`
	AverageLatency    time.Duration `json:"average_latency"`
	P95Latency        time.Duration `json:"p95_latency"`
	P99Latency        time.Duration `json:"p99_latency"`
}

// PooledRequest represents a pooled HTTP request
type PooledRequest struct {
	Method  string
	URL     string
	Headers http.Header
	Body    io.Reader
}

// PooledResponse represents a pooled HTTP response
type PooledResponse struct {
	StatusCode int
	Headers    http.Header
	buffer     []byte
	Body       io.ReadCloser
}

// Reset resets the pooled request for reuse
func (pr *PooledRequest) Reset() {
	pr.Method = ""
	pr.URL = ""
	pr.Body = nil
	for k := range pr.Headers {
		delete(pr.Headers, k)
	}
}

// Reset resets the pooled response for reuse
func (pr *PooledResponse) Reset() {
	pr.StatusCode = 0
	pr.Body = nil
	// Return buffer to pool and get a new one
	if pr.buffer != nil {
		pool.GlobalBufferPool.Put(pr.buffer)
		pr.buffer = pool.GlobalBufferPool.Get(4096)[:0]
	}
	for k := range pr.Headers {
		delete(pr.Headers, k)
	}
}
