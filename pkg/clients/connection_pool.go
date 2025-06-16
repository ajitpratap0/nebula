// Package clients provides connection pool management for HTTP clients
package clients

import (
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// ConnectionPool manages HTTP connection pooling with advanced features including
// connection reuse, health checking, and automatic cleanup of idle connections.
type ConnectionPool struct {
	config *HTTPConfig
	logger *zap.Logger

	// Connection tracking
	connections     map[string]*PooledConnection
	idleConnections map[string][]*PooledConnection
	activeCount     int64
	idleCount       int64
	totalCreated    int64
	totalReused     int64

	// Cleanup
	cleanupTicker *time.Ticker
	stopCh        chan struct{}

	mu sync.RWMutex
}

// PooledConnection represents a pooled HTTP connection with metadata for tracking
// usage, health, and protocol information.
type PooledConnection struct {
	conn      net.Conn
	httpConn  *http.Client
	host      string
	createdAt time.Time
	lastUsed  time.Time
	useCount  int64
	isHTTP2   bool
	isHealthy bool
}

// ConnectionPoolStats provides detailed statistics about the connection pool's
// performance and resource utilization.
type ConnectionPoolStats struct {
	ActiveConnections int64         `json:"active_connections"`
	IdleConnections   int64         `json:"idle_connections"`
	TotalConnections  int64         `json:"total_connections"`
	TotalCreated      int64         `json:"total_created"`
	TotalReused       int64         `json:"total_reused"`
	ReuseRate         float64       `json:"reuse_rate"`
	AverageUseCount   float64       `json:"average_use_count"`
	OldestConnection  time.Duration `json:"oldest_connection_age"`
}

// NewConnectionPool creates a new connection pool with the given configuration.
// It starts a background cleanup goroutine to remove idle connections.
func NewConnectionPool(config *HTTPConfig, logger *zap.Logger) *ConnectionPool {
	pool := &ConnectionPool{
		config:          config,
		logger:          logger.With(zap.String("component", "connection_pool")),
		connections:     make(map[string]*PooledConnection),
		idleConnections: make(map[string][]*PooledConnection),
		stopCh:          make(chan struct{}),
	}

	// Start cleanup goroutine
	pool.cleanupTicker = time.NewTicker(30 * time.Second)
	go pool.cleanupLoop()

	return pool
}

// Get retrieves a connection from the pool for the given host.
// It returns an existing idle connection if available, otherwise creates a new one.
func (cp *ConnectionPool) Get(host string) (*PooledConnection, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	// Check for idle connections for this host
	if idleConns, exists := cp.idleConnections[host]; exists && len(idleConns) > 0 {
		// Get the most recently used connection
		conn := idleConns[len(idleConns)-1]
		cp.idleConnections[host] = idleConns[:len(idleConns)-1]

		// Move from idle to active
		atomic.AddInt64(&cp.idleCount, -1)
		atomic.AddInt64(&cp.activeCount, 1)
		atomic.AddInt64(&cp.totalReused, 1)

		conn.lastUsed = time.Now()
		conn.useCount++

		cp.logger.Debug("reusing connection",
			zap.String("host", host),
			zap.Int64("use_count", conn.useCount),
			zap.Duration("age", time.Since(conn.createdAt)))

		return conn, nil
	}

	// Create new connection
	conn := &PooledConnection{
		host:      host,
		createdAt: time.Now(),
		lastUsed:  time.Now(),
		useCount:  1,
		isHealthy: true,
	}

	atomic.AddInt64(&cp.activeCount, 1)
	atomic.AddInt64(&cp.totalCreated, 1)

	cp.logger.Debug("created new connection",
		zap.String("host", host),
		zap.Int64("active", atomic.LoadInt64(&cp.activeCount)))

	return conn, nil
}

// Put returns a connection to the pool
func (cp *ConnectionPool) Put(conn *PooledConnection) {
	if conn == nil || !conn.isHealthy {
		atomic.AddInt64(&cp.activeCount, -1)
		return
	}

	cp.mu.Lock()
	defer cp.mu.Unlock()

	// Check if we have room for more idle connections
	currentIdleForHost := len(cp.idleConnections[conn.host])
	if currentIdleForHost >= cp.config.MaxIdleConnsPerHost {
		// Close the connection instead of pooling
		if conn.conn != nil {
			conn.conn.Close()
		}
		atomic.AddInt64(&cp.activeCount, -1)
		return
	}

	// Add to idle pool
	cp.idleConnections[conn.host] = append(cp.idleConnections[conn.host], conn)

	// Update counters
	atomic.AddInt64(&cp.activeCount, -1)
	atomic.AddInt64(&cp.idleCount, 1)

	cp.logger.Debug("returned connection to pool",
		zap.String("host", conn.host),
		zap.Int("idle_for_host", currentIdleForHost+1))
}

// MarkUnhealthy marks a connection as unhealthy
func (cp *ConnectionPool) MarkUnhealthy(conn *PooledConnection) {
	if conn != nil {
		conn.isHealthy = false
		cp.logger.Debug("marked connection unhealthy",
			zap.String("host", conn.host),
			zap.Int64("use_count", conn.useCount))
	}
}

// cleanupLoop periodically cleans up idle connections
func (cp *ConnectionPool) cleanupLoop() {
	for {
		select {
		case <-cp.cleanupTicker.C:
			cp.cleanup()
		case <-cp.stopCh:
			return
		}
	}
}

// cleanup removes old idle connections
func (cp *ConnectionPool) cleanup() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	now := time.Now()
	totalCleaned := 0

	for host, conns := range cp.idleConnections {
		remaining := make([]*PooledConnection, 0, len(conns))

		for _, conn := range conns {
			idleTime := now.Sub(conn.lastUsed)

			// Remove connections that have been idle too long
			if idleTime > cp.config.IdleConnTimeout {
				if conn.conn != nil {
					conn.conn.Close()
				}
				atomic.AddInt64(&cp.idleCount, -1)
				totalCleaned++
			} else {
				remaining = append(remaining, conn)
			}
		}

		if len(remaining) > 0 {
			cp.idleConnections[host] = remaining
		} else {
			delete(cp.idleConnections, host)
		}
	}

	if totalCleaned > 0 {
		cp.logger.Info("cleaned up idle connections",
			zap.Int("cleaned", totalCleaned),
			zap.Int64("remaining_idle", atomic.LoadInt64(&cp.idleCount)))
	}
}

// GetStats returns connection pool statistics
func (cp *ConnectionPool) GetStats() ConnectionPoolStats {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	stats := ConnectionPoolStats{
		ActiveConnections: atomic.LoadInt64(&cp.activeCount),
		IdleConnections:   atomic.LoadInt64(&cp.idleCount),
		TotalCreated:      atomic.LoadInt64(&cp.totalCreated),
		TotalReused:       atomic.LoadInt64(&cp.totalReused),
	}

	stats.TotalConnections = stats.ActiveConnections + stats.IdleConnections

	// Calculate reuse rate
	totalUsed := stats.TotalCreated + stats.TotalReused
	if totalUsed > 0 {
		stats.ReuseRate = float64(stats.TotalReused) / float64(totalUsed) * 100
	}

	// Find oldest connection
	var oldestAge time.Duration
	now := time.Now()
	for _, conns := range cp.idleConnections {
		for _, conn := range conns {
			age := now.Sub(conn.createdAt)
			if age > oldestAge {
				oldestAge = age
			}
		}
	}
	stats.OldestConnection = oldestAge

	// Calculate average use count
	var totalUseCount int64
	var connCount int64
	for _, conns := range cp.idleConnections {
		for _, conn := range conns {
			totalUseCount += conn.useCount
			connCount++
		}
	}
	if connCount > 0 {
		stats.AverageUseCount = float64(totalUseCount) / float64(connCount)
	}

	return stats
}

// Close closes all connections in the pool
func (cp *ConnectionPool) Close() {
	close(cp.stopCh)
	cp.cleanupTicker.Stop()

	cp.mu.Lock()
	defer cp.mu.Unlock()

	// Close all idle connections
	for _, conns := range cp.idleConnections {
		for _, conn := range conns {
			if conn.conn != nil {
				conn.conn.Close()
			}
		}
	}

	cp.idleConnections = make(map[string][]*PooledConnection)
	atomic.StoreInt64(&cp.idleCount, 0)

	cp.logger.Info("connection pool closed")
}
