// Package testutil provides testing utilities for Nebula
package testutil

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// TestLogger creates a test logger that writes to the test output.
// The logger is automatically cleaned up when the test completes.
func TestLogger(t *testing.T) *zap.Logger {
	return zaptest.NewLogger(t)
}

// TestContext creates a test context with a 30-second timeout.
// The caller must call the returned cancel function to avoid leaks.
func TestContext(_ *testing.T) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

// AssertEventually asserts that a condition becomes true within the specified timeout.
// It checks the condition every 10ms until it succeeds or the timeout expires.
func AssertEventually(t *testing.T, condition func() bool, timeout time.Duration, msg string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("condition not met within %v: %s", timeout, msg)
}

// RequireNoError fails the test immediately if err is not nil.
// The msg parameter provides additional context in the failure message.
func RequireNoError(t *testing.T, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", msg, err)
	}
}

// RequireEqual fails the test immediately if expected != actual.
// Note: This uses simple equality comparison and may not work for complex types.
func RequireEqual(t *testing.T, expected, actual interface{}, msg string) {
	t.Helper()
	if expected != actual {
		t.Fatalf("%s: expected %v, got %v", msg, expected, actual)
	}
}
