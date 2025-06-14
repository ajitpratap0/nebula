package base

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// RetryPolicy defines retry behavior
type RetryPolicy struct {
	MaxAttempts     int
	InitialDelay    time.Duration
	MaxDelay        time.Duration
	Multiplier      float64
	RandomizeFactor float64
}

// NewRetryPolicy creates a new retry policy with exponential backoff
func NewRetryPolicy(maxAttempts int, initialDelay time.Duration) *RetryPolicy {
	return &RetryPolicy{
		MaxAttempts:     maxAttempts,
		InitialDelay:    initialDelay,
		MaxDelay:        5 * time.Minute,
		Multiplier:      2.0,
		RandomizeFactor: 0.25,
	}
}

// Execute runs a function with the retry policy
func (rp *RetryPolicy) Execute(ctx context.Context, fn func() error) error {
	var lastErr error

	for attempt := 0; attempt < rp.MaxAttempts; attempt++ {
		// Execute the function
		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		// Don't retry on the last attempt
		if attempt == rp.MaxAttempts-1 {
			break
		}

		// Calculate delay
		delay := rp.calculateDelay(attempt)

		// Wait with context cancellation
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("retry cancelled: %w", ctx.Err())
		case <-timer.C:
			// Continue to next attempt
		}
	}

	return fmt.Errorf("all %d attempts failed: %w", rp.MaxAttempts, lastErr)
}

// ExecuteWithCondition runs a function with retry only if condition is met
func (rp *RetryPolicy) ExecuteWithCondition(ctx context.Context, fn func() error, shouldRetry func(error) bool) error {
	var lastErr error

	for attempt := 0; attempt < rp.MaxAttempts; attempt++ {
		// Execute the function
		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if we should retry
		if !shouldRetry(err) {
			return err
		}

		// Don't retry on the last attempt
		if attempt == rp.MaxAttempts-1 {
			break
		}

		// Calculate delay
		delay := rp.calculateDelay(attempt)

		// Wait with context cancellation
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("retry cancelled: %w", ctx.Err())
		case <-timer.C:
			// Continue to next attempt
		}
	}

	return fmt.Errorf("all %d attempts failed: %w", rp.MaxAttempts, lastErr)
}

// calculateDelay calculates the delay for a given attempt
func (rp *RetryPolicy) calculateDelay(attempt int) time.Duration {
	// Base delay calculation with exponential backoff
	delay := float64(rp.InitialDelay) * math.Pow(rp.Multiplier, float64(attempt))

	// Apply max delay cap
	if delay > float64(rp.MaxDelay) {
		delay = float64(rp.MaxDelay)
	}

	// Apply randomization factor (jitter)
	if rp.RandomizeFactor > 0 {
		delta := delay * rp.RandomizeFactor
		minDelay := delay - delta
		maxDelay := delay + delta

		// Random value between min and max
		delay = minDelay + (rand.Float64() * (maxDelay - minDelay))
	}

	return time.Duration(delay)
}

// GetDelay returns the delay for a specific attempt (for testing/preview)
func (rp *RetryPolicy) GetDelay(attempt int) time.Duration {
	return rp.calculateDelay(attempt)
}

// Clone creates a copy of the retry policy
func (rp *RetryPolicy) Clone() *RetryPolicy {
	return &RetryPolicy{
		MaxAttempts:     rp.MaxAttempts,
		InitialDelay:    rp.InitialDelay,
		MaxDelay:        rp.MaxDelay,
		Multiplier:      rp.Multiplier,
		RandomizeFactor: rp.RandomizeFactor,
	}
}

// WithMaxAttempts returns a new policy with updated max attempts
func (rp *RetryPolicy) WithMaxAttempts(attempts int) *RetryPolicy {
	policy := rp.Clone()
	policy.MaxAttempts = attempts
	return policy
}

// WithDelay returns a new policy with updated delays
func (rp *RetryPolicy) WithDelay(initial, max time.Duration) *RetryPolicy {
	policy := rp.Clone()
	policy.InitialDelay = initial
	policy.MaxDelay = max
	return policy
}

// WithMultiplier returns a new policy with updated multiplier
func (rp *RetryPolicy) WithMultiplier(multiplier float64) *RetryPolicy {
	policy := rp.Clone()
	policy.Multiplier = multiplier
	return policy
}

// WithRandomization returns a new policy with updated randomization
func (rp *RetryPolicy) WithRandomization(factor float64) *RetryPolicy {
	policy := rp.Clone()
	policy.RandomizeFactor = factor
	return policy
}

// DefaultRetryPolicy returns a sensible default retry policy
func DefaultRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		MaxAttempts:     3,
		InitialDelay:    1 * time.Second,
		MaxDelay:        30 * time.Second,
		Multiplier:      2.0,
		RandomizeFactor: 0.25,
	}
}

// AggressiveRetryPolicy returns a policy for critical operations
func AggressiveRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		MaxAttempts:     5,
		InitialDelay:    500 * time.Millisecond,
		MaxDelay:        2 * time.Minute,
		Multiplier:      1.5,
		RandomizeFactor: 0.5,
	}
}

// NoRetryPolicy returns a policy that doesn't retry
func NoRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		MaxAttempts: 1,
	}
}
