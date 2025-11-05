package aerospike

import (
	"context"
	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// Note on testing context-based methods:
// The context-based methods (putWithCtx, operateWithCtx, etc.) in the Statement struct
// all follow the same pattern:
// 1. If the base policy is nil, use a default policy from the client
// 2. Clone the policy with context-based timeout using clonePolicyWithContext
// 3. Call the corresponding client method with the cloned policy
//
// Since we've already thoroughly tested the clonePolicyWithContext function,
// we can be confident that these methods will correctly set the timeout in the policy
// if they use this function properly.
//
// Testing these methods directly would require mocking the Aerospike client,
// which is complex due to the large number of methods in the client interface.
// Instead, we focus on testing the core functionality (deriveTimeoutFromContext and
// clonePolicyWithContext) that these methods rely on.

func Test_deriveTimeoutFromContext(t *testing.T) {
	testCases := []struct {
		description string
		ctx         context.Context
		expected    time.Duration
	}{
		{
			description: "context without deadline",
			ctx:         context.Background(),
			expected:    0,
		},
		{
			description: "context with future deadline",
			ctx:         contextWithDeadline(time.Now().Add(5 * time.Second)),
			expected:    5 * time.Second,
		},
		{
			description: "context with past deadline",
			ctx:         contextWithDeadline(time.Now().Add(-5 * time.Second)),
			expected:    time.Millisecond,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// Allow for small timing differences
			result := deriveTimeoutFromContext(tc.ctx)

			if tc.description == "context with future deadline" {
				// For future deadlines, just check that the result is close to expected
				assert.True(t, result > 0, "Expected positive timeout")
				assert.True(t, result <= tc.expected, "Expected timeout to be less than or equal to deadline")
			} else {
				// For other cases, exact comparison is fine
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func Test_clonePolicyWithContext(t *testing.T) {
	testCases := []struct {
		description string
		ctx         context.Context
		policy      as.Policy
		expectClone bool
	}{
		{
			description: "context without deadline",
			ctx:         context.Background(),
			policy:      as.NewWritePolicy(0, 0),
			expectClone: false,
		},
		{
			description: "context with deadline - WritePolicy",
			ctx:         contextWithDeadline(time.Now().Add(5 * time.Second)),
			policy:      as.NewWritePolicy(0, 0),
			expectClone: true,
		},
		{
			description: "context with deadline - BatchPolicy",
			ctx:         contextWithDeadline(time.Now().Add(5 * time.Second)),
			policy:      as.NewBatchPolicy(),
			expectClone: true,
		},
		{
			description: "context with deadline - QueryPolicy",
			ctx:         contextWithDeadline(time.Now().Add(5 * time.Second)),
			policy:      as.NewQueryPolicy(),
			expectClone: true,
		},
		{
			description: "context with deadline - BasePolicy",
			ctx:         contextWithDeadline(time.Now().Add(5 * time.Second)),
			policy:      as.NewPolicy(),
			expectClone: true,
		},
		{
			description: "context with deadline - ScanPolicy",
			ctx:         contextWithDeadline(time.Now().Add(5 * time.Second)),
			policy:      as.NewScanPolicy(),
			expectClone: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			result := clonePolicyWithContext(tc.ctx, tc.policy)

			if tc.expectClone {
				assert.NotEqual(t, tc.policy, result, "Expected a new policy instance")

				// Check that timeout was set correctly
				timeout := deriveTimeoutFromContext(tc.ctx)
				var passed bool

				switch p := result.(type) {
				case *as.WritePolicy:
					passed = (timeout > p.TotalTimeout-time.Millisecond) // Allow for small timing differences
					assert.True(t, passed, tc.description)
				case *as.BatchPolicy:
					passed = (timeout > p.TotalTimeout-time.Millisecond) // Allow for small timing differences
					assert.True(t, passed, tc.description)
				case *as.QueryPolicy:
					passed = (timeout > p.TotalTimeout-time.Millisecond) // Allow for small timing differences
					assert.True(t, passed, tc.description)
				case *as.BasePolicy:
					passed = (timeout > p.TotalTimeout-time.Millisecond) // Allow for small timing differences
					assert.True(t, passed, tc.description)
				case *as.ScanPolicy:
					passed = (timeout > p.TotalTimeout-time.Millisecond) // Allow for small timing differences
					assert.True(t, passed, tc.description)
				default:
					t.Fatalf("Unexpected policy type: %T", p)
				}
			} else {
				assert.Equal(t, tc.policy, result, "Expected the same policy instance")
			}
		})
	}
}

// Helper function to create a context with a deadline
func contextWithDeadline(deadline time.Time) context.Context {
	ctx, _ := context.WithDeadline(context.Background(), deadline)
	return ctx
}
