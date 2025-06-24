package aerospike

import (
	"context"
	as "github.com/aerospike/aerospike-client-go/v6"
	"time"
)

// deriveTimeoutFromContext returns the timeout duration to use in policies.
func deriveTimeoutFromContext(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0 // no timeout
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return time.Millisecond
	}
	return remaining
}

// clonePolicyWithContext returns a copy of any policy type with TotalTimeout set from context.
// It uses the Policy interface to access the BasePolicy and set the TotalTimeout.
func clonePolicyWithContext(ctx context.Context, policy as.Policy) as.Policy {
	ctxTimeout := deriveTimeoutFromContext(ctx)
	if ctxTimeout == 0 {
		return policy // no timeout, return original policy
	}

	// Handle specific policy types
	switch p := policy.(type) {
	case *as.WritePolicy:
		result := *p // shallow copy
		result.TotalTimeout = ctxTimeout
		return &result
	case *as.BatchPolicy:
		result := *p // shallow copy
		result.TotalTimeout = ctxTimeout
		return &result
	case *as.QueryPolicy:
		result := *p // shallow copy
		result.TotalTimeout = ctxTimeout
		return &result
	case *as.BasePolicy:
		result := *p // shallow copy
		result.TotalTimeout = ctxTimeout
		return &result
	case *as.ScanPolicy:
		result := *p // shallow copy
		result.TotalTimeout = ctxTimeout
		return &result
	default:
		// This should never happen with the current policy types
		return policy
	}
}

// putWithCtx wraps Put with context-based timeout.
func (s *Statement) putWithCtx(ctx context.Context, base *as.WritePolicy, key *as.Key, bins as.BinMap) error {
	if base == nil {
		base = s.client.DefaultWritePolicy
	}
	policy := clonePolicyWithContext(ctx, base).(*as.WritePolicy)
	return s.client.Put(policy, key, bins)
}

// operateWithCtx wraps Operate with context-based timeout.
func (s *Statement) operateWithCtx(ctx context.Context, base *as.WritePolicy, key *as.Key, ops []*as.Operation) (*as.Record, as.Error) {
	if base == nil {
		base = s.client.DefaultWritePolicy
	}
	policy := clonePolicyWithContext(ctx, base).(*as.WritePolicy)
	return s.client.Operate(policy, key, ops...)
}

// getWithCtx performs a Get operation with context-based timeout support.
func (s *Statement) getWithCtx(ctx context.Context, basePolicy *as.BasePolicy, key *as.Key, binNames []string) (*as.Record, error) {
	if basePolicy == nil {
		basePolicy = s.client.DefaultPolicy
	}
	policy := clonePolicyWithContext(ctx, basePolicy).(*as.BasePolicy)
	return s.client.Get(policy, key, binNames...)
}

func (s *Statement) dropIndexWithCtx(ctx context.Context, basePolicy *as.WritePolicy, schema, table, name string) error {
	if basePolicy == nil {
		basePolicy = s.client.DefaultWritePolicy
	}
	policy := clonePolicyWithContext(ctx, basePolicy).(*as.WritePolicy)
	return s.client.DropIndex(policy, schema, table, name)
}

func (s *Statement) createIndexWithCtx(ctx context.Context, basePolicy *as.WritePolicy, namespace string, setName string, indexName string, binName string, indexType as.IndexType) (*as.IndexTask, error) {
	if basePolicy == nil {
		basePolicy = s.client.DefaultWritePolicy
	}
	policy := clonePolicyWithContext(ctx, basePolicy).(*as.WritePolicy)
	return s.client.CreateIndex(policy, namespace, setName, indexName, binName, indexType)
}

func (s *Statement) batchGetWithCtx(ctx context.Context, basePolicy *as.BatchPolicy, keys []*as.Key, binNames []string) ([]*as.Record, as.Error) {
	if basePolicy == nil {
		basePolicy = s.client.DefaultBatchPolicy
	}

	policy := clonePolicyWithContext(ctx, basePolicy).(*as.BatchPolicy)
	return s.client.BatchGet(policy, keys, binNames...)
}

func (s *Statement) queryWithCtx(ctx context.Context, basePolicy *as.QueryPolicy, statement *as.Statement) (*as.Recordset, as.Error) {
	if basePolicy == nil {
		basePolicy = s.client.DefaultQueryPolicy
	}

	policy := clonePolicyWithContext(ctx, basePolicy).(*as.QueryPolicy)
	return s.client.Query(policy, statement)
}

func (s *Statement) scanAllWithCtx(ctx context.Context, basePolicy *as.ScanPolicy, namespace string, setName string, binNames []string) (*as.Recordset, as.Error) {
	if basePolicy == nil {
		basePolicy = s.client.DefaultScanPolicy
	}

	policy := clonePolicyWithContext(ctx, basePolicy).(*as.ScanPolicy)
	return s.client.ScanAll(policy, namespace, setName, binNames...)
}

func (s *Statement) truncateWithCtx(ctx context.Context, basePolicy *as.WritePolicy, namespace string, setName string, beforeLastUpdate *time.Time) as.Error {
	if basePolicy == nil {
		basePolicy = s.client.DefaultWritePolicy
	}

	policy := clonePolicyWithContext(ctx, basePolicy).(*as.WritePolicy)
	return s.client.Truncate(policy, namespace, setName, beforeLastUpdate)
}
