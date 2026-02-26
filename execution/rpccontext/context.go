// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

// Package rpccontext provides RPC request context for header injection.
// This mirrors Nethermind's JsonRpcContext pattern.
package rpccontext

import (
	"context"
	"sync/atomic"
)

// currentRpcContext holds the current RPC request context for HTTP header injection.
// This mirrors Nethermind's JsonRpcContext.Current (AsyncLocal pattern).
// Safe because DigestMessage is protected by createBlocksMutex.
var currentRpcContext atomic.Value

// SetCurrent sets the current RPC context. Called by RPC layer before execution.
func SetCurrent(ctx context.Context) {
	currentRpcContext.Store(ctx)
}

// Current returns the current RPC context, or nil if not set.
func Current() context.Context {
	if ctx, ok := currentRpcContext.Load().(context.Context); ok {
		return ctx
	}
	return nil
}
