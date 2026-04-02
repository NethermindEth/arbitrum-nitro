// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md
package comparison

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"

	"github.com/offchainlabs/nitro/util/containers"
)

var cmpOpts = cmp.Options{
	cmp.Transformer("HashHex", func(h common.Hash) string { return h.Hex() }),
	cmp.Comparer(func(x, y *big.Int) bool {
		if x == nil && y == nil {
			return true
		}
		if x == nil || y == nil {
			return false
		}
		return x.Cmp(y) == 0
	}),
	cmpopts.EquateEmpty(),
}

func pickResult[T any](intRes, extRes T, selectResult []func(T, T) T) T {
	if len(selectResult) > 0 && selectResult[0] != nil {
		return selectResult[0](intRes, extRes)
	}
	return intRes
}

func comparePromises[T any](fatalErrChan chan error, op string,
	internal containers.PromiseInterface[T],
	external containers.PromiseInterface[T],
	selectResult ...func(intRes, extRes T) T,
) containers.PromiseInterface[T] {
	promise := containers.NewPromise[T](nil)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		var intRes, extRes T
		var intErr, extErr error
		var internalDuration, externalDuration time.Duration

		internalStart := time.Now()
		externalStart := time.Now()

		internalDone := false
		externalDone := false

		for !internalDone || !externalDone {
			select {
			case <-internal.ReadyChan():
				if !internalDone {
					intRes, intErr = internal.Current()
					internalDuration = time.Since(internalStart)
					internalDone = true
				}
			case <-external.ReadyChan():
				if !externalDone {
					extRes, extErr = external.Current()
					externalDuration = time.Since(externalStart)
					externalDone = true
				}
			case <-ctx.Done():
				promise.ProduceError(fmt.Errorf("comparison timeout for %s: %w", op, ctx.Err()))
				return
			}
		}

		metrics.GetOrRegisterHistogram("arb/compare/internal/"+op+"/duration", nil, metrics.NewBoundedHistogramSample()).Update(internalDuration.Nanoseconds())
		metrics.GetOrRegisterHistogram("arb/compare/external/"+op+"/duration", nil, metrics.NewBoundedHistogramSample()).Update(externalDuration.Nanoseconds())

		if err := compareResults(op, intRes, intErr, extRes, extErr); err != nil {
			select {
			case fatalErrChan <- fmt.Errorf("compareExecutionClient %s: %s", op, err.Error()):
				promise.ProduceError(err)
			default:
				log.Error("Non-fatal comparison error", "operation", op, "err", err)
				promise.Produce(pickResult(intRes, extRes, selectResult))
			}
		} else {
			promise.Produce(pickResult(intRes, extRes, selectResult))
		}
	}()
	return &promise
}

func compareResults[T any](op string, intRes T, intErr error, extRes T, extErr error) error {
	switch {
	case intErr != nil && extErr != nil:
		if intErr.Error() != extErr.Error() {
			return fmt.Errorf("both operations failed with different errors: internal=%v external=%v", intErr, extErr)
		}
		return nil
	case intErr != nil && extErr == nil:
		return fmt.Errorf("internal operation failed but external succeeded: internal=%v", intErr)
	case intErr == nil && extErr != nil:
		return fmt.Errorf("external operation failed but internal succeeded: external=%v", extErr)
	default:
		if !cmp.Equal(intRes, extRes, cmpOpts...) {
			diff := cmp.Diff(intRes, extRes, cmpOpts...)
			fmt.Printf("ERROR: Execution mismatch detected in operation: %s\n", op)
			fmt.Printf("Diff details:\n%s\n", diff)
			return fmt.Errorf("execution mismatch in %s", op)
		}
	}
	return nil
}
