package nethexec

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/go-cmp/cmp"
	"github.com/offchainlabs/nitro/arbutil"
	"github.com/offchainlabs/nitro/execution"
	"github.com/offchainlabs/nitro/util/containers"
)

// comparator encapsulates comparison dependencies and provides clean APIs
type comparator struct {
	workerPool   *comparisonWorkerPool
	fatalErrChan chan error
	logger       *slog.Logger
}

// newComparator creates a new comparator with encapsulated dependencies
func newComparator(workerPool *comparisonWorkerPool, fatalErrChan chan error, logger *slog.Logger) *comparator {
	return &comparator{
		workerPool:   workerPool,
		fatalErrChan: fatalErrChan,
		logger:       logger.With("component", "comparator"),
	}
}

func (c *comparator) compareMessageIndex(
	op string,
	internal arbutil.MessageIndex,
	internalErr error,
	external arbutil.MessageIndex,
	externalErr error) error {
	return compare(op, internal, internalErr, external, externalErr, c.fatalErrChan, c.logger)
}

func (c *comparator) compareMessageResultPromise(
	ctx context.Context,
	op string,
	internal containers.PromiseInterface[*execution.MessageResult],
	external containers.PromiseInterface[*execution.MessageResult],
) containers.PromiseInterface[*execution.MessageResult] {
	return comparePromises(ctx, op, internal, external, c.workerPool, c.fatalErrChan, c.logger)
}

func (c *comparator) compareMessageResultsPromise(
	ctx context.Context,
	op string,
	internal containers.PromiseInterface[[]*execution.MessageResult],
	external containers.PromiseInterface[[]*execution.MessageResult],
) containers.PromiseInterface[[]*execution.MessageResult] {
	return comparePromises(ctx, op, internal, external, c.workerPool, c.fatalErrChan, c.logger)
}

func (c *comparator) compareUint64Promise(
	ctx context.Context,
	op string,
	internal containers.PromiseInterface[uint64],
	external containers.PromiseInterface[uint64],
) containers.PromiseInterface[uint64] {
	return comparePromises(ctx, op, internal, external, c.workerPool, c.fatalErrChan, c.logger)
}

func (c *comparator) compareBoolPromise(
	ctx context.Context,
	op string,
	internal containers.PromiseInterface[bool],
	external containers.PromiseInterface[bool],
) containers.PromiseInterface[bool] {
	return comparePromises(ctx, op, internal, external, c.workerPool, c.fatalErrChan, c.logger)
}

func (c *comparator) compareVoidPromise(
	ctx context.Context,
	op string,
	internal containers.PromiseInterface[struct{}],
	external containers.PromiseInterface[struct{}],
) containers.PromiseInterface[struct{}] {
	return comparePromises(ctx, op, internal, external, c.workerPool, c.fatalErrChan, c.logger)
}

func (c *comparator) compareMessageIndexPromise(
	ctx context.Context,
	op string,
	internal containers.PromiseInterface[arbutil.MessageIndex],
	external containers.PromiseInterface[arbutil.MessageIndex],
) containers.PromiseInterface[arbutil.MessageIndex] {
	return comparePromises(ctx, op, internal, external, c.workerPool, c.fatalErrChan, c.logger)
}

func (c *comparator) compareMaintenanceStatusPromise(
	ctx context.Context,
	op string,
	internal containers.PromiseInterface[*execution.MaintenanceStatus],
	external containers.PromiseInterface[*execution.MaintenanceStatus],
) containers.PromiseInterface[*execution.MaintenanceStatus] {
	return comparePromises(ctx, op, internal, external, c.workerPool, c.fatalErrChan, c.logger)
}

func (c *comparator) compareError(
	op string,
	internal error,
	external error) error {
	return compare(op, struct{}{}, internal, struct{}{}, external, c.fatalErrChan, c.logger)
}

// Generic comparison function with type safety
func compare[T any](op string, internal T, internalErr error, external T, externalErr error, fatalErrChan chan error, logger *slog.Logger) error {
	var err error
	switch {
	case internalErr != nil:
		if errors.Is(internalErr, externalErr) {
			return internalErr
		}
		err = &ComparisonError{
			Operation: op,
			Internal:  internalErr,
		}
	case externalErr != nil:
		err = &ComparisonError{
			Operation: op,
			External:  externalErr,
		}
	default:
		if !cmp.Equal(internal, external) {
			opts := cmp.Options{
				cmp.Transformer("HashHex", func(h common.Hash) string { return h.Hex() }),
			}
			diff := cmp.Diff(internal, external, opts)
			err = &ComparisonError{
				Operation: op,
				Diff:      diff,
			}
		}
	}
	if err != nil {
		select {
		case fatalErrChan <- err:
		default:
		}
		logger.Error("Comparison error", "error", err)
	}
	return err
}

func comparePromises[T any](
	ctx context.Context,
	op string,
	internal containers.PromiseInterface[T],
	external containers.PromiseInterface[T],
	workerPool *comparisonWorkerPool,
	fatalErrChan chan error,
	logger *slog.Logger,
) containers.PromiseInterface[T] {
	promise := containers.NewPromise[T](nil)

	// Use worker pool to limit goroutine creation
	select {
	case workerPool.workers <- struct{}{}:
		go func() {
			defer func() { <-workerPool.workers }()

			intRes, intErr := internal.Await(ctx)
			extRes, extErr := external.Await(ctx)

			if err := compare(op, intRes, intErr, extRes, extErr, fatalErrChan, logger); err != nil {
				// Use non-blocking send to avoid goroutine leaks
				select {
				case fatalErrChan <- fmt.Errorf("compareExecutionClient %s: %w", op, err):
					logger.Error("Fatal comparison error", "operation", op, "error", err)
					promise.ProduceError(err)
				default:
					logger.Warn("Non-fatal comparison error", "operation", op, "error", err)
					promise.Produce(intRes)
				}
			} else {
				promise.Produce(intRes)
			}
		}()
	case <-ctx.Done():
		promise.ProduceError(ctx.Err())
	}

	return &promise
}
