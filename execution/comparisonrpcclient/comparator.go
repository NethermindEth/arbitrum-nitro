package comparisonrpcclient

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/log"
	"github.com/google/go-cmp/cmp"

	"github.com/offchainlabs/nitro/arbutil"
	"github.com/offchainlabs/nitro/execution"
	"github.com/offchainlabs/nitro/util/containers"
)

// Comparator handles comparison of execution results between primary and secondary clients
type Comparator struct {
	fatalErrChan chan<- error
}

// NewComparator creates a new Comparator
func NewComparator(fatalErrChan chan<- error) *Comparator {
	return &Comparator{fatalErrChan: fatalErrChan}
}

// compare uses cmp.Diff for deep comparison, returns error if not equal
func compare[T any](primary, secondary T) error {
	if diff := cmp.Diff(primary, secondary); diff != "" {
		return errors.New(diff)
	}
	return nil
}

// handleMismatch logs mismatch and sends error to fatalErrChan
func (c *Comparator) handleMismatch(method string, msgIdx *arbutil.MessageIndex, err error, primary, secondary any, primaryErr, secondaryErr error) {
	if msgIdx != nil {
		log.Error("Execution client comparison mismatch",
			"method", method,
			"msgIdx", *msgIdx,
			"diff", err,
			"primary.error", primaryErr,
			"secondary.error", secondaryErr,
		)
	} else {
		log.Error("Execution client comparison mismatch",
			"method", method,
			"diff", err,
			"primary.error", primaryErr,
			"secondary.error", secondaryErr,
		)
	}
	if c.fatalErrChan != nil {
		if msgIdx != nil {
			c.fatalErrChan <- fmt.Errorf("%w in %s at msgIdx %d:\n%s", ErrMismatch, method, *msgIdx, err)
		} else {
			c.fatalErrChan <- fmt.Errorf("%w in %s:\n%s", ErrMismatch, method, err)
		}
	}
}

// comparePromises awaits two promises, compares results AND errors using cmp
// Sends to fatalErrChan on mismatch, always returns a primary result as a new promise
// Note: This is a standalone function because Go doesn't support generic methods
func comparePromises[T any](
	ctx context.Context,
	c *Comparator,
	method string,
	msgIdx *arbutil.MessageIndex,
	primary containers.PromiseInterface[T],
	secondary containers.PromiseInterface[T],
) containers.PromiseInterface[T] {
	return containers.DoPromise(ctx, func(ctx context.Context) (T, error) {
		primaryResult, primaryErr := primary.Await(ctx)
		secondaryResult, secondaryErr := secondary.Await(ctx)

		// Compare errors
		if err := compare(primaryErr, secondaryErr); err != nil {
			c.handleMismatch(method, msgIdx, err, primaryResult, secondaryResult, primaryErr, secondaryErr)
		}

		// Compare results (only if both succeeded)
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				c.handleMismatch(method, msgIdx, err, primaryResult, secondaryResult, primaryErr, secondaryErr)
			}
		}

		// Always return a primary result
		return primaryResult, primaryErr
	})
}

// CompareMessageResult compares *execution.MessageResult promises
func (c *Comparator) CompareMessageResult(
	ctx context.Context,
	method string,
	msgIdx arbutil.MessageIndex,
	primary, secondary containers.PromiseInterface[*execution.MessageResult],
) containers.PromiseInterface[*execution.MessageResult] {
	return comparePromises(ctx, c, method, &msgIdx, primary, secondary)
}

// CompareMessageResults compares []*execution.MessageResult promises
func (c *Comparator) CompareMessageResults(
	ctx context.Context,
	method string,
	msgIdx arbutil.MessageIndex,
	primary, secondary containers.PromiseInterface[[]*execution.MessageResult],
) containers.PromiseInterface[[]*execution.MessageResult] {
	return comparePromises(ctx, c, method, &msgIdx, primary, secondary)
}

// CompareMessageIndex compares arbutil.MessageIndex promises
func (c *Comparator) CompareMessageIndex(
	ctx context.Context,
	method string,
	primary, secondary containers.PromiseInterface[arbutil.MessageIndex],
) containers.PromiseInterface[arbutil.MessageIndex] {
	return comparePromises(ctx, c, method, nil, primary, secondary)
}

// CompareEmpty compares only errors for void methods (struct{} results are always equal)
func (c *Comparator) CompareEmpty(
	ctx context.Context,
	method string,
	primary, secondary containers.PromiseInterface[struct{}],
) containers.PromiseInterface[struct{}] {
	return containers.DoPromise(ctx, func(ctx context.Context) (struct{}, error) {
		_, primaryErr := primary.Await(ctx)
		_, secondaryErr := secondary.Await(ctx)

		if err := compare(primaryErr, secondaryErr); err != nil {
			c.handleMismatch(method, nil, err, struct{}{}, struct{}{}, primaryErr, secondaryErr)
		}

		return struct{}{}, primaryErr
	})
}

// CompareEmptyWithMsgIdx compares only errors for void methods that have a message index
func (c *Comparator) CompareEmptyWithMsgIdx(
	ctx context.Context,
	method string,
	msgIdx arbutil.MessageIndex,
	primary, secondary containers.PromiseInterface[struct{}],
) containers.PromiseInterface[struct{}] {
	return containers.DoPromise(ctx, func(ctx context.Context) (struct{}, error) {
		_, primaryErr := primary.Await(ctx)
		_, secondaryErr := secondary.Await(ctx)

		if err := compare(primaryErr, secondaryErr); err != nil {
			c.handleMismatch(method, &msgIdx, err, struct{}{}, struct{}{}, primaryErr, secondaryErr)
		}

		return struct{}{}, primaryErr
	})
}

// CompareBool compares bool promises
func (c *Comparator) CompareBool(
	ctx context.Context,
	method string,
	primary, secondary containers.PromiseInterface[bool],
) containers.PromiseInterface[bool] {
	return comparePromises(ctx, c, method, nil, primary, secondary)
}

// CompareMaintenanceStatus compares *execution.MaintenanceStatus promises
func (c *Comparator) CompareMaintenanceStatus(
	ctx context.Context,
	method string,
	primary, secondary containers.PromiseInterface[*execution.MaintenanceStatus],
) containers.PromiseInterface[*execution.MaintenanceStatus] {
	return comparePromises(ctx, c, method, nil, primary, secondary)
}

// CompareUint64 compares uint64 promises
func (c *Comparator) CompareUint64(
	ctx context.Context,
	method string,
	msgIdx arbutil.MessageIndex,
	primary, secondary containers.PromiseInterface[uint64],
) containers.PromiseInterface[uint64] {
	return comparePromises(ctx, c, method, &msgIdx, primary, secondary)
}

// CompareRecordResult compares *execution.RecordResult promises
func (c *Comparator) CompareRecordResult(
	ctx context.Context,
	method string,
	msgIdx arbutil.MessageIndex,
	primary, secondary containers.PromiseInterface[*execution.RecordResult],
) containers.PromiseInterface[*execution.RecordResult] {
	return comparePromises(ctx, c, method, &msgIdx, primary, secondary)
}
