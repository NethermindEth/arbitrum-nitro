package comparisonrpcclient

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/ethereum/go-ethereum/arbitrum/multigas"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"

	"github.com/offchainlabs/nitro/arbutil"
	"github.com/offchainlabs/nitro/execution"
	executionrpcclient "github.com/offchainlabs/nitro/execution/rpcclient"
	"github.com/offchainlabs/nitro/util/containers"
)

// cmpOptions makes common.Hash, common.Address, *big.Int, and multigas.MultiGas atomic comparison units
var cmpOptions = cmp.Options{
	cmp.Comparer(func(x, y common.Hash) bool { return x == y }),
	cmp.Comparer(func(x, y common.Address) bool { return x == y }),
	cmp.Comparer(func(x, y *big.Int) bool {
		if x == nil || y == nil {
			return x == y
		}
		return x.Cmp(y) == 0
	}),
	cmp.Comparer(func(x, y multigas.MultiGas) bool {
		// Compare all resource kinds, total, and refund
		for kind := multigas.ResourceKind(1); kind < multigas.NumResourceKind; kind++ {
			if x.Get(kind) != y.Get(kind) {
				return false
			}
		}
		return x.SingleGas() == y.SingleGas() && x.GetRefund() == y.GetRefund()
	}),
}

// Comparator handles comparison of execution results between primary and secondary clients
type Comparator struct {
	fatalErrChan   chan<- error
	primary        *executionrpcclient.Client
	secondary      *executionrpcclient.Client
	compareHeaders bool // Whether to compare response headers
	receiptTimeout time.Duration

	// Block notification system for subscription-based waiting
	blockWaitersMu sync.Mutex
	blockWaiters   map[common.Hash][]chan struct{} // block hash -> channels waiting for it
}

// NewComparator creates a new Comparator
func NewComparator(fatalErrChan chan<- error, primary, secondary *executionrpcclient.Client, receiptTimeout time.Duration) *Comparator {
	return &Comparator{
		fatalErrChan:   fatalErrChan,
		primary:        primary,
		secondary:      secondary,
		compareHeaders: primary.CapturedHeaders() != nil && secondary.CapturedHeaders() != nil,
		receiptTimeout: receiptTimeout,
		blockWaiters:   make(map[common.Hash][]chan struct{}),
	}
}

// waitForBlock registers interest in a block hash and returns a channel that will be closed
// when the block is notified as available. The caller must call unregisterBlockWaiter when done.
func (c *Comparator) waitForBlock(hash common.Hash) <-chan struct{} {
	ch := make(chan struct{})
	c.blockWaitersMu.Lock()
	c.blockWaiters[hash] = append(c.blockWaiters[hash], ch)
	c.blockWaitersMu.Unlock()
	return ch
}

// unregisterBlockWaiter removes a specific waiter channel for a block hash.
func (c *Comparator) unregisterBlockWaiter(hash common.Hash, ch <-chan struct{}) {
	c.blockWaitersMu.Lock()
	defer c.blockWaitersMu.Unlock()
	waiters := c.blockWaiters[hash]
	for i, w := range waiters {
		if w == ch {
			c.blockWaiters[hash] = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}
	if len(c.blockWaiters[hash]) == 0 {
		delete(c.blockWaiters, hash)
	}
}

// NotifyBlock notifies all waiters that a block with the given hash is now available.
// This should be called when a new block head is received via subscription.
func (c *Comparator) NotifyBlock(hash common.Hash) {
	c.blockWaitersMu.Lock()
	waiters := c.blockWaiters[hash]
	delete(c.blockWaiters, hash)
	c.blockWaitersMu.Unlock()

	for _, ch := range waiters {
		close(ch)
	}
}

// compareResponseHeaders compares the captured X-Arb-* headers from both clients.
// Returns an error if there are mismatches.
func (c *Comparator) compareResponseHeaders(method string, msgIdx *arbutil.MessageIndex) {
	if !c.compareHeaders {
		return
	}

	primaryHeaders := c.primary.CapturedHeaders().All()
	secondaryHeaders := c.secondary.CapturedHeaders().All()

	// Build diff
	var diffs []string

	// Check primary headers against secondary
	for key, primaryVal := range primaryHeaders {
		secondaryVal, exists := secondaryHeaders[key]
		if !exists {
			diffs = append(diffs, fmt.Sprintf("  header %s present in primary (%s) but missing in secondary", key, primaryVal))
		} else if primaryVal != secondaryVal {
			diffs = append(diffs, fmt.Sprintf("  header %s: primary=%s, secondary=%s", key, primaryVal, secondaryVal))
		}
	}

	// Check for headers in secondary but not primary
	for key, secondaryVal := range secondaryHeaders {
		if _, exists := primaryHeaders[key]; !exists {
			diffs = append(diffs, fmt.Sprintf("  header %s present in secondary (%s) but missing in primary", key, secondaryVal))
		}
	}

	if len(diffs) > 0 {
		sort.Strings(diffs)
		diffErr := errors.New(strings.Join(diffs, "\n"))
		report := MismatchReport{
			Method: method + " (response headers)",
			MsgIdx: msgIdx,
			Diff:   diffErr,
		}
		printMismatchReport(report)
		// Note: header mismatches are logged but don't trigger a fatal error
		// since the primary result is still returned
		log.Warn("Response header mismatch detected", "method", method, "diffs", len(diffs))
	}
}

// compare uses cmp.Equal with a custom Reporter for clean, readable diffs
func compare[T any](primary, secondary T) error {
	var reporter diffReporter
	if cmp.Equal(primary, secondary, cmpOptions, cmp.Reporter(&reporter)) {
		return nil
	}
	return errors.New(reporter.String())
}

// isShutdownError checks if an error is related to shutdown (context cancellation or StopWaiter stopped)
// These errors should not be reported as comparison mismatches since they're expected during test cleanup
func isShutdownError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	// Check error message for shutdown-related errors
	errMsg := err.Error()
	// "stopped" is produced by StopWaiter when the component is shutting down
	if errMsg == "stopped" {
		return true
	}
	// Check for "context canceled" in various forms (direct, wrapped, RPC errors)
	return errMsg == "context canceled" ||
		strings.Contains(errMsg, "context canceled")
}

// compareErrors compares two errors by their message strings only.
// If ctx is cancelled (shutdown in progress), skip comparison entirely - any error
// mismatch during shutdown is expected due to race conditions between the two clients.
func compareErrors(ctx context.Context, primary, secondary error) error {
	// During shutdown, skip all error comparisons - mismatches are expected
	if ctx.Err() != nil {
		return nil
	}
	if primary == nil && secondary == nil {
		return nil
	}
	// If either error is a shutdown error, skip comparison - we're in the process of stopping
	if isShutdownError(primary) || isShutdownError(secondary) {
		return nil
	}
	if (primary == nil) != (secondary == nil) {
		//nolint:errorlint // intentionally not wrapping - comparing two independent errors
		return fmt.Errorf("error presence mismatch:\n  - %v\n  + %v", primary, secondary)
	}
	if primary.Error() != secondary.Error() {
		return fmt.Errorf("error message mismatch:\n  - %q\n  + %q", primary.Error(), secondary.Error())
	}
	return nil
}

// CompareWithExpected compares the secondary result with an expected result from the primary
// This is used when UseInternalSequencer is true - the primary already processed the block,
// so we only forward to secondary and compare with the expected result.
func (c *Comparator) CompareWithExpected(
	ctx context.Context,
	method string,
	msgIdx arbutil.MessageIndex,
	expectedResult *execution.MessageResult,
	secondary containers.PromiseInterface[*execution.MessageResult],
) (*execution.MessageResult, error) {
	secondaryResult, secondaryErr := secondary.Await(ctx)

	if secondaryErr != nil {
		// Don't report context cancellation as a mismatch - this happens during shutdown
		// when the node's StopWaiter context is canceled but async ForwardToSecondary
		// goroutines are still running
		if isShutdownError(secondaryErr) {
			log.Debug("Ignoring context cancellation in comparison", "method", method, "msgIdx", msgIdx)
			return expectedResult, secondaryErr
		}
		report := MismatchReport{
			Method:       method,
			MsgIdx:       &msgIdx,
			Diff:         fmt.Errorf("secondary error: %w", secondaryErr),
			SecondaryErr: secondaryErr,
		}
		printMismatchReport(report)
		sendFatalError(report, c.fatalErrChan)
		return expectedResult, secondaryErr
	}

	if err := compare(expectedResult, secondaryResult); err != nil {
		log.Debug("COMPARISON FAILED: block result mismatch", "method", method, "msgIdx", msgIdx)
		report := MismatchReport{Method: method, MsgIdx: &msgIdx, Diff: err}
		printMismatchReport(report)
		c.compareBlock(ctx, expectedResult, secondaryResult, msgIdx)
		sendFatalError(report, c.fatalErrChan)
	} else {
		log.Debug("COMPARISON PASSED: block result match", "method", method, "msgIdx", msgIdx, "blockHash", expectedResult.BlockHash.Hex())
		// Block hashes match - also compare receipts to validate MultiGasUsed
		// (MultiGasUsed is not part of consensus encoding, so block hash doesn't validate it)
		c.compareReceiptsForResult(ctx, method, msgIdx, expectedResult)
	}

	return expectedResult, nil
}

// CompareMessageResult compares *execution.MessageResult promises with block comparison on mismatch
func (c *Comparator) CompareMessageResult(
	ctx context.Context,
	method string,
	msgIdx arbutil.MessageIndex,
	primary, secondary containers.PromiseInterface[*execution.MessageResult],
) containers.PromiseInterface[*execution.MessageResult] {
	return containers.DoPromise(ctx, func(ctx context.Context) (*execution.MessageResult, error) {
		primaryResult, primaryErr := primary.Await(ctx)
		secondaryResult, secondaryErr := secondary.Await(ctx)

		// Compare response headers (X-Arb-*)
		c.compareResponseHeaders(method, &msgIdx)

		if err := compareErrors(ctx, primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, MsgIdx: &msgIdx, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan)
		}

		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				log.Debug("COMPARISON FAILED: block result mismatch", "method", method, "msgIdx", msgIdx)
				report := MismatchReport{Method: method, MsgIdx: &msgIdx, Diff: err}
				printMismatchReport(report)
				c.compareBlock(ctx, primaryResult, secondaryResult, msgIdx)
				sendFatalError(report, c.fatalErrChan)
			} else {
				log.Debug("COMPARISON PASSED: block result match", "method", method, "msgIdx", msgIdx, "blockHash", primaryResult.BlockHash.Hex())
				// Block hashes match - also compare receipts to validate MultiGasUsed
				// (MultiGasUsed is not part of consensus encoding, so block hash doesn't validate it)
				c.compareReceiptsForResult(ctx, method, msgIdx, primaryResult)
			}
		}

		return primaryResult, primaryErr
	})
}

// CompareMessageResults compares []*execution.MessageResult promises with block comparison on mismatch
func (c *Comparator) CompareMessageResults(
	ctx context.Context,
	method string,
	msgIdxStart arbutil.MessageIndex,
	primary, secondary containers.PromiseInterface[[]*execution.MessageResult],
) containers.PromiseInterface[[]*execution.MessageResult] {
	return containers.DoPromise(ctx, func(ctx context.Context) ([]*execution.MessageResult, error) {
		primaryResults, primaryErr := primary.Await(ctx)
		secondaryResults, secondaryErr := secondary.Await(ctx)

		// Compare response headers (X-Arb-*)
		c.compareResponseHeaders(method, &msgIdxStart)

		if err := compareErrors(ctx, primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, MsgIdx: &msgIdxStart, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan)
		}

		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResults, secondaryResults); err != nil {
				report := MismatchReport{Method: method, MsgIdx: &msgIdxStart, Diff: err}
				printMismatchReport(report)

				minLen := len(primaryResults)
				if len(secondaryResults) < minLen {
					minLen = len(secondaryResults)
				}
				for i := 0; i < minLen; i++ {
					if compare(primaryResults[i], secondaryResults[i]) != nil {
						//nolint:gosec // slice index is bounded by result count
						c.compareBlock(ctx, primaryResults[i], secondaryResults[i], msgIdxStart+arbutil.MessageIndex(i))
					}
				}

				sendFatalError(report, c.fatalErrChan)
			}
		}

		return primaryResults, primaryErr
	})
}

// CompareMessageIndex compares arbutil.MessageIndex promises
func (c *Comparator) CompareMessageIndex(
	ctx context.Context,
	method string,
	primary, secondary containers.PromiseInterface[arbutil.MessageIndex],
) containers.PromiseInterface[arbutil.MessageIndex] {
	return containers.DoPromise(ctx, func(ctx context.Context) (arbutil.MessageIndex, error) {
		primaryResult, primaryErr := primary.Await(ctx)
		secondaryResult, secondaryErr := secondary.Await(ctx)

		if err := compareErrors(ctx, primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan)
		}
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				printMismatch(MismatchReport{Method: method, Diff: err}, c.fatalErrChan)
			}
		}

		return primaryResult, primaryErr
	})
}

// CompareEmpty compares only errors for void methods
func (c *Comparator) CompareEmpty(
	ctx context.Context,
	method string,
	primary, secondary containers.PromiseInterface[struct{}],
) containers.PromiseInterface[struct{}] {
	return containers.DoPromise(ctx, func(ctx context.Context) (struct{}, error) {
		_, primaryErr := primary.Await(ctx)
		_, secondaryErr := secondary.Await(ctx)

		if err := compareErrors(ctx, primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan)
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

		if err := compareErrors(ctx, primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, MsgIdx: &msgIdx, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan)
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
	return containers.DoPromise(ctx, func(ctx context.Context) (bool, error) {
		primaryResult, primaryErr := primary.Await(ctx)
		secondaryResult, secondaryErr := secondary.Await(ctx)

		if err := compareErrors(ctx, primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan)
		}
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				printMismatch(MismatchReport{Method: method, Diff: err}, c.fatalErrChan)
			}
		}

		return primaryResult, primaryErr
	})
}

// CompareMaintenanceStatus compares *execution.MaintenanceStatus promises
func (c *Comparator) CompareMaintenanceStatus(
	ctx context.Context,
	method string,
	primary, secondary containers.PromiseInterface[*execution.MaintenanceStatus],
) containers.PromiseInterface[*execution.MaintenanceStatus] {
	return containers.DoPromise(ctx, func(ctx context.Context) (*execution.MaintenanceStatus, error) {
		primaryResult, primaryErr := primary.Await(ctx)
		secondaryResult, secondaryErr := secondary.Await(ctx)

		if err := compareErrors(ctx, primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan)
		}
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				printMismatch(MismatchReport{Method: method, Diff: err}, c.fatalErrChan)
			}
		}

		return primaryResult, primaryErr
	})
}

// CompareUint64 compares uint64 promises
func (c *Comparator) CompareUint64(
	ctx context.Context,
	method string,
	msgIdx arbutil.MessageIndex,
	primary, secondary containers.PromiseInterface[uint64],
) containers.PromiseInterface[uint64] {
	return containers.DoPromise(ctx, func(ctx context.Context) (uint64, error) {
		primaryResult, primaryErr := primary.Await(ctx)
		secondaryResult, secondaryErr := secondary.Await(ctx)

		if err := compareErrors(ctx, primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, MsgIdx: &msgIdx, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan)
		}
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				printMismatch(MismatchReport{Method: method, MsgIdx: &msgIdx, Diff: err}, c.fatalErrChan)
			}
		}

		return primaryResult, primaryErr
	})
}

// CompareRecordResult compares *execution.RecordResult promises
func (c *Comparator) CompareRecordResult(
	ctx context.Context,
	method string,
	msgIdx arbutil.MessageIndex,
	primary, secondary containers.PromiseInterface[*execution.RecordResult],
) containers.PromiseInterface[*execution.RecordResult] {
	return containers.DoPromise(ctx, func(ctx context.Context) (*execution.RecordResult, error) {
		primaryResult, primaryErr := primary.Await(ctx)
		secondaryResult, secondaryErr := secondary.Await(ctx)

		if err := compareErrors(ctx, primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, MsgIdx: &msgIdx, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan)
		}
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				printMismatch(MismatchReport{Method: method, MsgIdx: &msgIdx, Diff: err}, c.fatalErrChan)
			}
		}

		return primaryResult, primaryErr
	})
}

// CompareHeader compares *types.Header promises
func (c *Comparator) CompareHeader(
	ctx context.Context,
	method string,
	blockNum *big.Int,
	primary, secondary containers.PromiseInterface[*types.Header],
) containers.PromiseInterface[*types.Header] {
	return containers.DoPromise(ctx, func(ctx context.Context) (*types.Header, error) {
		primaryResult, primaryErr := primary.Await(ctx)
		secondaryResult, secondaryErr := secondary.Await(ctx)

		if err := compareErrors(ctx, primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, BlockNum: blockNum, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan)
		}
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				printMismatch(MismatchReport{Method: method, BlockNum: blockNum, Diff: err}, c.fatalErrChan)
			}
		}

		return primaryResult, primaryErr
	})
}

// CompareHeaderByHash compares *types.Header promises fetched by hash
func (c *Comparator) CompareHeaderByHash(
	ctx context.Context,
	method string,
	hash common.Hash,
	primary, secondary containers.PromiseInterface[*types.Header],
) containers.PromiseInterface[*types.Header] {
	return containers.DoPromise(ctx, func(ctx context.Context) (*types.Header, error) {
		primaryResult, primaryErr := primary.Await(ctx)
		secondaryResult, secondaryErr := secondary.Await(ctx)

		if err := compareErrors(ctx, primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, Hash: &hash, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan)
		}
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				printMismatch(MismatchReport{Method: method, Hash: &hash, Diff: err}, c.fatalErrChan)
			}
		}

		return primaryResult, primaryErr
	})
}

// CompareReceipts compares []*types.Receipt promises
func (c *Comparator) CompareReceipts(
	ctx context.Context,
	method string,
	blockNum *big.Int,
	primary, secondary containers.PromiseInterface[[]*types.Receipt],
) containers.PromiseInterface[[]*types.Receipt] {
	return containers.DoPromise(ctx, func(ctx context.Context) ([]*types.Receipt, error) {
		primaryResult, primaryErr := primary.Await(ctx)
		secondaryResult, secondaryErr := secondary.Await(ctx)

		if err := compareErrors(ctx, primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, BlockNum: blockNum, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan)
		}
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				printMismatch(MismatchReport{Method: method, BlockNum: blockNum, Diff: err}, c.fatalErrChan)
			}
		}

		return primaryResult, primaryErr
	})
}

// compareBlock performs detailed comparison of block data when a result mismatch is detected
func (c *Comparator) compareBlock(
	ctx context.Context,
	primaryResult, secondaryResult *execution.MessageResult,
	msgIdx arbutil.MessageIndex,
) {
	if c.primary == nil || c.secondary == nil {
		return
	}

	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "=============== BLOCK COMPARISON ===============")
	fmt.Fprintf(os.Stderr, "Message Index: %d\n", msgIdx)
	fmt.Fprintln(os.Stderr, "")

	// Fetch headers
	var primaryHeader, secondaryHeader *types.Header
	var primaryHeaderErr, secondaryHeaderErr error

	if primaryResult != nil && primaryResult.BlockHash != (common.Hash{}) {
		primaryHeader, primaryHeaderErr = c.primary.GetHeaderByHash(primaryResult.BlockHash).Await(ctx)
	}
	if secondaryResult != nil && secondaryResult.BlockHash != (common.Hash{}) {
		secondaryHeader, secondaryHeaderErr = c.secondary.GetHeaderByHash(secondaryResult.BlockHash).Await(ctx)
	}

	// Compare headers
	fmt.Fprintln(os.Stderr, "=== HEADER COMPARISON ===")
	if primaryHeaderErr != nil || secondaryHeaderErr != nil {
		fmt.Fprintf(os.Stderr, "  Primary Header Error:   %v\n", primaryHeaderErr)
		fmt.Fprintf(os.Stderr, "  Secondary Header Error: %v\n", secondaryHeaderErr)
	} else if primaryHeader == nil && secondaryHeader == nil {
		fmt.Fprintln(os.Stderr, "  Both headers are nil")
	} else if primaryHeader == nil {
		fmt.Fprintln(os.Stderr, "  Primary header is nil, secondary is not")
	} else if secondaryHeader == nil {
		fmt.Fprintln(os.Stderr, "  Secondary header is nil, primary is not")
	} else if err := compare(primaryHeader, secondaryHeader); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
	} else {
		fmt.Fprintln(os.Stderr, "  All header fields match")
	}
	fmt.Fprintln(os.Stderr, "")

	// Fetch and compare receipts if headers available
	if primaryHeader != nil && secondaryHeader != nil {
		blockNum := primaryHeader.Number
		fmt.Fprintf(os.Stderr, "=== RECEIPTS COMPARISON (Block %s) ===\n", blockNum.String())

		primaryReceipts, primaryReceiptsErr := c.primary.GetBlockReceipts(blockNum).Await(ctx)
		secondaryReceipts, secondaryReceiptsErr := c.secondary.GetBlockReceipts(blockNum).Await(ctx)

		if primaryReceiptsErr != nil || secondaryReceiptsErr != nil {
			fmt.Fprintf(os.Stderr, "  Primary Receipts Error:   %v\n", primaryReceiptsErr)
			fmt.Fprintf(os.Stderr, "  Secondary Receipts Error: %v\n", secondaryReceiptsErr)
		} else if err := compare(primaryReceipts, secondaryReceipts); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
		} else {
			fmt.Fprintln(os.Stderr, "  All receipts match")
		}
	}

	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "=====================================================")
}

// compareReceiptsForResult fetches and compares receipts from both clients after a successful
// DigestMessage. This validates MultiGasUsed which is NOT part of the consensus receipt encoding
// and therefore not validated by block hash comparison.
func (c *Comparator) compareReceiptsForResult(
	ctx context.Context,
	method string,
	msgIdx arbutil.MessageIndex,
	result *execution.MessageResult,
) {
	if c.primary == nil || c.secondary == nil || result == nil {
		return
	}
	if result.BlockHash == (common.Hash{}) {
		return
	}

	// Try fetching receipts immediately - block may already be available.
	// Only wait for subscription notification if initial fetch returns nil receipts (block not found).
	// Note: nil means block not found, empty slice [] means block found but has no transactions.
	primaryReceipts, primaryErr := c.primary.GetBlockReceiptsByHash(result.BlockHash).Await(ctx)

	// If primary returned error or nil receipts (block not found), wait for block notification and retry
	if primaryErr != nil || primaryReceipts == nil {
		blockWaiter := c.waitForBlock(result.BlockHash)

		select {
		case <-ctx.Done():
			c.unregisterBlockWaiter(result.BlockHash, blockWaiter)
			return
		case <-blockWaiter:
			log.Debug("Block notification received, retrying fetch", "blockHash", result.BlockHash.Hex()[:10])
		case <-time.After(c.receiptTimeout):
			log.Debug("Block wait timeout, retrying fetch anyway", "blockHash", result.BlockHash.Hex()[:10], "timeout", c.receiptTimeout)
		}
		c.unregisterBlockWaiter(result.BlockHash, blockWaiter)

		// Retry fetch after waiting
		primaryReceipts, primaryErr = c.primary.GetBlockReceiptsByHash(result.BlockHash).Await(ctx)
	}

	secondaryReceipts, secondaryErr := c.secondary.GetBlockReceiptsByHash(result.BlockHash).Await(ctx)

	// Fail if primary receipts are unavailable but secondary has receipts
	// (0 receipts on both sides is valid for blocks with no transactions)
	if primaryErr == nil && len(primaryReceipts) == 0 && len(secondaryReceipts) > 0 {
		report := MismatchReport{
			Method: method + " (receipts)",
			MsgIdx: &msgIdx,
			Diff:   fmt.Errorf("primary receipts unavailable for block %s after waiting %v (secondary has %d receipts)", result.BlockHash.Hex()[:10], c.receiptTimeout, len(secondaryReceipts)),
		}
		printMismatchReport(report)
		sendFatalError(report, c.fatalErrChan)
		return
	}

	if primaryErr != nil || secondaryErr != nil {
		if err := compareErrors(ctx, primaryErr, secondaryErr); err != nil {
			report := MismatchReport{
				Method:       method + " (receipts)",
				MsgIdx:       &msgIdx,
				Diff:         err,
				PrimaryErr:   primaryErr,
				SecondaryErr: secondaryErr,
			}
			printMismatch(report, c.fatalErrChan)
		}
		return
	}

	// Log MultiGas values for each receipt at debug level
	for i := 0; i < len(primaryReceipts) && i < len(secondaryReceipts); i++ {
		pRec := primaryReceipts[i]
		sRec := secondaryReceipts[i]
		log.Debug("MULTIGAS VALUES",
			"receiptIdx", i,
			"txHash", pRec.TxHash.Hex(),
			"primaryMultiGas", pRec.MultiGasUsed,
			"secondaryMultiGas", sRec.MultiGasUsed,
			"primaryEffGasPrice", pRec.EffectiveGasPrice,
			"secondaryEffGasPrice", sRec.EffectiveGasPrice)
	}

	// Compare receipts (includes MultiGasUsed comparison via cmpOptions)
	if err := compare(primaryReceipts, secondaryReceipts); err != nil {
		log.Debug("RECEIPT COMPARISON FAILED", "method", method, "msgIdx", msgIdx, "receiptCount", len(primaryReceipts))
		report := MismatchReport{
			Method: method + " (receipts/MultiGasUsed)",
			MsgIdx: &msgIdx,
			Diff:   err,
		}
		printMismatchReport(report)
		sendFatalError(report, c.fatalErrChan)
	} else {
		log.Debug("RECEIPT COMPARISON PASSED", "method", method, "msgIdx", msgIdx, "receiptCount", len(primaryReceipts))
	}
}

func printMismatchReport(report MismatchReport) {
	// Print human-readable diff to stderr (structured logging escapes newlines)
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "=============== EXECUTION CLIENT COMPARISON MISMATCH ===============")

	// Print context based on what's available
	switch {
	case report.MsgIdx != nil:
		fmt.Fprintf(os.Stderr, "Method: %s | Message Index: %d\n", report.Method, *report.MsgIdx)
	case report.BlockNum != nil:
		fmt.Fprintf(os.Stderr, "Method: %s | Block Number: %s\n", report.Method, report.BlockNum.String())
	case report.Hash != nil:
		fmt.Fprintf(os.Stderr, "Method: %s | Hash: %s\n", report.Method, report.Hash.Hex())
	default:
		fmt.Fprintf(os.Stderr, "Method: %s\n", report.Method)
	}

	if report.PrimaryErr != nil || report.SecondaryErr != nil {
		fmt.Fprintf(os.Stderr, "Primary Error:   %v\n", report.PrimaryErr)
		fmt.Fprintf(os.Stderr, "Secondary Error: %v\n", report.SecondaryErr)
	}
	fmt.Fprintln(os.Stderr, "Diff (- primary, + secondary):")
	fmt.Fprintln(os.Stderr, report.Diff.Error())
	fmt.Fprintln(os.Stderr, "===================================================================")
	fmt.Fprintln(os.Stderr, "")

	// Structured log entry for log aggregation
	logMismatch(report)
}

func logMismatch(report MismatchReport) {
	switch {
	case report.MsgIdx != nil:
		log.Error("Execution client comparison mismatch", "method", report.Method, "msgIdx", *report.MsgIdx)
	case report.BlockNum != nil:
		log.Error("Execution client comparison mismatch", "method", report.Method, "blockNum", report.BlockNum.String())
	case report.Hash != nil:
		log.Error("Execution client comparison mismatch", "method", report.Method, "hash", report.Hash.Hex())
	default:
		log.Error("Execution client comparison mismatch", "method", report.Method)
	}
}

func sendFatalError(report MismatchReport, fatalErrChan chan<- error) {
	if fatalErrChan == nil {
		return
	}

	var err error
	switch {
	case report.MsgIdx != nil:
		err = fmt.Errorf("%w in %s at msgIdx %d", ErrMismatch, report.Method, *report.MsgIdx)
	case report.BlockNum != nil:
		err = fmt.Errorf("%w in %s at block %s", ErrMismatch, report.Method, report.BlockNum.String())
	case report.Hash != nil:
		err = fmt.Errorf("%w in %s for hash %s", ErrMismatch, report.Method, report.Hash.Hex())
	default:
		err = fmt.Errorf("%w in %s", ErrMismatch, report.Method)
	}

	fatalErrChan <- err
}

// printMismatch outputs a formatted mismatch report to stderr and logs, then sends fatal error
func printMismatch(report MismatchReport, fatalErrChan chan<- error) {
	printMismatchReport(report)
	sendFatalError(report, fatalErrChan)
}
