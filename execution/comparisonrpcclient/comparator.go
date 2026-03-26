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
	errorRecorder  func(error) // called on every mismatch to accumulate errors for test cleanup
	primary        *executionrpcclient.Client
	secondary      *executionrpcclient.Client
	compareHeaders bool // Whether to compare response headers

	// blockCommitMu protects blockCommitWaiters for concurrent access from
	// async receipt comparison goroutines and NotifyBlockCommitted calls.
	blockCommitMu      sync.Mutex
	blockCommitWaiters map[common.Hash]chan struct{}
}

// NewComparator creates a new Comparator
func NewComparator(fatalErrChan chan<- error, errorRecorder func(error), primary, secondary *executionrpcclient.Client) *Comparator {
	return &Comparator{
		fatalErrChan:       fatalErrChan,
		errorRecorder:      errorRecorder,
		primary:            primary,
		secondary:          secondary,
		compareHeaders:     primary.CapturedHeaders() != nil && secondary.CapturedHeaders() != nil,
		blockCommitWaiters: make(map[common.Hash]chan struct{}),
	}
}

// RegisterBlockWaiter creates a channel that will be closed when NotifyBlockCommitted
// is called for the given block hash. If the notification already arrived, returns
// a pre-closed channel that selects immediately.
func (c *Comparator) RegisterBlockWaiter(blockHash common.Hash) <-chan struct{} {
	c.blockCommitMu.Lock()
	defer c.blockCommitMu.Unlock()

	ch, exists := c.blockCommitWaiters[blockHash]
	if exists {
		// Notification arrived before waiter registered — channel already closed
		return ch
	}
	ch = make(chan struct{})
	c.blockCommitWaiters[blockHash] = ch
	return ch
}

// NotifyBlockCommitted signals that a block has been committed to the primary's database.
// Async receipt comparison goroutines waiting on this block hash will unblock.
func (c *Comparator) NotifyBlockCommitted(blockHash common.Hash) {
	c.blockCommitMu.Lock()
	defer c.blockCommitMu.Unlock()

	ch, exists := c.blockCommitWaiters[blockHash]
	if exists {
		close(ch)
	} else {
		// Notification arrived before waiter registered — store a pre-closed channel
		ch = make(chan struct{})
		close(ch)
		c.blockCommitWaiters[blockHash] = ch
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

// isShutdownError checks if an error is related to shutdown (context cancellation or StopWaiter stopped).
// These errors should not be reported as comparison mismatches since they're expected during test cleanup.
//
// String matching is necessary because RPC serialization loses Go error types —
// errors.Is(err, context.Canceled) doesn't work when the error was marshalled
// as an RPC response and unmarshalled on the client side.
func isShutdownError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	errMsg := err.Error()
	return strings.Contains(errMsg, "stopped") ||
		strings.Contains(errMsg, "context canceled")
}

// compareErrors compares two errors by their message strings only
func compareErrors(primary, secondary error) error {
	if primary == nil && secondary == nil {
		return nil
	}
	// If primary succeeded but secondary failed due to context cancellation,
	// this is a race condition during test shutdown - not a real mismatch
	if primary == nil && isShutdownError(secondary) {
		return nil
	}
	// If both errors are context cancellation, also not a mismatch
	if isShutdownError(primary) && isShutdownError(secondary) {
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
		sendFatalError(report, c.fatalErrChan, c.errorRecorder)
		return expectedResult, secondaryErr
	}

	if err := compare(expectedResult, secondaryResult); err != nil {
		report := MismatchReport{Method: method, MsgIdx: &msgIdx, Diff: err}
		printMismatchReport(report)
		c.compareBlock(ctx, expectedResult, secondaryResult, msgIdx)
		sendFatalError(report, c.fatalErrChan, c.errorRecorder)
	} else {
		log.Info("Comparison passed: block hash match", "method", method, "msgIdx", msgIdx)
		// Block hashes match - also compare receipts to validate MultiGasUsed
		// (MultiGasUsed is not part of consensus encoding, so block hash doesn't validate it)
		c.compareReceiptsForResult(ctx, method, msgIdx, expectedResult, nil)
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

		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, MsgIdx: &msgIdx, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan, c.errorRecorder)
		}

		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				report := MismatchReport{Method: method, MsgIdx: &msgIdx, Diff: err}
				printMismatchReport(report)
				c.compareBlock(ctx, primaryResult, secondaryResult, msgIdx)
				sendFatalError(report, c.fatalErrChan, c.errorRecorder)
			} else {
				log.Info("Comparison passed: block hash match", "method", method, "msgIdx", msgIdx)
				// Block hashes match - also compare receipts to validate MultiGasUsed
				// (MultiGasUsed is not part of consensus encoding, so block hash doesn't validate it)
				c.compareReceiptsForResult(ctx, method, msgIdx, primaryResult, nil)
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

		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, MsgIdx: &msgIdxStart, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan, c.errorRecorder)
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

				sendFatalError(report, c.fatalErrChan, c.errorRecorder)
			} else {
				log.Info("Comparison passed: block hashes match", "method", method, "msgIdxStart", msgIdxStart, "count", len(primaryResults))
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

		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan, c.errorRecorder)
		}
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				printMismatch(MismatchReport{Method: method, Diff: err}, c.fatalErrChan, c.errorRecorder)
			} else {
				log.Info("Comparison passed", "method", method, "result", primaryResult)
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

		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan, c.errorRecorder)
		} else {
			log.Info("Comparison passed", "method", method)
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

		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, MsgIdx: &msgIdx, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan, c.errorRecorder)
		} else {
			log.Info("Comparison passed", "method", method, "msgIdx", msgIdx)
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

		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan, c.errorRecorder)
		}
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				printMismatch(MismatchReport{Method: method, Diff: err}, c.fatalErrChan, c.errorRecorder)
			} else {
				log.Info("Comparison passed", "method", method, "result", primaryResult)
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

		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan, c.errorRecorder)
		}
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				printMismatch(MismatchReport{Method: method, Diff: err}, c.fatalErrChan, c.errorRecorder)
			} else {
				log.Info("Comparison passed", "method", method)
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

		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, MsgIdx: &msgIdx, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan, c.errorRecorder)
		}
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				printMismatch(MismatchReport{Method: method, MsgIdx: &msgIdx, Diff: err}, c.fatalErrChan, c.errorRecorder)
			} else {
				log.Info("Comparison passed", "method", method, "msgIdx", msgIdx, "result", primaryResult)
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

		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, MsgIdx: &msgIdx, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan, c.errorRecorder)
		}
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				printMismatch(MismatchReport{Method: method, MsgIdx: &msgIdx, Diff: err}, c.fatalErrChan, c.errorRecorder)
			} else {
				log.Info("Comparison passed", "method", method, "msgIdx", msgIdx)
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

		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, BlockNum: blockNum, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan, c.errorRecorder)
		}
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				printMismatch(MismatchReport{Method: method, BlockNum: blockNum, Diff: err}, c.fatalErrChan, c.errorRecorder)
			} else {
				log.Info("Comparison passed", "method", method, "blockNum", blockNum)
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

		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, Hash: &hash, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan, c.errorRecorder)
		}
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				printMismatch(MismatchReport{Method: method, Hash: &hash, Diff: err}, c.fatalErrChan, c.errorRecorder)
			} else {
				log.Info("Comparison passed", "method", method, "hash", hash)
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

		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			printMismatch(MismatchReport{Method: method, BlockNum: blockNum, Diff: err, PrimaryErr: primaryErr, SecondaryErr: secondaryErr}, c.fatalErrChan, c.errorRecorder)
		}
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				printMismatch(MismatchReport{Method: method, BlockNum: blockNum, Diff: err}, c.fatalErrChan, c.errorRecorder)
			} else {
				log.Info("Comparison passed: receipts match", "method", method, "blockNum", blockNum, "numReceipts", len(primaryResult))
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
//
// blockReady is an optional channel that, when non-nil, is waited on before fetching primary
// receipts. This is used when the block hasn't been committed to the primary's database yet
// (UseInternalSequencer mode where DigestMessageWithExpected runs before appendBlock).
// When nil, the block is assumed to already be committed (non-internal-sequencer paths).
func (c *Comparator) compareReceiptsForResult(
	ctx context.Context,
	method string,
	msgIdx arbutil.MessageIndex,
	result *execution.MessageResult,
	blockReady <-chan struct{},
) {
	if c.primary == nil || c.secondary == nil || result == nil {
		return
	}
	if result.BlockHash == (common.Hash{}) {
		return
	}

	// Wait for the block to be committed to primary's database if needed.
	// In UseInternalSequencer mode, DigestMessageWithExpected runs before appendBlock,
	// so we wait for the NotifyBlockCommitted signal rather than polling.
	if blockReady != nil {
		const blockCommitTimeout = 30 * time.Second
		select {
		case <-blockReady:
			// Block committed — clean up waiter entry
			c.blockCommitMu.Lock()
			delete(c.blockCommitWaiters, result.BlockHash)
			c.blockCommitMu.Unlock()
		case <-time.After(blockCommitTimeout):
			log.Error("Timed out waiting for block commit notification",
				"method", method, "msgIdx", msgIdx, "blockHash", result.BlockHash.Hex()[:10])
			c.blockCommitMu.Lock()
			delete(c.blockCommitWaiters, result.BlockHash)
			c.blockCommitMu.Unlock()
			return
		case <-ctx.Done():
			c.blockCommitMu.Lock()
			delete(c.blockCommitWaiters, result.BlockHash)
			c.blockCommitMu.Unlock()
			return // shutdown — don't report mismatch
		}
	}

	// Fetch receipts from both clients using block hash directly.
	// Using hash instead of number because newly produced blocks are indexed by hash
	// immediately, but the number-to-hash mapping may lag behind.
	primaryReceipts, primaryErr := c.primary.GetBlockReceiptsByHash(result.BlockHash).Await(ctx)
	secondaryReceipts, secondaryErr := c.secondary.GetBlockReceiptsByHash(result.BlockHash).Await(ctx)

	// Fail if primary receipts are unavailable but secondary has receipts
	// (0 receipts on both sides is valid for blocks with no transactions)
	if primaryErr == nil && len(primaryReceipts) == 0 && len(secondaryReceipts) > 0 {
		report := MismatchReport{
			Method: method + " (receipts)",
			MsgIdx: &msgIdx,
			Diff:   fmt.Errorf("primary returned 0 receipts for block %s (secondary has %d receipts)", result.BlockHash.Hex()[:10], len(secondaryReceipts)),
		}
		printMismatchReport(report)
		sendFatalError(report, c.fatalErrChan, c.errorRecorder)
		return
	}

	if primaryErr != nil || secondaryErr != nil {
		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			report := MismatchReport{
				Method:       method + " (receipts)",
				MsgIdx:       &msgIdx,
				Diff:         err,
				PrimaryErr:   primaryErr,
				SecondaryErr: secondaryErr,
			}
			printMismatch(report, c.fatalErrChan, c.errorRecorder)
		}
		return
	}

	// Compare receipts (includes MultiGasUsed comparison via cmpOptions)
	if err := compare(primaryReceipts, secondaryReceipts); err != nil {
		report := MismatchReport{
			Method: method + " (receipts/MultiGasUsed)",
			MsgIdx: &msgIdx,
			Diff:   err,
		}
		printMismatchReport(report)
		sendFatalError(report, c.fatalErrChan, c.errorRecorder)
	} else {
		log.Info("Comparison passed: receipts match", "method", method, "msgIdx", msgIdx, "numReceipts", len(primaryReceipts))
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

func sendFatalError(report MismatchReport, fatalErrChan chan<- error, recorder func(error)) {
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

	if recorder != nil {
		recorder(err)
	}
	if fatalErrChan != nil {
		fatalErrChan <- err
	}
}

// printMismatch outputs a formatted mismatch report to stderr and logs, then sends fatal error
func printMismatch(report MismatchReport, fatalErrChan chan<- error, recorder func(error)) {
	printMismatchReport(report)
	sendFatalError(report, fatalErrChan, recorder)
}
