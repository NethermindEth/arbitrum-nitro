package comparisonrpcclient

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"

	"github.com/offchainlabs/nitro/arbos/arbostypes"
	"github.com/offchainlabs/nitro/arbutil"
	"github.com/offchainlabs/nitro/execution"
	executionrpcclient "github.com/offchainlabs/nitro/execution/rpcclient"
	"github.com/offchainlabs/nitro/util/containers"
	"github.com/offchainlabs/nitro/util/rpcclient"
	"github.com/offchainlabs/nitro/util/stopwaiter"
)

var ErrMismatch = errors.New("execution client comparison mismatch")

// ComparisonClient wraps primary and secondary execution clients for comparison
type ComparisonClient struct {
	stopwaiter.StopWaiter
	primary    *executionrpcclient.Client
	secondary  *executionrpcclient.Client
	comparator *Comparator

	// forwardMu serializes DigestMessageWithExpected calls to ensure ordered
	// message delivery to Nethermind (which processes blocks sequentially)
	forwardMu sync.Mutex

	// errorsMu protects errors slice for concurrent access from comparison goroutines
	errorsMu sync.Mutex
	errors   []error
}

func NewComparisonClient(
	primaryConfig rpcclient.ClientConfigFetcher,
	secondaryConfig rpcclient.ClientConfigFetcher,
	stack *node.Node,
	fatalErrChan chan<- error,
) *ComparisonClient {
	primary := executionrpcclient.NewClient(primaryConfig, stack)
	secondary := executionrpcclient.NewClient(secondaryConfig, nil)
	cc := &ComparisonClient{
		primary:   primary,
		secondary: secondary,
	}
	cc.comparator = NewComparator(fatalErrChan, cc.RecordError, primary, secondary)
	return cc
}

func (c *ComparisonClient) Start(ctx context.Context) error {
	c.StopWaiter.Start(ctx, c)
	if err := c.primary.Start(ctx); err != nil {
		return fmt.Errorf("failed to start primary execution client: %w", err)
	}
	if err := c.secondary.Start(ctx); err != nil {
		return fmt.Errorf("failed to start secondary execution client: %w", err)
	}
	log.Info("Comparison execution client started", "primary", "connected", "secondary", "connected")
	return nil
}

func (c *ComparisonClient) StopAndWait() {
	// Cancel own context first so in-flight goroutines see ctx.Done(),
	// then stop children that those goroutines depend on.
	c.StopWaiter.StopAndWait()
	c.primary.StopAndWait()
	c.secondary.StopAndWait()
}

// RecordError records a comparison error for later retrieval via Errors().
func (c *ComparisonClient) RecordError(err error) {
	c.errorsMu.Lock()
	defer c.errorsMu.Unlock()
	c.errors = append(c.errors, err)
}

// HasErrors returns true if any comparison mismatches were recorded.
func (c *ComparisonClient) HasErrors() bool {
	c.errorsMu.Lock()
	defer c.errorsMu.Unlock()
	return len(c.errors) > 0
}

// Errors returns a copy of all recorded comparison errors.
func (c *ComparisonClient) Errors() []error {
	c.errorsMu.Lock()
	defer c.errorsMu.Unlock()
	result := make([]error, len(c.errors))
	copy(result, c.errors)
	return result
}

// ForwardToSecondary forwards a message to only the secondary execution client
// and compares the result with the expected result from the primary.
// This is used when UseInternalSequencer is true - the primary (internal) already
// processed the block, so we only need to forward to secondary and compare.
// Uses a mutex to serialize calls since Nethermind processes blocks sequentially.
// Synchronous — holds forwardMu for the duration of the call.
func (c *ComparisonClient) ForwardToSecondary(
	msgIdx arbutil.MessageIndex,
	msg *arbostypes.MessageWithMetadata,
	expectedResult *execution.MessageResult,
) (*execution.MessageResult, error) {
	// Serialize secondary calls to avoid "mutex held" errors from Nethermind
	c.forwardMu.Lock()
	defer c.forwardMu.Unlock()

	secondaryPromise := c.secondary.DigestMessage(msgIdx, msg, nil)
	return c.comparator.CompareWithExpected(
		c.GetContext(),
		"ForwardToSecondary",
		msgIdx,
		expectedResult,
		secondaryPromise,
	)
}

// PrimaryResultAtMessageIndex returns the result from the primary execution client only,
// without comparing to secondary. Used to get expected results before secondary is initialized.
func (c *ComparisonClient) PrimaryResultAtMessageIndex(msgIdx arbutil.MessageIndex) containers.PromiseInterface[*execution.MessageResult] {
	return c.primary.ResultAtMessageIndex(msgIdx)
}

// ForwardInitToSecondary forwards an init message (msgIdx 0) to the secondary
// execution client and compares the result with the expected result from the primary.
// This is used when UseInternalSequencer is true to initialize the secondary with
// the same genesis state as the primary and verify they match.
// Synchronous — blocks until the secondary processes the init message.
func (c *ComparisonClient) ForwardInitToSecondary(
	msg *arbostypes.MessageWithMetadata,
	expectedResult *execution.MessageResult,
) (*execution.MessageResult, error) {
	log.Info("Forwarding init message to secondary execution client with comparison")
	secondaryPromise := c.secondary.DigestMessage(0, msg, nil)
	return c.comparator.CompareWithExpected(
		c.GetContext(),
		"ForwardInitToSecondary",
		0,
		expectedResult,
		secondaryPromise,
	)
}

func (c *ComparisonClient) DigestMessage(
	msgIdx arbutil.MessageIndex,
	msg *arbostypes.MessageWithMetadata,
	msgForPrefetch *arbostypes.MessageWithMetadata,
) containers.PromiseInterface[*execution.MessageResult] {
	return c.comparator.CompareMessageResult(
		c.GetContext(),
		"DigestMessage",
		msgIdx,
		c.primary.DigestMessage(msgIdx, msg, msgForPrefetch),
		c.secondary.DigestMessage(msgIdx, msg, msgForPrefetch),
	)
}

// DigestMessageWithExpected processes a message on the secondary execution client
// and compares with an expected result from the primary (internal sequencer).
// Used when UseInternalSequencer is true - the primary already processed the block,
// so we only need to forward to secondary and compare.
// This method is synchronous and serialized to ensure ordered message delivery.
// Receipt comparison runs asynchronously — a block commit waiter is registered and the
// goroutine waits for NotifyBlockCommitted (called after appendBlock) before fetching
// primary receipts, avoiding the need for polling.
func (c *ComparisonClient) DigestMessageWithExpected(
	msgIdx arbutil.MessageIndex,
	msg *arbostypes.MessageWithMetadata,
	expectedResult *execution.MessageResult,
) error {
	// Serialize calls to ensure ordered delivery to Nethermind
	c.forwardMu.Lock()
	defer c.forwardMu.Unlock()

	secondaryResult, secondaryErr := c.secondary.DigestMessage(msgIdx, msg, nil).Await(c.GetContext())
	if secondaryErr != nil {
		if isShutdownError(secondaryErr) {
			log.Debug("Ignoring shutdown error in DigestMessageWithExpected", "msgIdx", msgIdx)
			return nil
		}
		report := MismatchReport{
			Method:       "DigestMessageWithExpected",
			MsgIdx:       &msgIdx,
			Diff:         fmt.Errorf("secondary error: %w", secondaryErr),
			SecondaryErr: secondaryErr,
		}
		printMismatchReport(report)
		sendFatalError(report, c.comparator.fatalErrChan, c.comparator.errorRecorder)
		return secondaryErr
	}

	// Register a waiter for the block commit notification before launching the goroutine.
	// The block will be committed to primary's DB shortly after this method returns
	// (appendBlock runs after WriteMessageFromSequencer, then NotifyBlockCommitted fires).
	// StopWaiter.LaunchThread ensures the goroutine is tracked and waited on during shutdown.
	blockReady := c.comparator.RegisterBlockWaiter(expectedResult.BlockHash)
	c.LaunchThread(func(ctx context.Context) {
		c.comparator.compareReceiptsForResult(ctx, "DigestMessageWithExpected", msgIdx, expectedResult, blockReady)
	})

	// Compare block hash (consensus-critical)
	if err := compare(expectedResult, secondaryResult); err != nil {
		mismatchErr := fmt.Errorf("comparison mismatch at msgIdx %d: %w", msgIdx, err)
		report := MismatchReport{
			Method: "DigestMessageWithExpected",
			MsgIdx: &msgIdx,
			Diff:   err,
		}
		printMismatchReport(report)
		sendFatalError(report, c.comparator.fatalErrChan, c.comparator.errorRecorder)
		return mismatchErr
	}

	log.Info("Comparison passed: block hash match", "method", "DigestMessageWithExpected", "msgIdx", msgIdx)
	return nil
}

// NotifyBlockCommitted signals that a block has been committed to the primary's database.
// Called by the execution engine after appendBlock succeeds, unblocking async receipt
// comparison goroutines that are waiting for this block hash.
func (c *ComparisonClient) NotifyBlockCommitted(blockHash common.Hash) {
	c.comparator.NotifyBlockCommitted(blockHash)
}

func (c *ComparisonClient) Reorg(
	msgIdxOfFirstMsgToAdd arbutil.MessageIndex,
	newMessages []arbostypes.MessageWithMetadataAndBlockInfo,
	oldMessages []*arbostypes.MessageWithMetadata,
) containers.PromiseInterface[[]*execution.MessageResult] {
	return c.comparator.CompareMessageResults(
		c.GetContext(),
		"Reorg",
		msgIdxOfFirstMsgToAdd,
		c.primary.Reorg(msgIdxOfFirstMsgToAdd, newMessages, oldMessages),
		c.secondary.Reorg(msgIdxOfFirstMsgToAdd, newMessages, oldMessages),
	)
}

func (c *ComparisonClient) HeadMessageIndex() containers.PromiseInterface[arbutil.MessageIndex] {
	return c.comparator.CompareMessageIndex(
		c.GetContext(),
		"HeadMessageIndex",
		c.primary.HeadMessageIndex(),
		c.secondary.HeadMessageIndex(),
	)
}

func (c *ComparisonClient) ResultAtMessageIndex(msgIdx arbutil.MessageIndex) containers.PromiseInterface[*execution.MessageResult] {
	return c.comparator.CompareMessageResult(
		c.GetContext(),
		"ResultAtMessageIndex",
		msgIdx,
		c.primary.ResultAtMessageIndex(msgIdx),
		c.secondary.ResultAtMessageIndex(msgIdx),
	)
}

func (c *ComparisonClient) SetFinalityData(
	safeFinalityData *arbutil.FinalityData,
	finalizedFinalityData *arbutil.FinalityData,
	validatedFinalityData *arbutil.FinalityData,
) containers.PromiseInterface[struct{}] {
	return c.comparator.CompareEmpty(
		c.GetContext(),
		"SetFinalityData",
		c.primary.SetFinalityData(safeFinalityData, finalizedFinalityData, validatedFinalityData),
		c.secondary.SetFinalityData(safeFinalityData, finalizedFinalityData, validatedFinalityData),
	)
}

func (c *ComparisonClient) SetConsensusSyncData(syncData *execution.ConsensusSyncData) containers.PromiseInterface[struct{}] {
	return c.comparator.CompareEmpty(
		c.GetContext(),
		"SetConsensusSyncData",
		c.primary.SetConsensusSyncData(syncData),
		c.secondary.SetConsensusSyncData(syncData),
	)
}

func (c *ComparisonClient) MarkFeedStart(to arbutil.MessageIndex) containers.PromiseInterface[struct{}] {
	return c.comparator.CompareEmptyWithMsgIdx(
		c.GetContext(),
		"MarkFeedStart",
		to,
		c.primary.MarkFeedStart(to),
		c.secondary.MarkFeedStart(to),
	)
}

func (c *ComparisonClient) TriggerMaintenance() containers.PromiseInterface[struct{}] {
	return c.comparator.CompareEmpty(
		c.GetContext(),
		"TriggerMaintenance",
		c.primary.TriggerMaintenance(),
		c.secondary.TriggerMaintenance(),
	)
}

func (c *ComparisonClient) ShouldTriggerMaintenance() containers.PromiseInterface[bool] {
	return c.comparator.CompareBool(
		c.GetContext(),
		"ShouldTriggerMaintenance",
		c.primary.ShouldTriggerMaintenance(),
		c.secondary.ShouldTriggerMaintenance(),
	)
}

func (c *ComparisonClient) MaintenanceStatus() containers.PromiseInterface[*execution.MaintenanceStatus] {
	return c.comparator.CompareMaintenanceStatus(
		c.GetContext(),
		"MaintenanceStatus",
		c.primary.MaintenanceStatus(),
		c.secondary.MaintenanceStatus(),
	)
}

func (c *ComparisonClient) ArbOSVersionForMessageIndex(msgIdx arbutil.MessageIndex) containers.PromiseInterface[uint64] {
	return c.comparator.CompareUint64(
		c.GetContext(),
		"ArbOSVersionForMessageIndex",
		msgIdx,
		c.primary.ArbOSVersionForMessageIndex(msgIdx),
		c.secondary.ArbOSVersionForMessageIndex(msgIdx),
	)
}

func (c *ComparisonClient) RecordBlockCreation(
	pos arbutil.MessageIndex,
	msg *arbostypes.MessageWithMetadata,
	wasmTargets []rawdb.WasmTarget,
) containers.PromiseInterface[*execution.RecordResult] {
	return c.comparator.CompareRecordResult(
		c.GetContext(),
		"RecordBlockCreation",
		pos,
		c.primary.RecordBlockCreation(pos, msg, wasmTargets),
		c.secondary.RecordBlockCreation(pos, msg, wasmTargets),
	)
}

func (c *ComparisonClient) PrepareForRecord(start, end arbutil.MessageIndex) containers.PromiseInterface[struct{}] {
	return c.comparator.CompareEmptyWithMsgIdx(
		c.GetContext(),
		"PrepareForRecord",
		start,
		c.primary.PrepareForRecord(start, end),
		c.secondary.PrepareForRecord(start, end),
	)
}

// GetHeaderByNumber retrieves block header by number from both clients and compares
func (c *ComparisonClient) GetHeaderByNumber(blockNum *big.Int) containers.PromiseInterface[*types.Header] {
	return c.comparator.CompareHeader(
		c.GetContext(),
		"GetHeaderByNumber",
		blockNum,
		c.primary.GetHeaderByNumber(blockNum),
		c.secondary.GetHeaderByNumber(blockNum),
	)
}

// GetBlockReceipts retrieves block receipts from both clients and compares
func (c *ComparisonClient) GetBlockReceipts(blockNum *big.Int) containers.PromiseInterface[[]*types.Receipt] {
	return c.comparator.CompareReceipts(
		c.GetContext(),
		"GetBlockReceipts",
		blockNum,
		c.primary.GetBlockReceipts(blockNum),
		c.secondary.GetBlockReceipts(blockNum),
	)
}

// GetHeaderByHash retrieves block header by hash from both clients and compares
func (c *ComparisonClient) GetHeaderByHash(hash common.Hash) containers.PromiseInterface[*types.Header] {
	return c.comparator.CompareHeaderByHash(
		c.GetContext(),
		"GetHeaderByHash",
		hash,
		c.primary.GetHeaderByHash(hash),
		c.secondary.GetHeaderByHash(hash),
	)
}
