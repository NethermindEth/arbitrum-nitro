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
}

func NewComparisonClient(
	primaryConfig rpcclient.ClientConfigFetcher,
	secondaryConfig rpcclient.ClientConfigFetcher,
	stack *node.Node,
	fatalErrChan chan<- error,
	config *ComparisonExecutionConfig,
) *ComparisonClient {
	primary := executionrpcclient.NewClient(primaryConfig, stack)
	secondary := executionrpcclient.NewClient(secondaryConfig, nil)
	return &ComparisonClient{
		primary:    primary,
		secondary:  secondary,
		comparator: NewComparator(fatalErrChan, primary, secondary, config.ReceiptRetries, config.ReceiptRetryDelay),
	}
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
	c.primary.StopAndWait()
	c.secondary.StopAndWait()
	c.StopWaiter.StopAndWait()
}

// ForwardToSecondary forwards a message to only the secondary execution client
// and compares the result with the expected result from the primary.
// This is used when UseInternalSequencer is true - the primary (internal) already
// processed the block, so we only need to forward to secondary and compare.
// Uses a mutex to serialize calls since Nethermind processes blocks sequentially.
func (c *ComparisonClient) ForwardToSecondary(
	msgIdx arbutil.MessageIndex,
	msg *arbostypes.MessageWithMetadata,
	expectedResult *execution.MessageResult,
) containers.PromiseInterface[*execution.MessageResult] {
	// Serialize secondary calls to avoid "mutex held" errors from Nethermind
	c.forwardMu.Lock()
	defer c.forwardMu.Unlock()

	secondaryPromise := c.secondary.DigestMessage(msgIdx, msg, nil)
	return containers.NewReadyPromise(c.comparator.CompareWithExpected(
		c.GetContext(),
		"ForwardToSecondary",
		msgIdx,
		expectedResult,
		secondaryPromise,
	))
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
func (c *ComparisonClient) ForwardInitToSecondary(
	msg *arbostypes.MessageWithMetadata,
	expectedResult *execution.MessageResult,
) containers.PromiseInterface[*execution.MessageResult] {
	log.Info("Forwarding init message to secondary execution client with comparison")
	secondaryPromise := c.secondary.DigestMessage(0, msg, nil)
	return containers.NewReadyPromise(c.comparator.CompareWithExpected(
		c.GetContext(),
		"ForwardInitToSecondary",
		0,
		expectedResult,
		secondaryPromise,
	))
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
// Note: Receipt comparison is skipped to avoid deadlock (block not yet committed to primary).
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
			// During graceful shutdown, return nil to allow clean termination.
			// Returning an error would cause cascading failures in the sequencer.
			// The shutdown is expected, so no comparison result is meaningful.
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
		sendFatalError(report, c.comparator.fatalErrChan)
		return secondaryErr
	}

	// Compare block hash (consensus-critical)
	if err := compare(expectedResult, secondaryResult); err != nil {
		report := MismatchReport{
			Method: "DigestMessageWithExpected",
			MsgIdx: &msgIdx,
			Diff:   err,
		}
		printMismatchReport(report)
		sendFatalError(report, c.comparator.fatalErrChan)
		return fmt.Errorf("comparison mismatch at msgIdx %d: %w", msgIdx, err)
	}

	return nil
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
