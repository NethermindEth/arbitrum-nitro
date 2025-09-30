package nethexec

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/offchainlabs/nitro/arbnode"
	"github.com/offchainlabs/nitro/arbos/arbostypes"
	"github.com/offchainlabs/nitro/arbutil"
	"github.com/offchainlabs/nitro/execution"
	"github.com/offchainlabs/nitro/execution/gethexec"
	"github.com/offchainlabs/nitro/util/containers"
)

// Constants for configuration
const (
	defaultWorkerPoolSize = 8 // Will be adjusted based on runtime.NumCPU()
)

// Interfaces for better separation of concerns
type FullExecutionClient interface {
	execution.ExecutionSequencer // includes ExecutionClient
	execution.ExecutionRecorder
	execution.ExecutionBatchPoster
}

// Main comparison client with optimized resource management
type compareExecutionClient struct {
	ctx          context.Context
	gethClient   *gethexec.ExecutionNode
	nethClient   *nethermindExecutionClient
	fatalErrChan chan error
	comparator   *comparator
	syncService  *syncService
	logger       *slog.Logger
}

// Ensure interface compliance
var (
	_ FullExecutionClient         = (*compareExecutionClient)(nil)
	_ arbnode.ExecutionNodeBridge = (*compareExecutionClient)(nil)
)

// NewCompareExecutionClient creates a new comparison execution client
func NewCompareExecutionClient(
	ctx context.Context,
	gethClient *gethexec.ExecutionNode,
	nethClient *nethermindExecutionClient,
	fatalErrChan chan error,
) *compareExecutionClient {
	logger := slog.Default().With("component", "compare-execution-client")

	// Calculate optimal worker pool size based on CPU count
	workerPoolSize := max(defaultWorkerPoolSize, runtime.NumCPU())

	workerPool := newComparisonWorkerPool(workerPoolSize, logger)
	comparator := newComparator(workerPool, fatalErrChan, logger)

	return &compareExecutionClient{
		ctx:          ctx,
		gethClient:   gethClient,
		nethClient:   nethClient,
		fatalErrChan: fatalErrChan,
		comparator:   comparator,
		syncService:  newSyncService(logger, comparator),
		logger:       logger,
	}
}

// synchronizeClients performs upfront synchronization during startup
func (c *compareExecutionClient) synchronizeClients(ctx context.Context) error {
	c.logger.Info("Starting client synchronization")

	internal := c.gethClient.HeadMessageIndex()
	external := c.nethClient.HeadMessageIndex()

	intRes, intErr := internal.Await(ctx)
	extRes, extErr := external.Await(ctx)

	// Handle cancellation
	if ctx.Err() != nil {
		return fmt.Errorf("synchronization cancelled: %w", ctx.Err())
	}

	// Handle bootstrap case
	if c.syncService.isBootstrapCase(intErr, extErr) {
		c.logger.Info("Bootstrap case detected, initializing external client")
		if err := c.syncService.handleBootstrapInitialization(ctx, c.nethClient); err != nil {
			return fmt.Errorf("bootstrap initialization failed: %w", err)
		}
		c.logger.Info("Bootstrap initialization completed")
	}

	// Synchronize if heads differ
	if intRes != extRes {
		c.logger.Info("Head message index mismatch detected, starting synchronization",
			"internal_head", intRes,
			"external_head", extRes)

		if err := c.syncService.synchronizeExecutionClients(ctx, c.gethClient, c.nethClient, intRes, extRes); err != nil {
			return fmt.Errorf("client synchronization failed: %w", err)
		}
		c.logger.Info("Client synchronization completed successfully")
	} else {
		c.logger.Info("Clients already synchronized",
			"head_message_index", intRes)
	}

	return nil
}

// Implementation of ExecutionClient interface methods

func (c *compareExecutionClient) DigestMessage(index arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata, msgForPrefetch *arbostypes.MessageWithMetadata) containers.PromiseInterface[*execution.MessageResult] {
	start := time.Now()

	internal := c.gethClient.DigestMessage(index, msg, msgForPrefetch)
	external := c.nethClient.DigestMessage(index, msg, msgForPrefetch)

	result := c.comparator.compareMessageResultPromise(c.ctx, "DigestMessage", internal, external)

	c.logger.Debug("DigestMessage completed",
		"index", index,
		"elapsed", time.Since(start))

	return result
}

func (c *compareExecutionClient) Reorg(count arbutil.MessageIndex, newMessages []arbostypes.MessageWithMetadataAndBlockInfo, oldMessages []*arbostypes.MessageWithMetadata) containers.PromiseInterface[[]*execution.MessageResult] {
	start := time.Now()

	internal := c.gethClient.Reorg(count, newMessages, oldMessages)
	external := c.nethClient.Reorg(count, newMessages, oldMessages)

	result := c.comparator.compareMessageResultsPromise(c.ctx, "Reorg", internal, external)

	c.logger.Debug("Reorg completed",
		"count", count,
		"new_messages", len(newMessages),
		"old_messages", len(oldMessages),
		"elapsed", time.Since(start))

	return result
}

func (c *compareExecutionClient) HeadMessageIndex() containers.PromiseInterface[arbutil.MessageIndex] {
	start := time.Now()

	internal := c.gethClient.HeadMessageIndex()
	external := c.nethClient.HeadMessageIndex()

	result := c.compareHeadMessageIndexWithSync(internal, external)

	c.logger.Debug("HeadMessageIndex completed", "elapsed", time.Since(start))
	return result
}

func (c *compareExecutionClient) compareHeadMessageIndexWithSync(
	internal containers.PromiseInterface[arbutil.MessageIndex],
	external containers.PromiseInterface[arbutil.MessageIndex],
) containers.PromiseInterface[arbutil.MessageIndex] {

	promise := containers.NewPromise[arbutil.MessageIndex](nil)

	go func() {
		intRes, intErr := internal.Await(c.ctx)
		extRes, extErr := external.Await(c.ctx)

		if err := c.comparator.compareError("compareHeadMessageIndexWithSync", intErr, extErr); err != nil {
			c.logger.Error("compareHeadMessageIndexWithSync", "error", err)
			promise.ProduceError(err)
			return
		}

		if intErr != nil {
			promise.ProduceError(intErr)
			return
		}

		if intRes != extRes {
			c.logger.Info("Head message index mismatch detected, starting synchronization",
				"internal_head", intRes,
				"external_head", extRes)
			if err := c.syncService.synchronizeExecutionClients(c.ctx, c.gethClient, c.nethClient, intRes, extRes); err != nil {
				c.logger.Error("client synchronization failed", "error", err)
				promise.ProduceError(err)
				return
			}
		}

		promise.Produce(max(intRes, extRes))
	}()

	return &promise
}

func (c *compareExecutionClient) ResultAtMessageIndex(index arbutil.MessageIndex) containers.PromiseInterface[*execution.MessageResult] {
	start := time.Now()

	internal := c.gethClient.ResultAtMessageIndex(index)
	external := c.nethClient.ResultAtMessageIndex(index)

	// Use nil fatalErrChan for non-critical operations
	result := c.comparator.compareMessageResultPromise(c.ctx, "ResultAtMessageIndex", internal, external)

	c.logger.Debug("ResultAtMessageIndex completed",
		"index", index,
		"elapsed", time.Since(start))

	return result
}

func (c *compareExecutionClient) MessageIndexToBlockNumber(messageIndex arbutil.MessageIndex) containers.PromiseInterface[uint64] {
	start := time.Now()

	internal := c.gethClient.MessageIndexToBlockNumber(messageIndex)
	external := c.nethClient.MessageIndexToBlockNumber(messageIndex)

	result := c.comparator.compareUint64Promise(c.ctx, "MessageIndexToBlockNumber", internal, external)

	c.logger.Debug("MessageIndexToBlockNumber completed",
		"message_index", messageIndex,
		"elapsed", time.Since(start))

	return result
}

func (c *compareExecutionClient) BlockNumberToMessageIndex(blockNum uint64) containers.PromiseInterface[arbutil.MessageIndex] {
	start := time.Now()

	internal := c.gethClient.BlockNumberToMessageIndex(blockNum)
	external := c.nethClient.BlockNumberToMessageIndex(blockNum)

	result := c.comparator.compareMessageIndexPromise(c.ctx, "BlockNumberToMessageIndex", internal, external)

	c.logger.Debug("BlockNumberToMessageIndex completed",
		"block_num", blockNum,
		"elapsed", time.Since(start))

	return result
}

func (c *compareExecutionClient) SetFinalityData(ctx context.Context, finalityData *arbutil.FinalityData, finalizedFinalityData *arbutil.FinalityData, validatedFinalityData *arbutil.FinalityData) containers.PromiseInterface[struct{}] {
	internal := c.gethClient.SetFinalityData(ctx, finalityData, finalizedFinalityData, validatedFinalityData)
	external := c.nethClient.SetFinalityData(ctx, finalityData, finalizedFinalityData, validatedFinalityData)

	return c.comparator.compareVoidPromise(ctx, "SetFinalityData", internal, external)
}

func (c *compareExecutionClient) MarkFeedStart(to arbutil.MessageIndex) containers.PromiseInterface[struct{}] {
	start := time.Now()

	internal := c.gethClient.MarkFeedStart(to)
	external := c.nethClient.MarkFeedStart(to)

	result := c.comparator.compareVoidPromise(c.ctx, "MarkFeedStart", internal, external)

	c.logger.Debug("MarkFeedStart completed",
		"to", to,
		"elapsed", time.Since(start))

	return result
}

func (c *compareExecutionClient) TriggerMaintenance() containers.PromiseInterface[struct{}] {
	start := time.Now()
	result := c.gethClient.TriggerMaintenance()
	c.logger.Debug("TriggerMaintenance completed", "elapsed", time.Since(start))
	return result
}

func (c *compareExecutionClient) ShouldTriggerMaintenance() containers.PromiseInterface[bool] {
	start := time.Now()

	internal := c.gethClient.ShouldTriggerMaintenance()
	external := c.nethClient.ShouldTriggerMaintenance()

	result := c.comparator.compareBoolPromise(c.ctx, "ShouldTriggerMaintenance", internal, external)

	c.logger.Debug("ShouldTriggerMaintenance completed", "elapsed", time.Since(start))
	return result
}

func (c *compareExecutionClient) MaintenanceStatus() containers.PromiseInterface[*execution.MaintenanceStatus] {
	start := time.Now()

	internal := c.gethClient.MaintenanceStatus()
	external := c.nethClient.MaintenanceStatus()

	result := c.comparator.compareMaintenanceStatusPromise(c.ctx, "MaintenanceStatus", internal, external)

	c.logger.Debug("MaintenanceStatus completed", "elapsed", time.Since(start))
	return result
}

func (c *compareExecutionClient) Start(ctx context.Context) error {
	start := time.Now()
	c.logger.Info("Starting comparison execution client")

	// Start the internal geth client first
	if err := c.gethClient.Start(ctx); err != nil {
		return fmt.Errorf("failed to start internal geth client: %w", err)
	}
	c.logger.Info("Internal geth client started successfully")

	// Perform upfront synchronization with proper cancellation support
	if err := c.synchronizeClients(ctx); err != nil {
		return fmt.Errorf("client synchronization failed during startup: %w", err)
	}

	c.logger.Info("Comparison execution client started successfully",
		"elapsed", time.Since(start))
	return nil
}

func (c *compareExecutionClient) StopAndWait() {
	start := time.Now()
	c.gethClient.StopAndWait()
	c.logger.Info("StopAndWait completed", "elapsed", time.Since(start))
}

// ExecutionSequencer interface methods
func (c *compareExecutionClient) Pause() {
	start := time.Now()
	c.gethClient.Pause()
	c.logger.Debug("Pause completed", "elapsed", time.Since(start))
}

func (c *compareExecutionClient) Activate() {
	start := time.Now()
	c.gethClient.Activate()
	c.logger.Debug("Activate completed", "elapsed", time.Since(start))
}

func (c *compareExecutionClient) ForwardTo(url string) error {
	start := time.Now()
	err := c.gethClient.ForwardTo(url)
	c.logger.Debug("ForwardTo completed", "url", url, "error", err, "elapsed", time.Since(start))
	return err
}

func (c *compareExecutionClient) SequenceDelayedMessage(message *arbostypes.L1IncomingMessage, delayedSeqNum uint64) error {
	start := time.Now()

	internalErr := c.gethClient.SequenceDelayedMessage(message, delayedSeqNum)
	externalErr := c.nethClient.SequenceDelayedMessage(message, delayedSeqNum)

	if err := c.comparator.compareError("SequenceDelayedMessage", internalErr, externalErr); err != nil {
		select {
		case c.fatalErrChan <- fmt.Errorf("compareExecutionClient SequenceDelayedMessage: %w", err):
		default:
			c.logger.Error("Failed to send comparison error to fatal channel", "error", err)
		}
		return err
	}

	c.logger.Debug("SequenceDelayedMessage completed",
		"delayed_seq_num", delayedSeqNum,
		"error", internalErr,
		"elapsed", time.Since(start))

	return internalErr
}

func (c *compareExecutionClient) NextDelayedMessageNumber() (uint64, error) {
	return c.gethClient.NextDelayedMessageNumber()
}

func (c *compareExecutionClient) Synced(ctx context.Context) bool {
	start := time.Now()
	result := c.gethClient.Synced(ctx)
	c.logger.Debug("Synced completed", "result", result, "elapsed", time.Since(start))
	return result
}

func (c *compareExecutionClient) FullSyncProgressMap(ctx context.Context) map[string]interface{} {
	start := time.Now()
	result := c.gethClient.FullSyncProgressMap(ctx)
	c.logger.Debug("FullSyncProgressMap completed", "elapsed", time.Since(start))
	return result
}

// ExecutionRecorder interface methods
func (c *compareExecutionClient) RecordBlockCreation(ctx context.Context, index arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata) (*execution.RecordResult, error) {
	start := time.Now()
	result, err := c.gethClient.RecordBlockCreation(ctx, index, msg)
	c.logger.Debug("RecordBlockCreation completed",
		"index", index,
		"error", err,
		"elapsed", time.Since(start))
	return result, err
}

func (c *compareExecutionClient) MarkValid(index arbutil.MessageIndex, resultHash common.Hash) {
	start := time.Now()
	c.gethClient.MarkValid(index, resultHash)
	c.logger.Debug("MarkValid completed",
		"index", index,
		"result_hash", resultHash,
		"elapsed", time.Since(start))
}

func (c *compareExecutionClient) PrepareForRecord(ctx context.Context, start, end arbutil.MessageIndex) error {
	startTime := time.Now()
	err := c.gethClient.PrepareForRecord(ctx, start, end)
	c.logger.Debug("PrepareForRecord completed",
		"start", start,
		"end", end,
		"error", err,
		"elapsed", time.Since(startTime))
	return err
}

// ExecutionBatchPoster interface methods
func (c *compareExecutionClient) ArbOSVersionForMessageIndex(msgIdx arbutil.MessageIndex) (uint64, error) {
	start := time.Now()
	result, err := c.gethClient.ArbOSVersionForMessageIndex(msgIdx)
	c.logger.Debug("ArbOSVersionForMessageIndex completed",
		"msg_idx", msgIdx,
		"result", result,
		"error", err,
		"elapsed", time.Since(start))
	return result, err
}

func (c *compareExecutionClient) SetConsensusClient(consensus execution.FullConsensusClient) {
	c.syncService.setConsensus(consensus)
	c.gethClient.SetConsensusClient(consensus)
}

func (c *compareExecutionClient) Initialize(ctx context.Context) error {
	return c.gethClient.Initialize(ctx)
}
