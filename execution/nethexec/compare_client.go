package nethexec

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/google/go-cmp/cmp"
	"github.com/offchainlabs/nitro/arbnode"
	"github.com/offchainlabs/nitro/arbos/arbostypes"
	"github.com/offchainlabs/nitro/arbutil"
	"github.com/offchainlabs/nitro/execution"
	"github.com/offchainlabs/nitro/execution/gethexec"
	"github.com/offchainlabs/nitro/util/containers"
)

type FullExecutionClient interface {
	execution.ExecutionSequencer // includes ExecutionClient
	execution.ExecutionRecorder
	execution.ExecutionBatchPoster
}

var (
	_ FullExecutionClient         = (*compareExecutionClient)(nil)
	_ arbnode.ExecutionNodeBridge = (*compareExecutionClient)(nil)
)

type compareExecutionClient struct {
	gethExecutionClient       *gethexec.ExecutionNode
	nethermindExecutionClient *nethermindExecutionClient
	fatalErrChan              chan error
	consensus                 execution.FullConsensusClient
	syncMutex                 sync.Mutex
}

func NewCompareExecutionClient(
	gethExecutionClient *gethexec.ExecutionNode,
	nethermindExecutionClient *nethermindExecutionClient,
	fatalErrChan chan error,
) *compareExecutionClient {
	return &compareExecutionClient{
		gethExecutionClient:       gethExecutionClient,
		nethermindExecutionClient: nethermindExecutionClient,
		fatalErrChan:              fatalErrChan,
	}
}

func comparePromises[T any](fatalErrChan chan error, op string,
	internal containers.PromiseInterface[T],
	external containers.PromiseInterface[T],
) containers.PromiseInterface[T] {
	promise := containers.NewPromise[T](nil)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		intRes, intErr := internal.Await(ctx)
		extRes, extErr := external.Await(ctx)

		if err := compare(op, intRes, intErr, extRes, extErr); err != nil {
			// Use non-blocking send to avoid goroutine leaks
			select {
			case fatalErrChan <- fmt.Errorf("compareExecutionClient %s: %w", op, err):
				// Successfully sent - this is a fatal operation
				promise.ProduceError(err)
			default:
				// Could not send (nil channel or full) - treat as non-fatal
				log.Error("Non-fatal comparison error", "operation", op, "err", err)
				promise.Produce(intRes)
			}
		} else {
			promise.Produce(intRes)
		}
	}()
	return &promise
}

const bootstrapErrorMsg = "Failed to get latest header"

// clientInfo holds information about an execution client for synchronization
type clientInfo struct {
	client execution.ExecutionClient
	head   arbutil.MessageIndex
	name   string
}

func (w *compareExecutionClient) isBootstrapCase(intErr, extErr error) bool {
	// Check if external client has "Failed to get latest header" error (uninitialized)
	// and internal client has a valid response (initialized)
	if intErr != nil || extErr == nil {
		return false
	}

	// Bootstrap case: external client is uninitialized and internal client is initialized
	return strings.Contains(extErr.Error(), bootstrapErrorMsg)
}

// handleBootstrapInitialization initializes the external Nethermind client using DigestInitMessage
func (w *compareExecutionClient) handleBootstrapInitialization(ctx context.Context, intRes arbutil.MessageIndex) error {
	w.syncMutex.Lock()
	defer w.syncMutex.Unlock()

	log.Info("Bootstrap: External Nethermind client is uninitialized, internal Geth client is initialized",
		"internalHead", intRes)

	if w.consensus == nil {
		return fmt.Errorf("consensus client not available for bootstrap initialization")
	}

	// Cast consensus to get access to transaction streamer
	arbNode, ok := w.consensus.(*arbnode.Node)
	if !ok {
		return fmt.Errorf("consensus client is not an arbnode.Node, cannot access init message")
	}

	// Get the original init message at index 0 to extract the correct InitialL1BaseFee
	initMessage, err := arbNode.TxStreamer.GetMessage(0)
	if err != nil {
		return fmt.Errorf("failed to get init message from consensus: %w", err)
	}

	// Parse the init message to get the original InitialL1BaseFee and chain config
	parsedInitMessage, err := initMessage.Message.ParseInitMessage()
	if err != nil {
		return fmt.Errorf("failed to parse init message: %w", err)
	}

	log.Info("Bootstrap: Initializing external Nethermind client with DigestInitMessage",
		"chainId", parsedInitMessage.ChainId,
		"initialL1BaseFee", parsedInitMessage.InitialL1BaseFee,
		"configSize", len(parsedInitMessage.SerializedChainConfig))

	// Call DigestInitMessage on the external Nethermind client with the original parameters
	result, err := w.nethermindExecutionClient.DigestInitMessage(ctx, parsedInitMessage.InitialL1BaseFee, parsedInitMessage.SerializedChainConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize external Nethermind client with DigestInitMessage: %w", err)
	}

	log.Info("Bootstrap: Successfully initialized external Nethermind client",
		"result", result)

	return nil
}

func compare[T any](op string, intRes T, intErr error, extRes T, extErr error) error {
	switch {
	case intErr != nil && extErr != nil:
		return fmt.Errorf("both operations failed: internal=%v external=%v", intErr, extErr)
	case intErr != nil:
		return fmt.Errorf("internal operation failed: %v", intErr)
	case extErr != nil:
		return fmt.Errorf("external operation failed: %v", extErr)
	default:
		if !cmp.Equal(intRes, extRes) {
			opts := cmp.Options{
				cmp.Transformer("HashHex", func(h common.Hash) string { return h.Hex() }),
			}
			diff := cmp.Diff(intRes, extRes, opts)
			// Log the detailed diff using fmt.Printf to avoid escaping
			fmt.Printf("ERROR: Execution mismatch detected in operation: %s\n", op)
			fmt.Printf("Diff details:\n%s\n", diff)
			return fmt.Errorf("execution mismatch in %s", op)
		}
	}
	return nil
}

// synchronizeExecutionClients attempts to bring both execution clients to the same head message index
// by replaying missing messages from the consensus client to the lagging client.
func (w *compareExecutionClient) synchronizeExecutionClients(ctx context.Context, internalHead, externalHead arbutil.MessageIndex) error {
	// Early return if heads are equal
	if internalHead == externalHead {
		return nil
	}

	w.syncMutex.Lock()
	defer w.syncMutex.Unlock()

	if w.consensus == nil {
		return fmt.Errorf("consensus client not available for synchronization")
	}

	// Cast consensus to get access to transaction streamer
	arbNode, ok := w.consensus.(*arbnode.Node)
	if !ok {
		return fmt.Errorf("consensus client is not an arbnode.Node, cannot access message data")
	}

	// Determine which client is lagging
	var lagging, leading clientInfo
	if internalHead > externalHead {
		lagging = clientInfo{w.nethermindExecutionClient, externalHead, "external (Nethermind)"}
		leading = clientInfo{w.gethExecutionClient, internalHead, "internal (Geth)"}
		log.Info("Synchronization: External client is behind internal client",
			"externalHead", externalHead,
			"internalHead", internalHead,
			"messageGap", internalHead-externalHead)
	} else {
		lagging = clientInfo{w.gethExecutionClient, internalHead, "internal (Geth)"}
		leading = clientInfo{w.nethermindExecutionClient, externalHead, "external (Nethermind)"}
		log.Info("Synchronization: Internal client is behind external client",
			"internalHead", internalHead,
			"externalHead", externalHead,
			"messageGap", externalHead-internalHead)
	}

	// Check if consensus client has the required messages
	consensusHeadIdx, err := arbNode.TxStreamer.GetHeadMessageIndex()
	if err != nil {
		log.Warn("Synchronization: Failed to get consensus head message index", "err", err)
		return fmt.Errorf("failed to get consensus head message index: %w", err)
	}

	// If consensus doesn't have all the messages we need, we can't synchronize yet
	if consensusHeadIdx < leading.head {
		log.Info("Synchronization: Consensus client doesn't have all required messages yet, skipping synchronization",
			"consensusHead", consensusHeadIdx,
			"leadingHead", leading.head,
			"client", lagging.name,
			"messageGap", leading.head-lagging.head)
		return fmt.Errorf("consensus client only has messages up to %d, but need messages up to %d", consensusHeadIdx, leading.head)
	}

	return w.replayMessages(ctx, arbNode, lagging, leading)
}

// replayMessages replays messages from lagging client to leading client head
func (w *compareExecutionClient) replayMessages(ctx context.Context, arbNode *arbnode.Node, lagging, leading clientInfo) error {
	messagesToReplay := leading.head - lagging.head
	log.Info("Synchronization: Starting message replay",
		"client", lagging.name,
		"fromIndex", lagging.head+1,
		"toIndex", leading.head,
		"messageCount", messagesToReplay)

	syncStart := time.Now()
	var successfulReplays arbutil.MessageIndex

	for msgIdx := lagging.head + 1; msgIdx <= leading.head; msgIdx++ {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return fmt.Errorf("synchronization cancelled: %w", ctx.Err())
		default:
		}

		msg, err := arbNode.TxStreamer.GetMessage(msgIdx)
		if err != nil {
			log.Error("Synchronization: Failed to retrieve message from consensus",
				"messageIndex", msgIdx, "err", err)
			return fmt.Errorf("failed to get message %d from consensus: %w", msgIdx, err)
		}

		// Replay message on lagging client
		log.Debug("Synchronization: Replaying message", "client", lagging.name, "messageIndex", msgIdx)

		laggingResult := lagging.client.DigestMessage(msgIdx, msg, nil)
		leadingResult := leading.client.ResultAtMessageIndex(msgIdx)

		result := comparePromises(
			w.fatalErrChan,
			fmt.Sprintf("Synchronization: leading client %s and lagging client %s", leading.name, lagging.name),
			leadingResult,
			laggingResult,
		)

		if _, err = result.Await(ctx); err != nil {
			log.Error("Synchronization: Failed to validate message result", "messageIndex", msgIdx, "err", err)
			return fmt.Errorf("failed to validate message result: %w", err)
		}

		successfulReplays++

		// Log progress every 10 messages or at the end
		if successfulReplays%10 == 0 || msgIdx == leading.head {
			log.Info("Synchronization: Progress update",
				"client", lagging.name,
				"replayed", successfulReplays,
				"total", messagesToReplay,
				"currentIndex", msgIdx,
				"elapsed", time.Since(syncStart))
		}
	}

	log.Info("Synchronization: Message replay completed successfully",
		"client", lagging.name,
		"replayedMessages", successfulReplays,
		"totalElapsed", time.Since(syncStart))

	return nil
}

// isFatalSyncError determines if a synchronization error should cause fatal shutdown
func (w *compareExecutionClient) isFatalSyncError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "failed to validate message result")
}

func (w *compareExecutionClient) compareHeadMessageIndexWithSync(
	ctx context.Context,
	internal containers.PromiseInterface[arbutil.MessageIndex],
	external containers.PromiseInterface[arbutil.MessageIndex],
) containers.PromiseInterface[arbutil.MessageIndex] {
	promise := containers.NewPromise[arbutil.MessageIndex](nil)
	go func() {
		awaitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()

		intRes, intErr := internal.Await(awaitCtx)
		extRes, extErr := external.Await(awaitCtx)

		if w.isBootstrapCase(intErr, extErr) {
			log.Info("Bootstrap case detected: attempting to initialize external Nethermind client")
			if bootstrapErr := w.handleBootstrapInitialization(ctx, intRes); bootstrapErr != nil {
				log.Error("Bootstrap initialization failed", "err", bootstrapErr)
				// Continue with normal comparison to log the error
			} else {
				log.Info("Bootstrap initialization successful, will retry on next HeadMessageIndex call")
				// Return the internal result for now, next call should work
				promise.Produce(intRes)
				return
			}
		}

		if intErr == nil && extErr == nil && intRes != extRes {
			log.Warn("Synchronization: Head message index mismatch detected",
				"internalHead", intRes,
				"externalHead", extRes,
				"attempting", "synchronization")

			// Attempt synchronization
			if syncErr := w.synchronizeExecutionClients(ctx, intRes, extRes); syncErr != nil {
				if w.isFatalSyncError(syncErr) {
					log.Error("Synchronization: Execution mismatch detected during synchronization - stopping", "err", syncErr)
					// Send to fatal error channel for graceful shutdown
					select {
					case w.fatalErrChan <- fmt.Errorf("compareExecutionClient synchronization: %w", syncErr):
					default:
						log.Error("Failed to send synchronization error to fatal channel", "err", syncErr)
					}
					promise.ProduceError(syncErr)
					return
				}

				log.Warn("Synchronization: Cannot synchronize execution clients at this time",
					"err", syncErr, "reason", "will retry when consensus client catches up")
			} else {
				log.Info("Synchronization: Successfully synchronized execution clients")
				// Return the leading head as the result since both should now be synchronized
				leadingHead := intRes
				if extRes > intRes {
					leadingHead = extRes
				}
				promise.Produce(leadingHead)
				return
			}
		}

		// Perform normal comparison
		if err := compare("HeadMessageIndex", intRes, intErr, extRes, extErr); err != nil {
			log.Error("Non-fatal comparison error", "operation", "HeadMessageIndex", "err", err)
			promise.Produce(intRes)
		} else {
			promise.Produce(intRes)
		}
	}()
	return &promise
}

func (w *compareExecutionClient) DigestMessage(index arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata, msgForPrefetch *arbostypes.MessageWithMetadata) containers.PromiseInterface[*execution.MessageResult] {
	start := time.Now()
	log.Info("CompareExecutionClient: DigestMessage", "index", index)
	internal := w.gethExecutionClient.DigestMessage(index, msg, msgForPrefetch)
	external := w.nethermindExecutionClient.DigestMessage(index, msg, msgForPrefetch)

	result := comparePromises(w.fatalErrChan,
		"DigestMessage",
		internal,
		external,
	)
	log.Info("CompareExecutionClient: DigestMessage completed", "index", index, "elapsed", time.Since(start))
	return result
}

func (w *compareExecutionClient) Reorg(count arbutil.MessageIndex, newMessages []arbostypes.MessageWithMetadataAndBlockInfo, oldMessages []*arbostypes.MessageWithMetadata) containers.PromiseInterface[[]*execution.MessageResult] {
	start := time.Now()
	log.Info("CompareExecutionClient: Reorg", "count", count, "newMessagesCount", len(newMessages), "oldMessagesCount", len(oldMessages))

	internal := w.gethExecutionClient.Reorg(count, newMessages, oldMessages)
	external := w.nethermindExecutionClient.Reorg(count, newMessages, oldMessages)

	result := comparePromises(w.fatalErrChan, "Reorg", internal, external)
	log.Info("CompareExecutionClient: Reorg completed", "count", count, "elapsed", time.Since(start))
	return result
}

func (w *compareExecutionClient) HeadMessageIndex() containers.PromiseInterface[arbutil.MessageIndex] {
	start := time.Now()
	log.Info("CompareExecutionClient: HeadMessageIndex")
	internal := w.gethExecutionClient.HeadMessageIndex()
	external := w.nethermindExecutionClient.HeadMessageIndex()

	// Use synchronization-aware comparison for HeadMessageIndex
	result := w.compareHeadMessageIndexWithSync(context.Background(), internal, external)
	log.Info("CompareExecutionClient: HeadMessageIndex completed", "elapsed", time.Since(start))
	return result
}

func (w *compareExecutionClient) ResultAtMessageIndex(index arbutil.MessageIndex) containers.PromiseInterface[*execution.MessageResult] {
	w.syncMutex.Lock()
	defer w.syncMutex.Unlock()

	start := time.Now()
	log.Info("CompareExecutionClient: ResultAtMessageIndex", "index", index)
	internal := w.gethExecutionClient.ResultAtMessageIndex(index)
	external := w.nethermindExecutionClient.ResultAtMessageIndex(index)
	result := comparePromises(nil, "ResultAtMessageIndex", internal, external)
	log.Info("CompareExecutionClient: ResultAtMessageIndex completed", "index", index, "elapsed", time.Since(start))
	return result
}

func (w *compareExecutionClient) MessageIndexToBlockNumber(messageIndex arbutil.MessageIndex) containers.PromiseInterface[uint64] {
	start := time.Now()
	log.Info("CompareExecutionClient: MessageIndexToBlockNumber", "messageIndex", messageIndex)
	internal := w.gethExecutionClient.MessageIndexToBlockNumber(messageIndex)
	external := w.nethermindExecutionClient.MessageIndexToBlockNumber(messageIndex)
	result := comparePromises(w.fatalErrChan, "MessageIndexToBlockNumber", internal, external)
	log.Info("CompareExecutionClient: MessageIndexToBlockNumber completed", "messageIndex", messageIndex, "elapsed", time.Since(start))
	return result
}

func (w *compareExecutionClient) BlockNumberToMessageIndex(blockNum uint64) containers.PromiseInterface[arbutil.MessageIndex] {
	start := time.Now()
	log.Info("CompareExecutionClient: BlockNumberToMessageIndex", "blockNum", blockNum)
	internal := w.gethExecutionClient.BlockNumberToMessageIndex(blockNum)
	external := w.nethermindExecutionClient.BlockNumberToMessageIndex(blockNum)
	result := comparePromises(w.fatalErrChan, "BlockNumberToMessageIndex", internal, external)
	log.Info("CompareExecutionClient: BlockNumberToMessageIndex completed", "blockNum", blockNum, "elapsed", time.Since(start))
	return result
}

func (w *compareExecutionClient) SetFinalityData(ctx context.Context, finalityData *arbutil.FinalityData, finalizedFinalityData *arbutil.FinalityData, validatedFinalityData *arbutil.FinalityData) containers.PromiseInterface[struct{}] {
	log.Info("CompareExecutionClient: SetFinalityData",
		"safeFinalityData", finalityData,
		"finalizedFinalityData", finalizedFinalityData,
		"validatedFinalityData", validatedFinalityData)

	internal := w.gethExecutionClient.SetFinalityData(ctx, finalityData, finalizedFinalityData, validatedFinalityData)
	external := w.nethermindExecutionClient.SetFinalityData(ctx, finalityData, finalizedFinalityData, validatedFinalityData)
	return comparePromises(w.fatalErrChan, "SetFinalityData", internal, external)
}

func (w *compareExecutionClient) MarkFeedStart(to arbutil.MessageIndex) containers.PromiseInterface[struct{}] {
	start := time.Now()
	log.Info("CompareExecutionClient: MarkFeedStart", "to", to)
	internal := w.gethExecutionClient.MarkFeedStart(to)
	external := w.nethermindExecutionClient.MarkFeedStart(to)
	result := comparePromises(w.fatalErrChan, "MarkFeedStart", internal, external)
	log.Info("CompareExecutionClient: MarkFeedStart completed", "to", to, "elapsed", time.Since(start))
	return result
}

func (w *compareExecutionClient) TriggerMaintenance() containers.PromiseInterface[struct{}] {
	start := time.Now()
	log.Info("CompareExecutionClient: TriggerMaintenance")
	result := w.gethExecutionClient.TriggerMaintenance()
	log.Info("CompareExecutionClient: TriggerMaintenance completed", "elapsed", time.Since(start))
	return result
}

func (w *compareExecutionClient) ShouldTriggerMaintenance() containers.PromiseInterface[bool] {
	start := time.Now()
	log.Info("CompareExecutionClient: ShouldTriggerMaintenance")
	internal := w.gethExecutionClient.ShouldTriggerMaintenance()
	external := w.nethermindExecutionClient.ShouldTriggerMaintenance()
	result := comparePromises(w.fatalErrChan, "ShouldTriggerMaintenance", internal, external)
	log.Info("CompareExecutionClient: ShouldTriggerMaintenance completed", "elapsed", time.Since(start))
	return result
}

func (w *compareExecutionClient) MaintenanceStatus() containers.PromiseInterface[*execution.MaintenanceStatus] {
	start := time.Now()
	log.Info("CompareExecutionClient: MaintenanceStatus")
	internal := w.gethExecutionClient.MaintenanceStatus()
	external := w.nethermindExecutionClient.MaintenanceStatus()
	result := comparePromises(w.fatalErrChan, "MaintenanceStatus", internal, external)
	log.Info("CompareExecutionClient: MaintenanceStatus completed", "elapsed", time.Since(start))
	return result
}

func (w *compareExecutionClient) Start(ctx context.Context) error {
	start := time.Now()
	log.Info("CompareExecutionClient: Start")
	err := w.gethExecutionClient.Start(ctx)
	log.Info("CompareExecutionClient: Start completed", "elapsed", time.Since(start))
	return err
}

func (w *compareExecutionClient) StopAndWait() {
	start := time.Now()
	log.Info("CompareExecutionClient: StopAndWait")
	w.gethExecutionClient.StopAndWait()
	log.Info("CompareExecutionClient: StopAndWait completed", "elapsed", time.Since(start))
}

// ---- execution.ExecutionSequencer interface methods ----

func (w *compareExecutionClient) Pause() {
	start := time.Now()
	log.Info("CompareExecutionClient: Pause")
	w.gethExecutionClient.Pause()
	log.Info("CompareExecutionClient: Pause completed", "elapsed", time.Since(start))
}

func (w *compareExecutionClient) Activate() {
	start := time.Now()
	log.Info("CompareExecutionClient: Activate")
	w.gethExecutionClient.Activate()
	log.Info("CompareExecutionClient: Activate completed", "elapsed", time.Since(start))
}

func (w *compareExecutionClient) ForwardTo(url string) error {
	start := time.Now()
	log.Info("CompareExecutionClient: ForwardTo", "url", url)
	err := w.gethExecutionClient.ForwardTo(url)
	log.Info("CompareExecutionClient: ForwardTo completed", "url", url, "err", err, "elapsed", time.Since(start))
	return err
}

func (w *compareExecutionClient) SequenceDelayedMessage(message *arbostypes.L1IncomingMessage, delayedSeqNum uint64) error {
	start := time.Now()
	log.Info("CompareExecutionClient: SequenceDelayedMessage", "delayedSeqNum", delayedSeqNum)

	internalErr := w.gethExecutionClient.SequenceDelayedMessage(message, delayedSeqNum)
	externalErr := w.nethermindExecutionClient.SequenceDelayedMessage(message, delayedSeqNum)

	if err := compare("SequenceDelayedMessage", struct{}{}, internalErr, struct{}{}, externalErr); err != nil {
		// Send to fatal error channel for graceful shutdown
		select {
		case w.fatalErrChan <- fmt.Errorf("compareExecutionClient SequenceDelayedMessage: %w", err):
		default:
			log.Error("Failed to send comparison error to fatal channel", "err", err)
		}
		return err
	}

	log.Info("CompareExecutionClient: SequenceDelayedMessage completed",
		"delayedSeqNum", delayedSeqNum, "err", internalErr, "elapsed", time.Since(start))
	return internalErr
}

func (w *compareExecutionClient) NextDelayedMessageNumber() (uint64, error) {
	// start := time.Now()
	// log.Info("CompareExecutionClient: NextDelayedMessageNumber")
	result, err := w.gethExecutionClient.NextDelayedMessageNumber()
	// log.Info("CompareExecutionClient: NextDelayedMessageNumber completed", "result", result, "err", err, "elapsed", time.Since(start))
	return result, err
}

func (w *compareExecutionClient) Synced(ctx context.Context) bool {
	start := time.Now()
	log.Info("CompareExecutionClient: Synced")
	result := w.gethExecutionClient.Synced(ctx)
	log.Info("CompareExecutionClient: Synced completed", "result", result, "elapsed", time.Since(start))
	return result
}

func (w *compareExecutionClient) FullSyncProgressMap(ctx context.Context) map[string]interface{} {
	start := time.Now()
	log.Info("CompareExecutionClient: FullSyncProgressMap")
	result := w.gethExecutionClient.FullSyncProgressMap(ctx)
	log.Info("CompareExecutionClient: FullSyncProgressMap completed", "elapsed", time.Since(start))
	return result
}

// ---- execution.ExecutionRecorder interface methods ----

func (w *compareExecutionClient) RecordBlockCreation(ctx context.Context, index arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata) (*execution.RecordResult, error) {
	start := time.Now()
	log.Info("CompareExecutionClient: RecordBlockCreation", "index", index)
	result, err := w.gethExecutionClient.RecordBlockCreation(ctx, index, msg)
	log.Info("CompareExecutionClient: RecordBlockCreation completed", "index", index, "err", err, "elapsed", time.Since(start))
	return result, err
}

func (w *compareExecutionClient) MarkValid(index arbutil.MessageIndex, resultHash common.Hash) {
	start := time.Now()
	log.Info("CompareExecutionClient: MarkValid", "index", index, "resultHash", resultHash)
	w.gethExecutionClient.MarkValid(index, resultHash)
	log.Info("CompareExecutionClient: MarkValid completed", "index", index, "elapsed", time.Since(start))
}

func (w *compareExecutionClient) PrepareForRecord(ctx context.Context, start, end arbutil.MessageIndex) error {
	startTime := time.Now()
	log.Info("CompareExecutionClient: PrepareForRecord", "start", start, "end", end)
	err := w.gethExecutionClient.PrepareForRecord(ctx, start, end)
	log.Info("CompareExecutionClient: PrepareForRecord completed", "start", start, "end", end, "err", err, "elapsed", time.Since(startTime))
	return err
}

// ---- execution.ExecutionBatchindexter interface methods ----

func (w *compareExecutionClient) ArbOSVersionForMessageIndex(msgIdx arbutil.MessageIndex) (uint64, error) {
	start := time.Now()
	log.Info("CompareExecutionClient: ArbOSVersionForMessageIndex", "msgIdx", msgIdx)
	result, err := w.gethExecutionClient.ArbOSVersionForMessageIndex(msgIdx)
	log.Info("CompareExecutionClient: ArbOSVersionForMessageIndex completed", "msgIdx", msgIdx, "result", result, "err", err, "elapsed", time.Since(start))
	return result, err
}

func (w *compareExecutionClient) SetConsensusClient(consensus execution.FullConsensusClient) {
	w.consensus = consensus
	w.gethExecutionClient.SetConsensusClient(consensus)
}

func (w *compareExecutionClient) Initialize(ctx context.Context) error {
	return w.gethExecutionClient.Initialize(ctx)
}
