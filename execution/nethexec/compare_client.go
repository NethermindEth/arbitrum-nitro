package nethexec

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
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
	_ FullExecutionClient         = (*CompareExecutionClient)(nil)
	_ arbnode.ExecutionNodeBridge = (*CompareExecutionClient)(nil)
)

type CompareExecutionClient struct {
	gethExecutionClient       *gethexec.ExecutionNode
	nethermindExecutionClient *NethermindExecutionClient
	maxMsgsToDigest           uint64
}

func NewCompareExecutionClient(gethExecutionClient *gethexec.ExecutionNode, nethermindExecutionClient *NethermindExecutionClient) *CompareExecutionClient {
	maxMsgsToDigest, err := strconv.ParseUint(os.Getenv("PR_MAX_MESSAGES_TO_DIGEST"), 10, 64)
	if err != nil {
		log.Warn("Wasn't able to read PR_MAX_MESSAGES_TO_DIGEST, setting to max value", "error", err)
		maxMsgsToDigest = math.MaxUint64
	}

	return &CompareExecutionClient{
		gethExecutionClient:       gethExecutionClient,
		nethermindExecutionClient: nethermindExecutionClient,
		maxMsgsToDigest:           maxMsgsToDigest,
	}
}

func (w *CompareExecutionClient) DigestMessage(num arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata, msgForPrefetch *arbostypes.MessageWithMetadata) containers.PromiseInterface[*execution.MessageResult] {
	promise := containers.NewPromise[*execution.MessageResult](nil)
	go func() {
		internalPromise := w.gethExecutionClient.DigestMessage(num, msg, msgForPrefetch)
		externalPromise := w.nethermindExecutionClient.DigestMessage(num, msg, msgForPrefetch)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		intRes, err := internalPromise.Await(ctx)
		if err != nil || intRes == nil {
			log.Warn("CompareExecutionClient: comparison skipped due to internal err/nil", "err", err, "intNil", intRes == nil)
			promise.ProduceError(fmt.Errorf("internal DigestMessage returned nil"))
		}
		extRes, eerr := externalPromise.Await(ctx)
		if eerr != nil || extRes == nil {
			log.Warn("CompareExecutionClient: comparison skipped due to external err/nil", "err", eerr, "extNil", extRes == nil)
			promise.ProduceError(fmt.Errorf("external DigestMessage returned nil"))
		}
		if intRes.BlockHash != extRes.BlockHash || intRes.SendRoot != extRes.SendRoot {
			details := fmt.Sprintf("\n  num: %d\n  BlockHash:\n    internal: %s\n    external: %s\n  SendRoot:\n    internal: %s\n    external: %s",
				num,
				intRes.BlockHash.Hex(), extRes.BlockHash.Hex(),
				intRes.SendRoot.Hex(), extRes.SendRoot.Hex(),
			)
			log.Error(
				"Execution mismatch between internal and external",
				"num", num,
				"internalBlock", intRes.BlockHash.Hex(),
				"externalBlock", extRes.BlockHash.Hex(),
				"internalSendRoot", intRes.SendRoot.Hex(),
				"externalSendRoot", extRes.SendRoot.Hex(),
				"details", details,
			)
		} else {
			log.Info("Execution match verified", "num", num, "block", intRes.BlockHash)
		}
		promise.Produce(intRes)
	}()
	return &promise

}

func (w *CompareExecutionClient) Reorg(count arbutil.MessageIndex, newMessages []arbostypes.MessageWithMetadataAndBlockInfo, oldMessages []*arbostypes.MessageWithMetadata) containers.PromiseInterface[[]*execution.MessageResult] {
	start := time.Now()
	log.Info("CompareExecutionClient: Reorg", "count", count, "newMessagesCount", len(newMessages), "oldMessagesCount", len(oldMessages))
	result := w.gethExecutionClient.Reorg(count, newMessages, oldMessages)
	log.Info("CompareExecutionClient: Reorg completed", "count", count, "elapsed", time.Since(start))
	return result
}

func (w *CompareExecutionClient) HeadMessageIndex() containers.PromiseInterface[arbutil.MessageIndex] {
	// start := time.Now()
	// log.Info("CompareExecutionClient: HeadMessageIndex")
	result := w.gethExecutionClient.HeadMessageIndex()
	// log.Info("CompareExecutionClient: HeadMessageIndex completed", "elapsed", time.Since(start))
	return result
}

func (w *CompareExecutionClient) ResultAtMessageIndex(pos arbutil.MessageIndex) containers.PromiseInterface[*execution.MessageResult] {
	start := time.Now()
	log.Info("CompareExecutionClient: ResultAtMessageIndex", "pos", pos)
	result := w.gethExecutionClient.ResultAtMessageIndex(pos)
	log.Info("CompareExecutionClient: ResultAtMessageIndex completed", "pos", pos, "elapsed", time.Since(start))
	return result
}

func (w *CompareExecutionClient) MessageIndexToBlockNumber(messageNum arbutil.MessageIndex) containers.PromiseInterface[uint64] {
	start := time.Now()
	log.Info("CompareExecutionClient: MessageIndexToBlockNumber", "messageNum", messageNum)
	result := w.gethExecutionClient.MessageIndexToBlockNumber(messageNum)
	log.Info("CompareExecutionClient: MessageIndexToBlockNumber completed", "messageNum", messageNum, "elapsed", time.Since(start))
	return result
}

func (w *CompareExecutionClient) BlockNumberToMessageIndex(blockNum uint64) containers.PromiseInterface[arbutil.MessageIndex] {
	start := time.Now()
	log.Info("CompareExecutionClient: BlockNumberToMessageIndex", "blockNum", blockNum)
	result := w.gethExecutionClient.BlockNumberToMessageIndex(blockNum)
	log.Info("CompareExecutionClient: BlockNumberToMessageIndex completed", "blockNum", blockNum, "elapsed", time.Since(start))
	return result
}

func (w *CompareExecutionClient) SetFinalityData(ctx context.Context, finalityData *arbutil.FinalityData, finalizedFinalityData *arbutil.FinalityData, validatedFinalityData *arbutil.FinalityData) containers.PromiseInterface[struct{}] {
	log.Info("CompareExecutionClient: SetFinalityData",
		"safeFinalityData", finalityData,
		"finalizedFinalityData", finalizedFinalityData,
		"validatedFinalityData", validatedFinalityData)

	promise := containers.NewPromise[struct{}](nil)
	go func() {
		internalPromise := w.gethExecutionClient.SetFinalityData(ctx, finalityData, finalizedFinalityData, validatedFinalityData)
		externalPromise := w.nethermindExecutionClient.SetFinalityData(ctx, finalityData, finalizedFinalityData, validatedFinalityData)
		// nothing to compare other than success; Await internal to ensure completion
		ctx2, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		_, internalErr := internalPromise.Await(ctx2)
		_, externalErr := externalPromise.Await(ctx2)
		// Compare errors
		if internalErr != externalErr {
			promise.ProduceError(fmt.Errorf("SetFinalityData failed: internalErr=%v, externalErr=%v", internalErr, externalErr))
			return
		}
		if internalErr != nil {
			promise.ProduceError(internalErr)
			return
		}
		promise.Produce(struct{}{})
	}()
	return &promise
}

func (w *CompareExecutionClient) MarkFeedStart(to arbutil.MessageIndex) containers.PromiseInterface[struct{}] {
	start := time.Now()
	log.Info("CompareExecutionClient: MarkFeedStart", "to", to)
	result := w.gethExecutionClient.MarkFeedStart(to)
	log.Info("CompareExecutionClient: MarkFeedStart completed", "to", to, "elapsed", time.Since(start))
	return result
}

func (w *CompareExecutionClient) Maintenance() containers.PromiseInterface[struct{}] {
	start := time.Now()
	log.Info("CompareExecutionClient: Maintenance")
	result := w.gethExecutionClient.Maintenance()
	log.Info("CompareExecutionClient: Maintenance completed", "elapsed", time.Since(start))
	return result
}

func (w *CompareExecutionClient) Start(ctx context.Context) error {
	start := time.Now()
	log.Info("CompareExecutionClient: Start")
	error := w.gethExecutionClient.Start(ctx)
	log.Info("CompareExecutionClient: Start completed", "elapsed", time.Since(start))
	return error
}

func (w *CompareExecutionClient) StopAndWait() {
	start := time.Now()
	log.Info("CompareExecutionClient: StopAndWait")
	w.gethExecutionClient.StopAndWait()
	log.Info("CompareExecutionClient: StopAndWait completed", "elapsed", time.Since(start))
}

// ---- execution.ExecutionSequencer interface methods ----

func (w *CompareExecutionClient) Pause() {
	start := time.Now()
	log.Info("CompareExecutionClient: Pause")
	w.gethExecutionClient.Pause()
	log.Info("CompareExecutionClient: Pause completed", "elapsed", time.Since(start))
}

func (w *CompareExecutionClient) Activate() {
	start := time.Now()
	log.Info("CompareExecutionClient: Activate")
	w.gethExecutionClient.Activate()
	log.Info("CompareExecutionClient: Activate completed", "elapsed", time.Since(start))
}

func (w *CompareExecutionClient) ForwardTo(url string) error {
	start := time.Now()
	log.Info("CompareExecutionClient: ForwardTo", "url", url)
	err := w.gethExecutionClient.ForwardTo(url)
	log.Info("CompareExecutionClient: ForwardTo completed", "url", url, "err", err, "elapsed", time.Since(start))
	return err
}

func (w *CompareExecutionClient) SequenceDelayedMessage(message *arbostypes.L1IncomingMessage, delayedSeqNum uint64) error {
	start := time.Now()
	log.Info("CompareExecutionClient: SequenceDelayedMessage", "delayedSeqNum", delayedSeqNum)
	err := w.gethExecutionClient.SequenceDelayedMessage(message, delayedSeqNum)
	log.Info("CompareExecutionClient: SequenceDelayedMessage completed", "delayedSeqNum", delayedSeqNum, "err", err, "elapsed", time.Since(start))
	return err
}

func (w *CompareExecutionClient) NextDelayedMessageNumber() (uint64, error) {
	// start := time.Now()
	// log.Info("CompareExecutionClient: NextDelayedMessageNumber")
	result, err := w.gethExecutionClient.NextDelayedMessageNumber()
	// log.Info("CompareExecutionClient: NextDelayedMessageNumber completed", "result", result, "err", err, "elapsed", time.Since(start))
	return result, err
}

func (w *CompareExecutionClient) Synced(ctx context.Context) bool {
	start := time.Now()
	log.Info("CompareExecutionClient: Synced")
	result := w.gethExecutionClient.Synced(ctx)
	log.Info("CompareExecutionClient: Synced completed", "result", result, "elapsed", time.Since(start))
	return result
}

func (w *CompareExecutionClient) FullSyncProgressMap(ctx context.Context) map[string]interface{} {
	start := time.Now()
	log.Info("CompareExecutionClient: FullSyncProgressMap")
	result := w.gethExecutionClient.FullSyncProgressMap(ctx)
	log.Info("CompareExecutionClient: FullSyncProgressMap completed", "elapsed", time.Since(start))
	return result
}

// ---- execution.ExecutionRecorder interface methods ----

func (w *CompareExecutionClient) RecordBlockCreation(ctx context.Context, pos arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata) (*execution.RecordResult, error) {
	start := time.Now()
	log.Info("CompareExecutionClient: RecordBlockCreation", "pos", pos)
	result, err := w.gethExecutionClient.RecordBlockCreation(ctx, pos, msg)
	log.Info("CompareExecutionClient: RecordBlockCreation completed", "pos", pos, "err", err, "elapsed", time.Since(start))
	return result, err
}

func (w *CompareExecutionClient) MarkValid(pos arbutil.MessageIndex, resultHash common.Hash) {
	start := time.Now()
	log.Info("CompareExecutionClient: MarkValid", "pos", pos, "resultHash", resultHash)
	w.gethExecutionClient.MarkValid(pos, resultHash)
	log.Info("CompareExecutionClient: MarkValid completed", "pos", pos, "elapsed", time.Since(start))
}

func (w *CompareExecutionClient) PrepareForRecord(ctx context.Context, start, end arbutil.MessageIndex) error {
	startTime := time.Now()
	log.Info("CompareExecutionClient: PrepareForRecord", "start", start, "end", end)
	err := w.gethExecutionClient.PrepareForRecord(ctx, start, end)
	log.Info("CompareExecutionClient: PrepareForRecord completed", "start", start, "end", end, "err", err, "elapsed", time.Since(startTime))
	return err
}

// ---- execution.ExecutionBatchPoster interface methods ----

func (w *CompareExecutionClient) ArbOSVersionForMessageIndex(msgIdx arbutil.MessageIndex) (uint64, error) {
	start := time.Now()
	log.Info("CompareExecutionClient: ArbOSVersionForMessageIndex", "msgIdx", msgIdx)
	result, err := w.gethExecutionClient.ArbOSVersionForMessageIndex(msgIdx)
	log.Info("CompareExecutionClient: ArbOSVersionForMessageIndex completed", "msgIdx", msgIdx, "result", result, "err", err, "elapsed", time.Since(start))
	return result, err
}

func (w *CompareExecutionClient) SetConsensusClient(consensus execution.FullConsensusClient) {
	w.gethExecutionClient.SetConsensusClient(consensus)
}

func (w *CompareExecutionClient) Initialize(ctx context.Context) error {
	return w.gethExecutionClient.Initialize(ctx)
}
