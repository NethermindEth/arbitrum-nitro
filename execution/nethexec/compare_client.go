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
	_ FullExecutionClient         = (*compareExecutionClient)(nil)
	_ arbnode.ExecutionNodeBridge = (*compareExecutionClient)(nil)
)

type compareExecutionClient struct {
	gethExecutionClient       *gethexec.ExecutionNode
	nethermindExecutionClient *nethermindExecutionClient
	maxMsgsToDigest           uint64
}

func NewCompareExecutionClient(gethExecutionClient *gethexec.ExecutionNode, nethermindExecutionClient *nethermindExecutionClient) *compareExecutionClient {
	maxMsgsToDigest, err := strconv.ParseUint(os.Getenv("PR_MAX_MESSAGES_TO_DIGEST"), 10, 64)
	if err != nil {
		log.Warn("Wasn't able to read PR_MAX_MESSAGES_TO_DIGEST, setting to max value", "error", err)
		maxMsgsToDigest = math.MaxUint64
	}

	return &compareExecutionClient{
		gethExecutionClient:       gethExecutionClient,
		nethermindExecutionClient: nethermindExecutionClient,
		maxMsgsToDigest:           maxMsgsToDigest,
	}
}

func (w *compareExecutionClient) DigestMessage(num arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata, msgForPrefetch *arbostypes.MessageWithMetadata) containers.PromiseInterface[*execution.MessageResult] {
	promise := containers.NewPromise[*execution.MessageResult](nil)
	go func() {
		internalPromise := w.gethExecutionClient.DigestMessage(num, msg, msgForPrefetch)
		externalPromise := w.nethermindExecutionClient.DigestMessage(num, msg, msgForPrefetch)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		intRes, intErr := internalPromise.Await(ctx)
		extRes, extErr := externalPromise.Await(ctx)

		intOK := intErr == nil && intRes != nil
		extOK := extErr == nil && extRes != nil

		switch {
		case !intOK && !extOK:
			log.Error("Both execution digests failed", "num", num, "internalErr", intErr, "externalErr", extErr)
			promise.ProduceError(fmt.Errorf("both DigestMessage calls failed: internal=%v external=%v", intErr, extErr))
			return
		case intOK && !extOK:
			log.Warn("External digest failed; returning internal result", "num", num, "externalErr", extErr)
			promise.Produce(intRes)
			return
		case !intOK && extOK:
			log.Warn("Internal digest failed; returning external result", "num", num, "internalErr", intErr)
			promise.Produce(extRes)
			return
		default:
			// both OK
			if intRes.BlockHash != extRes.BlockHash || intRes.SendRoot != extRes.SendRoot {
				details := fmt.Sprintf("\n  num: %d\n  BlockHash:\n    internal: %s\n    external: %s\n  SendRoot:\n    internal: %s\n    external: %s",
					num,
					intRes.BlockHash.Hex(), extRes.BlockHash.Hex(),
					intRes.SendRoot.Hex(), extRes.SendRoot.Hex(),
				)
				log.Error(
					"Execution mismatch between internal and external",
					"num", num,
					"internalBlock", intRes.BlockHash,
					"externalBlock", extRes.BlockHash,
					"internalSendRoot", intRes.SendRoot,
					"externalSendRoot", extRes.SendRoot,
					"details", details,
				)
			} else {
				log.Info("Execution match verified", "num", num, "block", intRes.BlockHash)
			}
			promise.Produce(intRes)
			return
		}
	}()
	return &promise

}

func (w *compareExecutionClient) Reorg(count arbutil.MessageIndex, newMessages []arbostypes.MessageWithMetadataAndBlockInfo, oldMessages []*arbostypes.MessageWithMetadata) containers.PromiseInterface[[]*execution.MessageResult] {
	start := time.Now()
	log.Info("CompareExecutionClient: Reorg", "count", count, "newMessagesCount", len(newMessages), "oldMessagesCount", len(oldMessages))
	result := w.gethExecutionClient.Reorg(count, newMessages, oldMessages)
	log.Info("CompareExecutionClient: Reorg completed", "count", count, "elapsed", time.Since(start))
	return result
}

func (w *compareExecutionClient) HeadMessageIndex() containers.PromiseInterface[arbutil.MessageIndex] {
	// start := time.Now()
	// log.Info("CompareExecutionClient: HeadMessageIndex")
	result := w.gethExecutionClient.HeadMessageIndex()
	// log.Info("CompareExecutionClient: HeadMessageIndex completed", "elapsed", time.Since(start))
	return result
}

func (w *compareExecutionClient) ResultAtMessageIndex(pos arbutil.MessageIndex) containers.PromiseInterface[*execution.MessageResult] {
	start := time.Now()
	log.Info("CompareExecutionClient: ResultAtMessageIndex", "pos", pos)
	result := w.gethExecutionClient.ResultAtMessageIndex(pos)
	log.Info("CompareExecutionClient: ResultAtMessageIndex completed", "pos", pos, "elapsed", time.Since(start))
	return result
}

func (w *compareExecutionClient) MessageIndexToBlockNumber(messageNum arbutil.MessageIndex) containers.PromiseInterface[uint64] {
	start := time.Now()
	log.Info("CompareExecutionClient: MessageIndexToBlockNumber", "messageNum", messageNum)
	result := w.gethExecutionClient.MessageIndexToBlockNumber(messageNum)
	log.Info("CompareExecutionClient: MessageIndexToBlockNumber completed", "messageNum", messageNum, "elapsed", time.Since(start))
	return result
}

func (w *compareExecutionClient) BlockNumberToMessageIndex(blockNum uint64) containers.PromiseInterface[arbutil.MessageIndex] {
	start := time.Now()
	log.Info("CompareExecutionClient: BlockNumberToMessageIndex", "blockNum", blockNum)
	result := w.gethExecutionClient.BlockNumberToMessageIndex(blockNum)
	log.Info("CompareExecutionClient: BlockNumberToMessageIndex completed", "blockNum", blockNum, "elapsed", time.Since(start))
	return result
}

func (w *compareExecutionClient) SetFinalityData(ctx context.Context, finalityData *arbutil.FinalityData, finalizedFinalityData *arbutil.FinalityData, validatedFinalityData *arbutil.FinalityData) containers.PromiseInterface[struct{}] {
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

func (w *compareExecutionClient) MarkFeedStart(to arbutil.MessageIndex) containers.PromiseInterface[struct{}] {
	start := time.Now()
	log.Info("CompareExecutionClient: MarkFeedStart", "to", to)
	result := w.gethExecutionClient.MarkFeedStart(to)
	log.Info("CompareExecutionClient: MarkFeedStart completed", "to", to, "elapsed", time.Since(start))
	return result
}

func (w *compareExecutionClient) Maintenance() containers.PromiseInterface[struct{}] {
	start := time.Now()
	log.Info("CompareExecutionClient: Maintenance")
	result := w.gethExecutionClient.Maintenance()
	log.Info("CompareExecutionClient: Maintenance completed", "elapsed", time.Since(start))
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
	err := w.gethExecutionClient.SequenceDelayedMessage(message, delayedSeqNum)
	log.Info("CompareExecutionClient: SequenceDelayedMessage completed", "delayedSeqNum", delayedSeqNum, "err", err, "elapsed", time.Since(start))
	return err
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

func (w *compareExecutionClient) RecordBlockCreation(ctx context.Context, pos arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata) (*execution.RecordResult, error) {
	start := time.Now()
	log.Info("CompareExecutionClient: RecordBlockCreation", "pos", pos)
	result, err := w.gethExecutionClient.RecordBlockCreation(ctx, pos, msg)
	log.Info("CompareExecutionClient: RecordBlockCreation completed", "pos", pos, "err", err, "elapsed", time.Since(start))
	return result, err
}

func (w *compareExecutionClient) MarkValid(pos arbutil.MessageIndex, resultHash common.Hash) {
	start := time.Now()
	log.Info("CompareExecutionClient: MarkValid", "pos", pos, "resultHash", resultHash)
	w.gethExecutionClient.MarkValid(pos, resultHash)
	log.Info("CompareExecutionClient: MarkValid completed", "pos", pos, "elapsed", time.Since(start))
}

func (w *compareExecutionClient) PrepareForRecord(ctx context.Context, start, end arbutil.MessageIndex) error {
	startTime := time.Now()
	log.Info("CompareExecutionClient: PrepareForRecord", "start", start, "end", end)
	err := w.gethExecutionClient.PrepareForRecord(ctx, start, end)
	log.Info("CompareExecutionClient: PrepareForRecord completed", "start", start, "end", end, "err", err, "elapsed", time.Since(startTime))
	return err
}

// ---- execution.ExecutionBatchPoster interface methods ----

func (w *compareExecutionClient) ArbOSVersionForMessageIndex(msgIdx arbutil.MessageIndex) (uint64, error) {
	start := time.Now()
	log.Info("CompareExecutionClient: ArbOSVersionForMessageIndex", "msgIdx", msgIdx)
	result, err := w.gethExecutionClient.ArbOSVersionForMessageIndex(msgIdx)
	log.Info("CompareExecutionClient: ArbOSVersionForMessageIndex completed", "msgIdx", msgIdx, "result", result, "err", err, "elapsed", time.Since(start))
	return result, err
}

func (w *compareExecutionClient) SetConsensusClient(consensus execution.FullConsensusClient) {
	w.gethExecutionClient.SetConsensusClient(consensus)
}

func (w *compareExecutionClient) Initialize(ctx context.Context) error {
	return w.gethExecutionClient.Initialize(ctx)
}
