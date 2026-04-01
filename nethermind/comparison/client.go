// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md
package comparison

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"

	"github.com/offchainlabs/nitro/arbos/arbostypes"
	"github.com/offchainlabs/nitro/arbutil"
	"github.com/offchainlabs/nitro/execution"
	"github.com/offchainlabs/nitro/execution/gethexec"
	executionrpcclient "github.com/offchainlabs/nitro/execution/rpcclient"
	"github.com/offchainlabs/nitro/util/containers"
	"github.com/offchainlabs/nitro/util/rpcclient"
)

var (
	_ execution.FullExecutionClient = (*ComparisonClient)(nil)
)

type ComparisonClient struct {
	internal     *gethexec.ExecutionNode
	external     *executionrpcclient.Client
	fatalErrChan chan error
}

func NewComparisonClient(
	internal *gethexec.ExecutionNode,
	externalConfigFetcher rpcclient.ClientConfigFetcher,
	stack *node.Node,
	fatalErrChan chan error,
) *ComparisonClient {
	return &ComparisonClient{
		internal:     internal,
		external:     executionrpcclient.NewClient(externalConfigFetcher, stack),
		fatalErrChan: fatalErrChan,
	}
}

// --- Compared methods (call both, compare, return internal result) ---

func (c *ComparisonClient) DigestMessage(msgIdx arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata, msgForPrefetch *arbostypes.MessageWithMetadata) containers.PromiseInterface[*execution.MessageResult] {
	log.Info("ComparisonClient: DigestMessage", "msgIdx", msgIdx)
	internal := c.internal.DigestMessage(msgIdx, msg, msgForPrefetch)
	external := c.external.DigestMessage(msgIdx, msg, msgForPrefetch)
	return comparePromises(c.fatalErrChan, "DigestMessage", internal, external)
}

func (c *ComparisonClient) Reorg(msgIdxOfFirstMsgToAdd arbutil.MessageIndex, newMessages []arbostypes.MessageWithMetadataAndBlockInfo, oldMessages []*arbostypes.MessageWithMetadata) containers.PromiseInterface[[]*execution.MessageResult] {
	log.Info("ComparisonClient: Reorg", "count", msgIdxOfFirstMsgToAdd, "newMessages", len(newMessages), "oldMessages", len(oldMessages))
	internal := c.internal.Reorg(msgIdxOfFirstMsgToAdd, newMessages, oldMessages)
	external := c.external.Reorg(msgIdxOfFirstMsgToAdd, newMessages, oldMessages)
	return comparePromises(c.fatalErrChan, "Reorg", internal, external)
}

func (c *ComparisonClient) HeadMessageIndex() containers.PromiseInterface[arbutil.MessageIndex] {
	internal := c.internal.HeadMessageIndex()
	external := c.external.HeadMessageIndex()
	return comparePromises(nil, "HeadMessageIndex", internal, external)
}

func (c *ComparisonClient) ResultAtMessageIndex(msgIdx arbutil.MessageIndex) containers.PromiseInterface[*execution.MessageResult] {
	internal := c.internal.ResultAtMessageIndex(msgIdx)
	external := c.external.ResultAtMessageIndex(msgIdx)
	return comparePromises(nil, "ResultAtMessageIndex", internal, external)
}

func (c *ComparisonClient) SetFinalityData(safeFinalityData *arbutil.FinalityData, finalizedFinalityData *arbutil.FinalityData, validatedFinalityData *arbutil.FinalityData) containers.PromiseInterface[struct{}] {
	internal := c.internal.SetFinalityData(safeFinalityData, finalizedFinalityData, validatedFinalityData)
	external := c.external.SetFinalityData(safeFinalityData, finalizedFinalityData, validatedFinalityData)
	return comparePromises(c.fatalErrChan, "SetFinalityData", internal, external)
}

func (c *ComparisonClient) MarkFeedStart(to arbutil.MessageIndex) containers.PromiseInterface[struct{}] {
	internal := c.internal.MarkFeedStart(to)
	external := c.external.MarkFeedStart(to)
	return comparePromises(c.fatalErrChan, "MarkFeedStart", internal, external)
}

func (c *ComparisonClient) ShouldTriggerMaintenance() containers.PromiseInterface[bool] {
	internal := c.internal.ShouldTriggerMaintenance()
	external := c.external.ShouldTriggerMaintenance()
	return comparePromises(c.fatalErrChan, "ShouldTriggerMaintenance", internal, external)
}

func (c *ComparisonClient) MaintenanceStatus() containers.PromiseInterface[*execution.MaintenanceStatus] {
	internal := c.internal.MaintenanceStatus()
	external := c.external.MaintenanceStatus()
	return comparePromises(c.fatalErrChan, "MaintenanceStatus", internal, external)
}

func (c *ComparisonClient) ArbOSVersionForMessageIndex(msgIdx arbutil.MessageIndex) containers.PromiseInterface[uint64] {
	internal := c.internal.ArbOSVersionForMessageIndex(msgIdx)
	external := c.external.ArbOSVersionForMessageIndex(msgIdx)
	return comparePromises(c.fatalErrChan, "ArbOSVersionForMessageIndex", internal, external)
}

// --- Internal-only methods (delegate to geth only) ---

func (c *ComparisonClient) SetConsensusSyncData(syncData *execution.ConsensusSyncData) containers.PromiseInterface[struct{}] {
	return c.internal.SetConsensusSyncData(syncData)
}

func (c *ComparisonClient) TriggerMaintenance() containers.PromiseInterface[struct{}] {
	return c.internal.TriggerMaintenance()
}

// Start is a no-op. In comparison mode, internal geth is started by
// execution_consensus and external RPC is started by arbnode.Node.Start()
// via StartExternal().
func (c *ComparisonClient) Start(ctx context.Context) error {
	return nil
}

// StopAndWait is a no-op. In comparison mode, internal geth is stopped by
// execution_consensus and external RPC is stopped by StopExternal().
func (c *ComparisonClient) StopAndWait() {
}

// StartExternal starts only the external RPC client.
// Called by arbnode.Node.Start() in comparison mode.
func (c *ComparisonClient) StartExternal(ctx context.Context) error {
	return c.external.Start(ctx)
}

// StopExternal stops only the external RPC client.
func (c *ComparisonClient) StopExternal() {
	c.external.StopAndWait()
}

func (c *ComparisonClient) Pause() {
	c.internal.Pause()
}

func (c *ComparisonClient) Activate() {
	c.internal.Activate()
}

func (c *ComparisonClient) ForwardTo(url string) error {
	return c.internal.ForwardTo(url)
}

func (c *ComparisonClient) NextDelayedMessageNumber() (uint64, error) {
	return c.internal.NextDelayedMessageNumber()
}

func (c *ComparisonClient) IsTxHashInOnchainFilter(txHash common.Hash) (bool, error) {
	return c.internal.IsTxHashInOnchainFilter(txHash)
}

func (c *ComparisonClient) RecordBlockCreation(pos arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata, wasmTargets []rawdb.WasmTarget) containers.PromiseInterface[*execution.RecordResult] {
	return c.internal.RecordBlockCreation(pos, msg, wasmTargets)
}

func (c *ComparisonClient) PrepareForRecord(start, end arbutil.MessageIndex) containers.PromiseInterface[struct{}] {
	return c.internal.PrepareForRecord(start, end)
}

// --- ExecutionSequencer methods only available on internal ---

func (c *ComparisonClient) SequenceDelayedMessage(message *arbostypes.L1IncomingMessage, delayedSeqNum uint64) error {
	return c.internal.SequenceDelayedMessage(message, delayedSeqNum)
}

func (c *ComparisonClient) Synced(ctx context.Context) bool {
	return c.internal.Synced(ctx)
}

func (c *ComparisonClient) FullSyncProgressMap(ctx context.Context) map[string]interface{} {
	return c.internal.FullSyncProgressMap(ctx)
}

// --- Lifecycle helpers for execution_consensus ---

func (c *ComparisonClient) Internal() *gethexec.ExecutionNode {
	return c.internal
}
