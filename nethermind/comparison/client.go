// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md
package comparison

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
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
	return compareOps(c.fatalErrChan, "DigestMessage",
		func() containers.PromiseInterface[*execution.MessageResult] {
			return digestOrResult(c.internal, msgIdx, msg, msgForPrefetch)
		},
		func() containers.PromiseInterface[*execution.MessageResult] {
			return digestOrResult(c.external, msgIdx, msg, msgForPrefetch)
		},
	)
}

// digestOrResult calls DigestMessage if the engine needs this block, or
// ResultAtMessageIndex if the engine already has it (head >= msgIdx).
// This handles engines being at different positions after a crash/restart.
func digestOrResult(engine execution.ExecutionClient, msgIdx arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata, msgForPrefetch *arbostypes.MessageWithMetadata) containers.PromiseInterface[*execution.MessageResult] {
	head, err := engine.HeadMessageIndex().Current()
	if err == nil && head >= msgIdx {
		return engine.ResultAtMessageIndex(msgIdx)
	}
	return engine.DigestMessage(msgIdx, msg, msgForPrefetch)
}

func (c *ComparisonClient) Reorg(msgIdxOfFirstMsgToAdd arbutil.MessageIndex, newMessages []arbostypes.MessageWithMetadataAndBlockInfo, oldMessages []*arbostypes.MessageWithMetadata) containers.PromiseInterface[[]*execution.MessageResult] {
	return compareOps(c.fatalErrChan, "Reorg",
		func() containers.PromiseInterface[[]*execution.MessageResult] {
			return c.internal.Reorg(msgIdxOfFirstMsgToAdd, newMessages, oldMessages)
		},
		func() containers.PromiseInterface[[]*execution.MessageResult] {
			return c.external.Reorg(msgIdxOfFirstMsgToAdd, newMessages, oldMessages)
		},
	)
}

func (c *ComparisonClient) HeadMessageIndex() containers.PromiseInterface[arbutil.MessageIndex] {
	return compareOps(nil, "HeadMessageIndex",
		func() containers.PromiseInterface[arbutil.MessageIndex] { return c.internal.HeadMessageIndex() },
		func() containers.PromiseInterface[arbutil.MessageIndex] { return c.external.HeadMessageIndex() },
		func(a, b arbutil.MessageIndex) arbutil.MessageIndex { return min(a, b) },
	)
}

func (c *ComparisonClient) ResultAtMessageIndex(msgIdx arbutil.MessageIndex) containers.PromiseInterface[*execution.MessageResult] {
	return compareOps(nil, "ResultAtMessageIndex",
		func() containers.PromiseInterface[*execution.MessageResult] {
			return c.internal.ResultAtMessageIndex(msgIdx)
		},
		func() containers.PromiseInterface[*execution.MessageResult] {
			return c.external.ResultAtMessageIndex(msgIdx)
		},
	)
}

func (c *ComparisonClient) SetFinalityData(safeFinalityData *arbutil.FinalityData, finalizedFinalityData *arbutil.FinalityData, validatedFinalityData *arbutil.FinalityData) containers.PromiseInterface[struct{}] {
	return compareOps(c.fatalErrChan, "SetFinalityData",
		func() containers.PromiseInterface[struct{}] {
			return c.internal.SetFinalityData(safeFinalityData, finalizedFinalityData, validatedFinalityData)
		},
		func() containers.PromiseInterface[struct{}] {
			return c.external.SetFinalityData(safeFinalityData, finalizedFinalityData, validatedFinalityData)
		},
	)
}

func (c *ComparisonClient) MarkFeedStart(to arbutil.MessageIndex) containers.PromiseInterface[struct{}] {
	return compareOps(c.fatalErrChan, "MarkFeedStart",
		func() containers.PromiseInterface[struct{}] { return c.internal.MarkFeedStart(to) },
		func() containers.PromiseInterface[struct{}] { return c.external.MarkFeedStart(to) },
	)
}

func (c *ComparisonClient) ShouldTriggerMaintenance() containers.PromiseInterface[bool] {
	return compareOps(c.fatalErrChan, "ShouldTriggerMaintenance",
		func() containers.PromiseInterface[bool] { return c.internal.ShouldTriggerMaintenance() },
		func() containers.PromiseInterface[bool] { return c.external.ShouldTriggerMaintenance() },
	)
}

func (c *ComparisonClient) MaintenanceStatus() containers.PromiseInterface[*execution.MaintenanceStatus] {
	return compareOps(c.fatalErrChan, "MaintenanceStatus",
		func() containers.PromiseInterface[*execution.MaintenanceStatus] {
			return c.internal.MaintenanceStatus()
		},
		func() containers.PromiseInterface[*execution.MaintenanceStatus] {
			return c.external.MaintenanceStatus()
		},
	)
}

func (c *ComparisonClient) ArbOSVersionForMessageIndex(msgIdx arbutil.MessageIndex) containers.PromiseInterface[uint64] {
	return compareOps(c.fatalErrChan, "ArbOSVersionForMessageIndex",
		func() containers.PromiseInterface[uint64] { return c.internal.ArbOSVersionForMessageIndex(msgIdx) },
		func() containers.PromiseInterface[uint64] { return c.external.ArbOSVersionForMessageIndex(msgIdx) },
	)
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
