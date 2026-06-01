// Copyright 2025-2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package rpcclient

import (
	"context"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/node"

	"github.com/offchainlabs/nitro/arbos/arbostypes"
	"github.com/offchainlabs/nitro/arbutil"
	"github.com/offchainlabs/nitro/execution"
	"github.com/offchainlabs/nitro/execution/gethexec"
	"github.com/offchainlabs/nitro/util/containers"
	"github.com/offchainlabs/nitro/util/rpcclient"
	"github.com/offchainlabs/nitro/util/stopwaiter"
)

type Client struct {
	stopwaiter.StopWaiter
	client *rpcclient.RpcClient
}

func NewClient(config rpcclient.ClientConfigFetcher, stack *node.Node) *Client {
	return &Client{
		client: rpcclient.NewRpcClient(config, stack),
	}
}

func (c *Client) Start(ctx_in context.Context) error {
	c.StopWaiter.Start(ctx_in, c)
	ctx := c.GetContext()
	return c.client.Start(ctx)
}

func (c *Client) StopAndWait() {
	c.client.Close()
	c.StopWaiter.StopAndWait()
}

// CapturedHeaders returns the captured response headers if header capture is enabled.
// Returns nil if header capture is not enabled.
func (c *Client) CapturedHeaders() *rpcclient.CapturedHeaders {
	return c.client.CapturedHeaders()
}

func convertError(err error) error {
	if err == nil {
		return nil
	}
	errStr := err.Error()
	if strings.Contains(errStr, execution.ErrRetrySequencer.Error()) {
		return execution.ErrRetrySequencer
	} else if strings.Contains(errStr, gethexec.ResultNotFound.Error()) {
		return gethexec.ResultNotFound
	}
	return err
}

func sendRequest[T any](c *Client, method string, args ...any) containers.PromiseInterface[T] {
	return stopwaiter.LaunchPromiseThread(c, func(ctx context.Context) (T, error) {
		var res T
		err := c.client.CallContext(ctx, &res, execution.RPCNamespace+method, args...)
		return res, convertError(err)
	})
}

func (c *Client) DigestMessage(msgIdx arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata, msgForPrefetch *arbostypes.MessageWithMetadata) containers.PromiseInterface[*execution.MessageResult] {
	return sendRequest[*execution.MessageResult](c, "_digestMessage", msgIdx, msg, msgForPrefetch)
}

func (c *Client) Reorg(msgIdxOfFirstMsgToAdd arbutil.MessageIndex, newMessages []arbostypes.MessageWithMetadataAndBlockInfo, oldMessages []*arbostypes.MessageWithMetadata) containers.PromiseInterface[[]*execution.MessageResult] {
	return sendRequest[[]*execution.MessageResult](c, "_reorg", msgIdxOfFirstMsgToAdd, newMessages, oldMessages)
}

func (c *Client) HeadMessageIndex() containers.PromiseInterface[arbutil.MessageIndex] {
	return sendRequest[arbutil.MessageIndex](c, "_headMessageIndex")
}

func (c *Client) ResultAtMessageIndex(msgIdx arbutil.MessageIndex) containers.PromiseInterface[*execution.MessageResult] {
	return sendRequest[*execution.MessageResult](c, "_resultAtMessageIndex", msgIdx)
}

func (c *Client) SetFinalityData(safeFinalityData *arbutil.FinalityData, finalizedFinalityData *arbutil.FinalityData, validatedFinalityData *arbutil.FinalityData) containers.PromiseInterface[struct{}] {
	return sendRequest[struct{}](c, "_setFinalityData", safeFinalityData, finalizedFinalityData, validatedFinalityData)
}

func (c *Client) SetConsensusSyncData(syncData *execution.ConsensusSyncData) containers.PromiseInterface[struct{}] {
	return sendRequest[struct{}](c, "_setConsensusSyncData", syncData)
}

func (c *Client) MarkFeedStart(to arbutil.MessageIndex) containers.PromiseInterface[struct{}] {
	return sendRequest[struct{}](c, "_markFeedStart", to)
}

func (c *Client) TriggerMaintenance() containers.PromiseInterface[struct{}] {
	return sendRequest[struct{}](c, "_triggerMaintenance")
}

func (c *Client) ShouldTriggerMaintenance() containers.PromiseInterface[bool] {
	return sendRequest[bool](c, "_shouldTriggerMaintenance")
}

func (c *Client) MaintenanceStatus() containers.PromiseInterface[*execution.MaintenanceStatus] {
	return sendRequest[*execution.MaintenanceStatus](c, "_maintenanceStatus")
}

func (c *Client) ArbOSVersionForMessageIndex(msgIdx arbutil.MessageIndex) containers.PromiseInterface[uint64] {
	return sendRequest[uint64](c, "_arbOSVersionForMessageIndex", msgIdx)
}

func (c *Client) RecordBlockCreation(pos arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata, wasmTargets []rawdb.WasmTarget) containers.PromiseInterface[*execution.RecordResult] {
	return sendRequest[*execution.RecordResult](c, "_recordBlockCreation", pos, msg, wasmTargets)
}

func (c *Client) PrepareForRecord(start, end arbutil.MessageIndex) containers.PromiseInterface[struct{}] {
	return sendRequest[struct{}](c, "_prepareForRecord", start, end)
}

// sendEthRequest sends a standard Ethereum JSON-RPC request (without namespace prefix)
func sendEthRequest[T any](c *Client, method string, args ...any) containers.PromiseInterface[T] {
	return stopwaiter.LaunchPromiseThread(c, func(ctx context.Context) (T, error) {
		var res T
		err := c.client.CallContext(ctx, &res, method, args...)
		return res, convertError(err)
	})
}

// toBlockNumArg converts a block number to JSON-RPC format
func toBlockNumArg(number *big.Int) string {
	if number == nil {
		return "latest"
	}
	if number.Sign() < 0 {
		// Use special block tags for negative numbers
		return "latest"
	}
	return hexutil.EncodeBig(number)
}

// GetHeaderByNumber retrieves a block header by number using eth_getBlockByNumber
func (c *Client) GetHeaderByNumber(blockNum *big.Int) containers.PromiseInterface[*types.Header] {
	return stopwaiter.LaunchPromiseThread(c, func(ctx context.Context) (*types.Header, error) {
		var head *types.Header
		err := c.client.CallContext(ctx, &head, "eth_getBlockByNumber", toBlockNumArg(blockNum), false)
		return head, convertError(err)
	})
}

// GetBlockReceipts retrieves all receipts for a block by number using eth_getBlockReceipts
func (c *Client) GetBlockReceipts(blockNum *big.Int) containers.PromiseInterface[[]*types.Receipt] {
	return stopwaiter.LaunchPromiseThread(c, func(ctx context.Context) ([]*types.Receipt, error) {
		var receipts []*types.Receipt
		err := c.client.CallContext(ctx, &receipts, "eth_getBlockReceipts", toBlockNumArg(blockNum))
		return receipts, convertError(err)
	})
}

// GetBlockReceiptsByHash retrieves all receipts for a block by hash using eth_getBlockReceipts
// This is preferred over GetBlockReceipts when the block was just produced, as blocks are
// indexed by hash immediately but the number-to-hash mapping may lag.
func (c *Client) GetBlockReceiptsByHash(hash common.Hash) containers.PromiseInterface[[]*types.Receipt] {
	return stopwaiter.LaunchPromiseThread(c, func(ctx context.Context) ([]*types.Receipt, error) {
		var receipts []*types.Receipt
		err := c.client.CallContext(ctx, &receipts, "eth_getBlockReceipts", hash)
		return receipts, convertError(err)
	})
}

// GetBlockByHash retrieves a block by its hash using eth_getBlockByHash (header only)
func (c *Client) GetHeaderByHash(hash common.Hash) containers.PromiseInterface[*types.Header] {
	return stopwaiter.LaunchPromiseThread(c, func(ctx context.Context) (*types.Header, error) {
		var head *types.Header
		err := c.client.CallContext(ctx, &head, "eth_getBlockByHash", hash, false)
		return head, convertError(err)
	})
}
