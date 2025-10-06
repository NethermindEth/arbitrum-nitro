// Copyright 2021-2024, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package arbtest

import (
	"context"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/offchainlabs/nitro/arbnode"
)

func TestExecutionClientOnlyInternal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	builder := NewNodeBuilder(ctx).DefaultConfig(t, true)
	cleanup := builder.Build(t)
	defer cleanup()
	seqTestClient := builder.L2

	replicaExecutionClientOnlyConfig := arbnode.ConfigDefaultL1NonSequencerTest()
	replicaExecutionClientOnlyTestClient, replicaExecutionClientOnlyCleanup := builder.Build2ndNode(t, &SecondNodeParams{nodeConfig: replicaExecutionClientOnlyConfig, useExecutionClientOnly: true})
	defer replicaExecutionClientOnlyCleanup()

	builder.L2Info.GenerateAccount("User2")
	for i := 0; i < 3; i++ {
		tx := builder.L2Info.PrepareTx("Owner", "User2", builder.L2Info.TransferGas, big.NewInt(1e12), nil)
		err := seqTestClient.Client.SendTransaction(ctx, tx)
		Require(t, err)
		_, err = seqTestClient.EnsureTxSucceeded(tx)
		Require(t, err)
		_, err = WaitForTx(ctx, replicaExecutionClientOnlyTestClient.Client, tx.Hash(), time.Second*15)
		Require(t, err)
	}

	replicaBalance, err := replicaExecutionClientOnlyTestClient.Client.BalanceAt(ctx, builder.L2Info.GetAddress("User2"), nil)
	Require(t, err)
	if replicaBalance.Cmp(big.NewInt(3e12)) != 0 {
		t.Fatal("Unexpected balance:", replicaBalance)
	}
}

func TestExecutionClientOnlyExternal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	builder := NewNodeBuilder(ctx).DefaultConfig(t, true)
	cleanup := builder.Build(t)
	defer cleanup()
	seqTestClient := builder.L2

	replicaExecutionClientOnlyConfig := arbnode.ConfigDefaultL1NonSequencerTest()
	_, replicaExecutionClientOnlyCleanup := builder.Build2ndNode(t, &SecondNodeParams{
		nodeConfig:                 replicaExecutionClientOnlyConfig,
		useExecutionClientOnly:     true,
		useExternalExecutionClient: true,
		useCompareExecutionClient:  false,
	})
	defer replicaExecutionClientOnlyCleanup()

	// Connect test client directly to Nethermind's RPC
	nethRpcUrl := os.Getenv("PR_NETH_RPC_CLIENT_URL")
	if nethRpcUrl == "" {
		nethRpcUrl = "http://localhost:20545"
	}
	nethermindRpcClient, err := rpc.Dial(nethRpcUrl)
	Require(t, err)
	replicaTestClient := &TestClient{
		ctx:    ctx,
		Client: ethclient.NewClient(nethermindRpcClient),
	}

	builder.L2Info.GenerateAccount("User2")
	for i := 0; i < 3; i++ {
		tx := builder.L2Info.PrepareTx("Owner", "User2", builder.L2Info.TransferGas, big.NewInt(1e12), nil)
		err := seqTestClient.Client.SendTransaction(ctx, tx)
		Require(t, err)
		_, err = seqTestClient.EnsureTxSucceeded(tx)
		Require(t, err)
		time.Sleep(time.Second * 2) // Allow time for L1 batch posting
	}

	// Wait for replica to sync by polling balance
	expectedBalance := big.NewInt(3e12)
	timeout := time.After(time.Second * 30)
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for replica to sync transactions")
		case <-ticker.C:
			replicaBalance, err := replicaTestClient.Client.BalanceAt(ctx, builder.L2Info.GetAddress("User2"), nil)
			Require(t, err)
			if replicaBalance.Cmp(expectedBalance) == 0 {
				return // Test passed
			}
		}
	}
}
