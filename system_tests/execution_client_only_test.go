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

func testExecutionClientOnly(t *testing.T, useExternalExecutionClient bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	builder := NewNodeBuilder(ctx).DefaultConfig(t, true)
	cleanup := builder.Build(t)
	defer cleanup()
	seqTestClient := builder.L2

	replicaConfig := arbnode.ConfigDefaultL1NonSequencerTest()
	replicaParams := &SecondNodeParams{
		nodeConfig:                 replicaConfig,
		useExecutionClientOnly:     true,
		useExternalExecutionClient: useExternalExecutionClient,
	}

	var replicaClient *ethclient.Client
	if useExternalExecutionClient {
		// For external execution client, we don't get a TestClient because Nitro doesn't store receipts
		// We need to connect directly to the external execution client's RPC
		_, replicaCleanup := builder.Build2ndNode(t, replicaParams)
		defer replicaCleanup()

		// Connect directly to Nethermind
		nethRpcUrl := os.Getenv("PR_NETH_RPC_CLIENT_URL")
		if nethRpcUrl == "" {
			nethRpcUrl = "http://localhost:20545"
		}
		rpcClient, err := rpc.Dial(nethRpcUrl)
		Require(t, err)
		replicaClient = ethclient.NewClient(rpcClient)
	} else {
		// For internal execution client, use the standard test client
		replicaTestClient, replicaCleanup := builder.Build2ndNode(t, replicaParams)
		defer replicaCleanup()
		replicaClient = replicaTestClient.Client
	}

	builder.L2Info.GenerateAccount("User2")
	for i := 0; i < 3; i++ {
		tx := builder.L2Info.PrepareTx("Owner", "User2", builder.L2Info.TransferGas, big.NewInt(1e12), nil)
		err := seqTestClient.Client.SendTransaction(ctx, tx)
		Require(t, err)
		_, err = seqTestClient.EnsureTxSucceeded(tx)
		Require(t, err)

		if useExternalExecutionClient {
			// Give time for L1 batch posting and sync
			time.Sleep(time.Second * 2)
		} else {
			_, err = WaitForTx(ctx, replicaClient, tx.Hash(), time.Second*15)
			Require(t, err)
		}
	}

	// Wait for replica to sync (poll balance for external client)
	expectedBalance := big.NewInt(3e12)
	timeout := time.After(time.Second * 30)
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	for {
		replicaBalance, err := replicaClient.BalanceAt(ctx, builder.L2Info.GetAddress("User2"), nil)
		Require(t, err)
		if replicaBalance.Cmp(expectedBalance) == 0 {
			return // Test passed!
		}

		select {
		case <-timeout:
			t.Fatalf("Timeout waiting for replica to sync. Balance: %s, expected: %s", replicaBalance, expectedBalance)
		case <-ticker.C:
			// Continue polling
		}
	}
}

func TestExecutionClientOnlyInternal(t *testing.T) {
	testExecutionClientOnly(t, false)
}

func TestExecutionClientOnlyExternal(t *testing.T) {
	testExecutionClientOnly(t, true)
}
