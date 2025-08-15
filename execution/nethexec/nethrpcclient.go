package nethexec

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/offchainlabs/nitro/arbos/arbostypes"
	"github.com/offchainlabs/nitro/arbutil"
	"github.com/offchainlabs/nitro/execution"
)

var defaultUrl = "http://localhost:20545"

type NethRpcClient struct {
	client *rpc.Client
	url    string
}

type MessageParams struct {
	Number             arbutil.MessageIndex            `json:"number"`
	Message            *arbostypes.MessageWithMetadata `json:"message"`
	MessageForPrefetch *arbostypes.MessageWithMetadata `json:"messageForPrefetch,omitempty"`
}

type InitializeMessageParams struct {
	InitialL1BaseFee      *big.Int `json:"initialL1BaseFee"`
	SerializedChainConfig []byte   `json:"serializedChainConfig"`
}

type SetFinalityDataParams struct {
	SafeFinalityData      *RpcFinalityData `json:"safeFinalityData,omitempty"`
	FinalizedFinalityData *RpcFinalityData `json:"finalizedFinalityData,omitempty"`
	ValidatedFinalityData *RpcFinalityData `json:"validatedFinalityData,omitempty"`
}

type RpcFinalityData struct {
	MsgIdx    uint64      `json:"msgIdx"`
	BlockHash common.Hash `json:"blockHash"`
}

type InitMessageDigester interface {
	DigestInitMessage(ctx context.Context, initialL1BaseFee *big.Int, serializedChainConfig []byte) *execution.MessageResult
}

type FakeRemoteExecutionRpcClient struct{}

func (n *FakeRemoteExecutionRpcClient) DigestInitMessage(ctx context.Context, initialL1BaseFee *big.Int, serializedChainConfig []byte) *execution.MessageResult {
	return &execution.MessageResult{}
}

var _ InitMessageDigester = (*FakeRemoteExecutionRpcClient)(nil)

var _ InitMessageDigester = (*NethRpcClient)(nil)

func NewNethRpcClient() (*NethRpcClient, error) {
	url, exists := os.LookupEnv("PR_NETH_RPC_CLIENT_URL")
	if !exists {
		log.Warn("Wasn't able to read PR_NETH_RPC_CLIENT_URL, using default url", "url", defaultUrl)
		url = defaultUrl
	}

	httpClient := rpc.WithHTTPClient(&http.Client{
		Timeout: 30 * time.Second,
	})

	ctx := context.Background()
	rpcClient, err := rpc.DialOptions(ctx, url, httpClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create Neth RPC client: %w", err)
	}

	return &NethRpcClient{
		client: rpcClient,
		url:    url,
	}, nil
}

func (c *NethRpcClient) Close() {
	c.client.Close()
}

func (c *NethRpcClient) DigestMessage(ctx context.Context, num arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata, msgForPrefetch *arbostypes.MessageWithMetadata) *execution.MessageResult {
	params := MessageParams{
		Number:             num,
		Message:            msg,
		MessageForPrefetch: msgForPrefetch,
	}

	log.Info("Making JSON-RPC call to DigestMessage",
		"url", c.url,
		"num", num,
		"messageType", msg.Message.Header.Kind,
	)

	var result execution.MessageResult
	err := c.client.CallContext(ctx, &result, "DigestMessage", params)
	if err != nil {
		log.Error("Failed to call DigestMessage", "error", err)
		return nil
	}

	return &result
}

func (c *NethRpcClient) DigestInitMessage(ctx context.Context, initialL1BaseFee *big.Int, serializedChainConfig []byte) *execution.MessageResult {
	var result execution.MessageResult

	params := InitializeMessageParams{
		InitialL1BaseFee:      initialL1BaseFee,
		SerializedChainConfig: serializedChainConfig,
	}

	log.Info("Making JSON-RPC call to DigestInitMessage",
		"url", c.url,
		"initialL1BaseFee", initialL1BaseFee,
		"len(serializedChainConfig)", len(serializedChainConfig))

	if err := c.client.CallContext(ctx, &result, "DigestInitMessage", params); err != nil {
		panic(fmt.Sprintf("failed to call DigestInitMessage: %v", err))
	}

	return &result
}

func (c *NethRpcClient) SetFinalityData(ctx context.Context, safeFinalityData *arbutil.FinalityData, finalizedFinalityData *arbutil.FinalityData, validatedFinalityData *arbutil.FinalityData) error {
	params := SetFinalityDataParams{
		SafeFinalityData:      convertToRpcFinalityData(safeFinalityData),
		FinalizedFinalityData: convertToRpcFinalityData(finalizedFinalityData),
		ValidatedFinalityData: convertToRpcFinalityData(validatedFinalityData),
	}

	log.Info("Making JSON-RPC call to SetFinalityData",
		"url", c.url,
		"safeFinalityData", safeFinalityData,
		"finalizedFinalityData", finalizedFinalityData,
		"validatedFinalityData", validatedFinalityData)

	var result interface{}
	err := c.client.CallContext(ctx, &result, "SetFinalityData", params)
	if err != nil {
		log.Error("Failed to call SetFinalityData", "error", err)
		return err
	}

	return nil
}

func convertToRpcFinalityData(data *arbutil.FinalityData) *RpcFinalityData {
	if data == nil {
		return nil
	}
	return &RpcFinalityData{
		MsgIdx:    uint64(data.MsgIdx),
		BlockHash: data.BlockHash,
	}
}
