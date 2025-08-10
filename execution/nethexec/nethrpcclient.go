package nethexec

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/offchainlabs/nitro/arbos/arbostypes"
	"github.com/offchainlabs/nitro/arbutil"
	"github.com/offchainlabs/nitro/execution"
)

var defaultUrl = "http://localhost:20545"

type RemoteExecutionRpcClient interface {
	DigestInitMessage(ctx context.Context, initialL1BaseFee *big.Int, serializedChainConfig []byte) *execution.MessageResult
}

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

type SequenceDelayedMessageParams struct {
	DelayedSeqNum uint64                        `json:"delayedSeqNum"`
	Message       *arbostypes.L1IncomingMessage `json:"message"`
}

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
		"messageType", msg.Message.Header.Kind)

	if payload, marshalErr := json.Marshal(params); marshalErr == nil {
		fmt.Println("DigestMessage request:", string(payload))
	} else {
		log.Warn("Failed to marshal DigestMessage params to JSON", "error", marshalErr)
	}

	var result execution.MessageResult
	err := c.client.CallContext(ctx, &result, "DigestMessage", params)
	if err != nil {
		panic(fmt.Sprintf("failed to call DigestMessage: %v", err))
	}

	return &result
}

// SequenceDelayedMessage forwards a delayed inbox message to Nethermind for sequencing.
// Note: servers may alternatively accept this via DigestMessage with the delayed index.
func (c *NethRpcClient) SequenceDelayedMessage(ctx context.Context, delayedSeqNum uint64, message *arbostypes.L1IncomingMessage) error {
	params := SequenceDelayedMessageParams{
		DelayedSeqNum: delayedSeqNum,
		Message:       message,
	}

	log.Info("Making JSON-RPC call to SequenceDelayedMessage",
		"url", c.url,
		"delayedSeqNum", delayedSeqNum,
		"messageType", message.Header.Kind)

	// Print JSON payload to the terminal for easier debugging

	var result interface{}
	if err := c.client.CallContext(ctx, &result, "SequenceDelayedMessage", params); err != nil {
		return fmt.Errorf("failed to call SequenceDelayedMessage: %w", err)
	}
	return nil
}

func (c *NethRpcClient) DigestInitMessage(ctx context.Context, initialL1BaseFee *big.Int, serializedChainConfig []byte) *execution.MessageResult {
	useExternalExecution, err := strconv.ParseBool(os.Getenv("PR_USE_EXTERNAL_EXECUTION"))
	if err != nil {
		log.Warn("Wasn't able to read PR_USE_EXTERNAL_EXECUTION, setting to false")
		useExternalExecution = false
	}

	var result execution.MessageResult

	if !useExternalExecution {
		return &result
	}

	params := InitializeMessageParams{
		InitialL1BaseFee:      initialL1BaseFee,
		SerializedChainConfig: serializedChainConfig,
	}

	log.Info("Making JSON-RPC call to DigestInitMessage",
		"url", c.url,
		"initialL1BaseFee", initialL1BaseFee,
		"len(serializedChainConfig)", len(serializedChainConfig))

	if payload, marshalErr := json.Marshal(params); marshalErr == nil {
		fmt.Println("DigestInitMessage request:", string(payload))
	} else {
		log.Warn("Failed to marshal DigestInitMessage params to JSON", "error", marshalErr)
	}

	err = c.client.CallContext(ctx, &result, "DigestInitMessage", params)
	if err != nil {
		panic(fmt.Sprintf("failed to call DigestInitMessage: %v", err))
	}

	return &result
}

type FakeRemoteExecutionRpcClient struct{}

func (c FakeRemoteExecutionRpcClient) DigestInitMessage(ctx context.Context, initialL1BaseFee *big.Int, serializedChainConfig []byte) *execution.MessageResult {
	return nil
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
