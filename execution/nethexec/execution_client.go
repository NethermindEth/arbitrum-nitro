package nethexec

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/offchainlabs/nitro/arbnode"
	"github.com/offchainlabs/nitro/arbos/arbostypes"
	"github.com/offchainlabs/nitro/arbutil"
	"github.com/offchainlabs/nitro/execution"
	"github.com/offchainlabs/nitro/util/containers"
)

var (
	_ FullExecutionClient         = (*NethermindExecutionClient)(nil)
	_ arbnode.ExecutionNodeBridge = (*NethermindExecutionClient)(nil)
)

type NethermindExecutionClient struct {
	rpcClient *NethRpcClient
}

func NewNethermindExecutionClient() (*NethermindExecutionClient, error) {
	rpcClient, err := NewNethRpcClient()
	if err != nil {
		return nil, err
	}
	return &NethermindExecutionClient{
		rpcClient: rpcClient,
	}, nil
}

func (p *NethermindExecutionClient) DigestMessage(num arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata, msgForPrefetch *arbostypes.MessageWithMetadata) containers.PromiseInterface[*execution.MessageResult] {
	promise := containers.NewPromise[*execution.MessageResult](nil)
	go func() {
		res := p.rpcClient.DigestMessage(context.Background(), num, msg, msgForPrefetch)
		if res == nil {
			promise.ProduceError(fmt.Errorf("external DigestMessage returned nil"))
			return
		}
		promise.Produce(res)
	}()
	return &promise
}

func (p *NethermindExecutionClient) SetFinalityData(ctx context.Context, safeFinalityData *arbutil.FinalityData, finalizedFinalityData *arbutil.FinalityData, validatedFinalityData *arbutil.FinalityData) containers.PromiseInterface[struct{}] {
	promise := containers.NewPromise[struct{}](nil)
	go func() {
		err := p.rpcClient.SetFinalityData(ctx, safeFinalityData, finalizedFinalityData, validatedFinalityData)
		if err != nil {
			promise.ProduceError(err)
			return
		}
		promise.Produce(struct{}{})
	}()
	return &promise
}
func (p *NethermindExecutionClient) Reorg(msgIdxOfFirstMsgToAdd arbutil.MessageIndex, newMessages []arbostypes.MessageWithMetadataAndBlockInfo, oldMessages []*arbostypes.MessageWithMetadata) containers.PromiseInterface[[]*execution.MessageResult] {
	//TODO implement me
	panic("implement me")
}

func (p *NethermindExecutionClient) HeadMessageIndex() containers.PromiseInterface[arbutil.MessageIndex] {
	//TODO implement me
	panic("implement me")
}

func (p *NethermindExecutionClient) ResultAtMessageIndex(msgIdx arbutil.MessageIndex) containers.PromiseInterface[*execution.MessageResult] {
	//TODO implement me
	panic("implement me")
}

func (p *NethermindExecutionClient) MessageIndexToBlockNumber(messageNum arbutil.MessageIndex) containers.PromiseInterface[uint64] {
	//TODO implement me
	panic("implement me")
}

func (p *NethermindExecutionClient) BlockNumberToMessageIndex(blockNum uint64) containers.PromiseInterface[arbutil.MessageIndex] {
	//TODO implement me
	panic("implement me")
}

func (p *NethermindExecutionClient) MarkFeedStart(to arbutil.MessageIndex) containers.PromiseInterface[struct{}] {
	//TODO implement me
	panic("implement me")
}

func (p *NethermindExecutionClient) Maintenance() containers.PromiseInterface[struct{}] {
	//TODO implement me
	panic("implement me")
}

func (p *NethermindExecutionClient) Start(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (p *NethermindExecutionClient) StopAndWait() {
	//TODO implement me
	panic("implement me")
}

func (p *NethermindExecutionClient) Pause() {
	//TODO implement me
	panic("implement me")
}

func (p *NethermindExecutionClient) Activate() {
	//TODO implement me
	panic("implement me")
}

func (p *NethermindExecutionClient) ForwardTo(url string) error {
	//TODO implement me
	panic("implement me")
}

func (p *NethermindExecutionClient) SequenceDelayedMessage(message *arbostypes.L1IncomingMessage, delayedSeqNum uint64) error {
	//TODO implement me
	panic("implement me")
}

func (p *NethermindExecutionClient) NextDelayedMessageNumber() (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (p *NethermindExecutionClient) Synced(ctx context.Context) bool {
	//TODO implement me
	panic("implement me")
}

func (p *NethermindExecutionClient) FullSyncProgressMap(ctx context.Context) map[string]interface{} {
	//TODO implement me
	panic("implement me")
}

func (p *NethermindExecutionClient) RecordBlockCreation(ctx context.Context, pos arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata) (*execution.RecordResult, error) {
	//TODO implement me
	panic("implement me")
}

func (p *NethermindExecutionClient) MarkValid(pos arbutil.MessageIndex, resultHash common.Hash) {
	//TODO implement me
	panic("implement me")
}

func (p *NethermindExecutionClient) PrepareForRecord(ctx context.Context, start, end arbutil.MessageIndex) error {
	//TODO implement me
	panic("implement me")
}

func (p *NethermindExecutionClient) ArbOSVersionForMessageIndex(msgIdx arbutil.MessageIndex) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (w *NethermindExecutionClient) SetConsensusClient(consensus execution.FullConsensusClient) {
	// TODO: implement me
	panic("implement me")
}

func (w *NethermindExecutionClient) Initialize(ctx context.Context) error {
	// TODO: implement me
	panic("implement me")
}
