package nethexec

import (
	"context"
	"fmt"
	"math/big"
	"reflect"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/node"

	"github.com/offchainlabs/nitro/execution"
	"github.com/offchainlabs/nitro/execution/gethexec"
	"github.com/offchainlabs/nitro/solgen/go/precompilesgen"
	"github.com/offchainlabs/nitro/util/headerreader"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// ExecutionTopology holds the execution components wired for a specific mode.
// The consensus node uses these to create the arbnode.Node.
type ExecutionTopology struct {
	Client    execution.ExecutionClient
	Sequencer execution.ExecutionSequencer // nil when sequencing is disabled
	Recorder  execution.ExecutionRecorder
	ArbOSVer  execution.ArbOSVersionGetter

	// ExecNode is non-nil only in ModeInternalOnly. Used by the entry point
	// for GraphQL and Timeboost initialization that require gethexec internals.
	ExecNode *gethexec.ExecutionNode
}

// TopologyDeps bundles the dependencies needed by topology builders.
// Populated from local variables in cmd/nitro/nitro.go.
type TopologyDeps struct {
	Ctx           context.Context
	Stack         *node.Node
	ChainDb       ethdb.Database
	L2BlockChain  *core.BlockChain
	L1Client      *ethclient.Client
	ParentChainID *big.Int
	SyncTillBlock uint64
	FatalErrChan  chan error

	// Config fetchers — topology builders use these to get live configuration.
	ExecConfigFetcher gethexec.ConfigFetcher
	LiveNodeConfigGet func() LiveNodeConfigView

	// Nethermind connection (required for external modes, nil for internal).
	NethermindUrl   string
	NethermindWsUrl string

	// Sequencer enabled flag (controls whether sequencing components are created).
	SequencerEnabled bool
}

// LiveNodeConfigView provides the configuration values topology builders need
// from the live node config. This avoids importing cmd/nitro types into nethexec.
type LiveNodeConfigView struct {
	ParentChainReaderConfig headerreader.Config
	SequencerConfig         gethexec.SequencerConfig
	ExposeMultiGas          bool
}

// BuildInternalTopology creates the default Geth-based execution topology.
func BuildInternalTopology(deps TopologyDeps) (*ExecutionTopology, error) {
	execNode, err := gethexec.CreateExecutionNode(
		deps.Ctx,
		deps.Stack,
		deps.ChainDb,
		deps.L2BlockChain,
		deps.L1Client,
		deps.ExecConfigFetcher,
		deps.ParentChainID,
		deps.SyncTillBlock,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create execution node: %w", err)
	}
	return &ExecutionTopology{
		Client:    execNode,
		Sequencer: execNode,
		Recorder:  execNode,
		ArbOSVer:  execNode,
		ExecNode:  execNode,
	}, nil
}

// BuildExternalExecutionTopology creates a topology where ExecutionClient is
// delegated to Nethermind via RPC. When sequencing is enabled, an internal
// Geth sequencer is wired via InternalSequencerWrapper.
func BuildExternalExecutionTopology(deps TopologyDeps) (*ExecutionTopology, error) {
	nethClient, err := NewNethermindExecutionClient(deps.NethermindUrl, deps.NethermindWsUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to create external execution client: %w", err)
	}

	topo := &ExecutionTopology{
		Client:   nethClient,
		Recorder: nethClient,
		ArbOSVer: nethClient,
	}

	if deps.SequencerEnabled {
		liveView := deps.LiveNodeConfigGet()

		execEngine, err := gethexec.NewExecutionEngine(
			deps.L2BlockChain,
			deps.SyncTillBlock,
			liveView.ExposeMultiGas,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create execution engine for external-execution mode: %w", err)
		}

		var parentChainReader *headerreader.HeaderReader
		if deps.L1Client != nil && !reflect.ValueOf(deps.L1Client).IsNil() {
			arbSys, _ := precompilesgen.NewArbSys(types.ArbSysAddress, deps.L1Client)
			parentChainReader, err = headerreader.New(
				deps.Ctx,
				deps.L1Client,
				func() *headerreader.Config {
					view := deps.LiveNodeConfigGet()
					return &view.ParentChainReaderConfig
				},
				arbSys,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to create parent chain reader for external-execution mode: %w", err)
			}
		}

		seqConfigFetcher := func() *gethexec.SequencerConfig {
			view := deps.LiveNodeConfigGet()
			return &view.SequencerConfig
		}
		sequencer, err := gethexec.NewSequencer(
			execEngine,
			parentChainReader,
			seqConfigFetcher,
			deps.ParentChainID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create sequencer for external-execution mode: %w", err)
		}

		topo.Sequencer = NewInternalSequencerWrapper(nethClient, execEngine, sequencer)
	}

	return topo, nil
}

// BuildExternalSequencerTopology creates a topology where both ExecutionClient
// and ExecutionSequencer are delegated to Nethermind via RPC.
func BuildExternalSequencerTopology(deps TopologyDeps) (*ExecutionTopology, error) {
	nethClient, err := NewNethermindExecutionClient(deps.NethermindUrl, deps.NethermindWsUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to create external execution client: %w", err)
	}
	seqClient := NewNethermindSequencerClient(nethClient)
	return &ExecutionTopology{
		Client:    nethClient,
		Sequencer: seqClient,
		Recorder:  nethClient,
		ArbOSVer:  nethClient,
	}, nil
}

// BuildComparisonExecutionTopology creates a topology that runs both Geth and
// Nethermind ExecutionClients in parallel and compares their results.
// Geth is the source of truth; divergences are reported via fatalErrChan.
// When sequencing is enabled, Geth sequences alone (no comparison for sequencing).
func BuildComparisonExecutionTopology(deps TopologyDeps) (*ExecutionTopology, error) {
	gethNode, err := gethexec.CreateExecutionNode(
		deps.Ctx,
		deps.Stack,
		deps.ChainDb,
		deps.L2BlockChain,
		deps.L1Client,
		deps.ExecConfigFetcher,
		deps.ParentChainID,
		deps.SyncTillBlock,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create internal execution node for comparison mode: %w", err)
	}

	nethClient, err := NewNethermindExecutionClient(deps.NethermindUrl, deps.NethermindWsUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to create external execution client for comparison mode: %w", err)
	}

	comparator := NewComparatorClient(gethNode, nethClient, deps.FatalErrChan)

	topo := &ExecutionTopology{
		Client:   comparator,
		Recorder: gethNode,
		ArbOSVer: gethNode,
	}

	if deps.SequencerEnabled {
		topo.Sequencer = gethNode
	}

	return topo, nil
}

// BuildComparisonSequencerTopology creates a topology that runs both Geth and
// Nethermind ExecutionSequencers in parallel and compares their results.
// Transactions are forwarded to both pools via ComparatorTxPublisher for
// deterministic comparison leveraging Arbitrum's FIFO ordering guarantee.
func BuildComparisonSequencerTopology(deps TopologyDeps) (*ExecutionTopology, error) {
	gethNode, err := gethexec.CreateExecutionNode(
		deps.Ctx,
		deps.Stack,
		deps.ChainDb,
		deps.L2BlockChain,
		deps.L1Client,
		deps.ExecConfigFetcher,
		deps.ParentChainID,
		deps.SyncTillBlock,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create internal execution node for sequencer comparison: %w", err)
	}

	nethClient, err := NewNethermindExecutionClient(deps.NethermindUrl, deps.NethermindWsUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to create external execution client for sequencer comparison: %w", err)
	}
	nethSeqClient := NewNethermindSequencerClient(nethClient)

	comparatorClient := NewComparatorClient(gethNode, nethClient, deps.FatalErrChan)
	comparatorSeq := NewComparatorSequencer(comparatorClient, gethNode, nethSeqClient, deps.FatalErrChan)

	// Must replace after CreateExecutionNode because ArbInterface is constructed internally.
	txPub, err := NewComparatorTxPublisher(gethNode.TxPublisher, deps.NethermindUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to create comparator tx publisher: %w", err)
	}
	gethNode.ArbInterface.SetTransactionPublisher(txPub)

	return &ExecutionTopology{
		Client:    comparatorSeq,
		Sequencer: comparatorSeq,
		Recorder:  gethNode,
		ArbOSVer:  gethNode,
		ExecNode:  gethNode,
	}, nil
}
