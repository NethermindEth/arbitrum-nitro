package nethexec

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/offchainlabs/nitro/arbnode"
	"github.com/offchainlabs/nitro/arbutil"
	"github.com/offchainlabs/nitro/execution"
	"github.com/offchainlabs/nitro/execution/gethexec"
)

// Constants for synchronization
const (
	bootstrapErrorMsg       = "Failed to get latest header"
	syncProgressLogInterval = 100
)

// clientInfo holds information about an execution client for synchronization
type clientInfo struct {
	client execution.ExecutionClient
	head   arbutil.MessageIndex
	name   string
}

// syncService manages synchronization between execution clients
type syncService struct {
	logger     *slog.Logger
	consensus  execution.FullConsensusClient
	mu         sync.RWMutex
	lastSync   atomic.Int64 // Unix timestamp of last successful sync
	comparator *comparator
}

// newSyncService creates a new synchronization service
func newSyncService(logger *slog.Logger, comparator *comparator) *syncService {
	return &syncService{
		logger:     logger.With("component", "sync-service"),
		comparator: comparator,
	}
}

// setConsensus sets the consensus client for synchronization operations
func (s *syncService) setConsensus(consensus execution.FullConsensusClient) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consensus = consensus
}

// getConsensus retrieves the consensus client in a thread-safe manner
func (s *syncService) getConsensus() execution.FullConsensusClient {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.consensus
}

// isBootstrapCase determines if this is a bootstrap scenario where external client needs initialization
func (s *syncService) isBootstrapCase(intErr, extErr error) bool {
	return intErr == nil && extErr != nil && strings.Contains(extErr.Error(), bootstrapErrorMsg)
}

// handleBootstrapInitialization initializes the external Nethermind client using DigestInitMessage
func (s *syncService) handleBootstrapInitialization(ctx context.Context, nethClient *nethermindExecutionClient, intRes arbutil.MessageIndex) error {
	s.logger.Info("Bootstrap initialization starting",
		"internal_head", intRes)

	consensus := s.getConsensus()
	if consensus == nil {
		return &BootstrapError{
			Client: "nethermind",
			Cause:  errors.New("consensus client not available"),
		}
	}

	arbNode, ok := consensus.(*arbnode.Node)
	if !ok {
		return &BootstrapError{
			Client: "nethermind",
			Cause:  errors.New("consensus client is not an arbnode.Node"),
		}
	}

	initMessage, err := arbNode.TxStreamer.GetMessage(0)
	if err != nil {
		return &BootstrapError{
			Client: "nethermind",
			Cause:  fmt.Errorf("failed to get init message: %w", err),
		}
	}

	parsedInitMessage, err := initMessage.Message.ParseInitMessage()
	if err != nil {
		return &BootstrapError{
			Client: "nethermind",
			Cause:  fmt.Errorf("failed to parse init message: %w", err),
		}
	}

	s.logger.Info("Bootstrap initialization proceeding",
		"chain_id", parsedInitMessage.ChainId,
		"initial_l1_base_fee", parsedInitMessage.InitialL1BaseFee,
		"config_size", len(parsedInitMessage.SerializedChainConfig))

	result, err := nethClient.DigestInitMessage(ctx, parsedInitMessage.InitialL1BaseFee, parsedInitMessage.SerializedChainConfig)
	if err != nil {
		return &BootstrapError{
			Client: "nethermind",
			Cause:  fmt.Errorf("DigestInitMessage failed: %w", err),
		}
	}

	s.logger.Info("Bootstrap initialization completed",
		"result", result)
	s.lastSync.Store(time.Now().Unix())

	return nil
}

// synchronizeExecutionClients attempts to bring both execution clients to the same head message index
func (s *syncService) synchronizeExecutionClients(ctx context.Context, gethClient *gethexec.ExecutionNode, nethClient *nethermindExecutionClient, internalHead, externalHead arbutil.MessageIndex) error {
	if internalHead == externalHead {
		return nil
	}

	consensus := s.getConsensus()
	if consensus == nil {
		return &SyncError{
			LaggingClient: "unknown",
			MessageIndex:  0,
			Cause:         errors.New("consensus client not available"),
		}
	}

	arbNode, ok := consensus.(*arbnode.Node)
	if !ok {
		return &SyncError{
			LaggingClient: "unknown",
			MessageIndex:  0,
			Cause:         errors.New("consensus client is not an arbnode.Node"),
		}
	}

	var lagging, leading clientInfo
	if internalHead > externalHead {
		lagging = clientInfo{nethClient, externalHead, "external (Nethermind)"}
		leading = clientInfo{gethClient, internalHead, "internal (Geth)"}
	} else {
		lagging = clientInfo{gethClient, internalHead, "internal (Geth)"}
		leading = clientInfo{nethClient, externalHead, "external (Nethermind)"}
	}

	s.logger.Info("Synchronization starting",
		"lagging_client", lagging.name,
		"lagging_head", lagging.head,
		"leading_head", leading.head,
		"message_gap", leading.head-lagging.head)

	consensusHeadIdx, err := arbNode.TxStreamer.GetHeadMessageIndex()
	if err != nil {
		return &SyncError{
			LaggingClient: lagging.name,
			MessageIndex:  lagging.head,
			Cause:         fmt.Errorf("failed to get consensus head: %w", err),
		}
	}

	if consensusHeadIdx < leading.head {
		return &SyncError{
			LaggingClient: lagging.name,
			MessageIndex:  leading.head,
			Cause:         fmt.Errorf("consensus only has messages up to %d, need %d", consensusHeadIdx, leading.head),
		}
	}

	return s.replayMessages(ctx, arbNode, lagging, leading)
}

// replayMessages replays messages from lagging client to leading client head
func (s *syncService) replayMessages(ctx context.Context, arbNode *arbnode.Node, lagging, leading clientInfo) error {
	messagesToReplay := leading.head - lagging.head
	s.logger.Info("Message replay starting",
		"client", lagging.name,
		"from_index", lagging.head+1,
		"to_index", leading.head,
		"message_count", messagesToReplay)

	syncStart := time.Now()
	var successfulReplays arbutil.MessageIndex

	op := fmt.Sprintf("Synchronization: leading client %s and lagging client %s", leading.name, lagging.name)

	for msgIdx := lagging.head + 1; msgIdx <= leading.head; msgIdx++ {
		select {
		case <-ctx.Done():
			return &SyncError{
				LaggingClient: lagging.name,
				MessageIndex:  msgIdx,
				Cause:         ctx.Err(),
			}
		default:
		}

		msg, err := arbNode.TxStreamer.GetMessage(msgIdx)
		if err != nil {
			return &SyncError{
				LaggingClient: lagging.name,
				MessageIndex:  msgIdx,
				Cause:         fmt.Errorf("failed to get message: %w", err),
			}
		}

		s.logger.Debug("Synchronization: Processing message", "client", lagging.name, "messageIndex", msgIdx)

		laggingResult := lagging.client.DigestMessage(msgIdx, msg, nil)
		leadingResult := leading.client.ResultAtMessageIndex(msgIdx)

		result := s.comparator.compareMessageResultPromise(
			ctx,
			op,
			leadingResult,
			laggingResult,
		)

		if _, err = result.Await(ctx); err != nil {
			s.logger.Error("Synchronization: Failed to validate message result", "messageIndex", msgIdx, "err", err)
			return fmt.Errorf("failed to validate message result: %w", err)
		}

		successfulReplays++

		if successfulReplays%syncProgressLogInterval == 0 || msgIdx == leading.head {
			s.logger.Info("Synchronization progress",
				"client", lagging.name,
				"replayed", successfulReplays,
				"total", messagesToReplay,
				"current_index", msgIdx,
				"elapsed", time.Since(syncStart))
		}
	}

	s.logger.Info("Message replay completed",
		"client", lagging.name,
		"replayed_messages", successfulReplays,
		"total_elapsed", time.Since(syncStart))

	s.lastSync.Store(time.Now().Unix())
	return nil
}

// isFatalSyncError determines if a synchronization error should cause fatal shutdown
func (s *syncService) isFatalSyncError(err error) bool {
	var syncErr *SyncError
	return errors.As(err, &syncErr) && strings.Contains(syncErr.Error(), "message validation failed")
}
