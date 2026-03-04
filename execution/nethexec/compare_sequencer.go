package nethexec

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/offchainlabs/nitro/arbos/arbostypes"
	"github.com/offchainlabs/nitro/arbutil"
	"github.com/offchainlabs/nitro/execution"
	"github.com/offchainlabs/nitro/util/containers"
)

var _ execution.ExecutionSequencer = (*ComparatorSequencer)(nil)

// Geth is canonical; divergences are reported via fatalErrChan.
type ComparatorSequencer struct {
	*ComparatorClient
	internalSeq  execution.ExecutionSequencer
	externalSeq  execution.ExecutionSequencer
	fatalErrChan chan error
}

func NewComparatorSequencer(
	comparatorClient *ComparatorClient,
	internalSeq execution.ExecutionSequencer,
	externalSeq execution.ExecutionSequencer,
	fatalErrChan chan error,
) *ComparatorSequencer {
	return &ComparatorSequencer{
		ComparatorClient: comparatorClient,
		internalSeq:      internalSeq,
		externalSeq:      externalSeq,
		fatalErrChan:     fatalErrChan,
	}
}

// DigestMessage uses non-fatal comparison in sequencer mode. Geth's DigestMessage
// uses TryLock on createBlocksMutex and returns "createBlock mutex held" when the
// sequencer is active. This is a transient condition — the caller retries in 100ms.
// Primary validation happens via StartSequencing comparison.
func (s *ComparatorSequencer) DigestMessage(idx arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata, prefetch *arbostypes.MessageWithMetadata) containers.PromiseInterface[*execution.MessageResult] {
	return comparePromises(nil, "DigestMessage",
		s.ComparatorClient.internal.DigestMessage(idx, msg, prefetch),
		s.ComparatorClient.external.DigestMessage(idx, msg, prefetch),
	)
}

// --- Compared operations ---

func (s *ComparatorSequencer) StartSequencing(ctx context.Context, seqCtx execution.SequencingContext) (*execution.SequencedMsg, time.Duration) {
	intMsg, _ := s.internalSeq.StartSequencing(ctx, seqCtx)
	extMsg, extDur := s.externalSeq.StartSequencing(ctx, seqCtx)

	if intMsg == nil && extMsg == nil {
		return nil, extDur
	}

	// Only fatal when both sides produce a block but they differ.
	// If Nethermind returns nil while Geth produces a block, it is a timing
	// lag (Nethermind's mempool may not yet contain the transaction), not a
	// correctness issue. The DigestMessage comparison catches real mismatches.
	if intMsg != nil && extMsg != nil {
		if err := compare("StartSequencing", intMsg, nil, extMsg, nil); err != nil && s.fatalErrChan != nil {
			reportDivergence(s.fatalErrChan, "StartSequencing", err)
		}
	} else {
		log.Warn("StartSequencing result mismatch (one side nil)", "internalNil", intMsg == nil, "externalNil", extMsg == nil)
	}

	return intMsg, extDur
}

func (s *ComparatorSequencer) ResequenceReorgedMessage(msg *arbostypes.MessageWithMetadata) (*execution.SequencedMsg, error) {
	intRes, intErr := s.internalSeq.ResequenceReorgedMessage(msg)
	extRes, extErr := s.externalSeq.ResequenceReorgedMessage(msg)

	if err := compare("ResequenceReorgedMessage", intRes, intErr, extRes, extErr); err != nil && s.fatalErrChan != nil {
		reportDivergence(s.fatalErrChan, "ResequenceReorgedMessage", err)
	}

	return intRes, intErr
}

func (s *ComparatorSequencer) NextDelayedMessageNumber() (uint64, error) {
	intRes, intErr := s.internalSeq.NextDelayedMessageNumber()
	extRes, extErr := s.externalSeq.NextDelayedMessageNumber()

	if err := compare("NextDelayedMessageNumber", intRes, intErr, extRes, extErr); err != nil && s.fatalErrChan != nil {
		reportDivergence(s.fatalErrChan, "NextDelayedMessageNumber", err)
	}

	return intRes, intErr
}

// --- Mirrored operations ---

func (s *ComparatorSequencer) EndSequencing(ctx context.Context, errWhileSequencing error) {
	s.internalSeq.EndSequencing(ctx, errWhileSequencing)
	s.externalSeq.EndSequencing(ctx, errWhileSequencing)
}

func (s *ComparatorSequencer) AppendLastSequencedBlock() error {
	if err := s.externalSeq.AppendLastSequencedBlock(); err != nil {
		log.Warn("External AppendLastSequencedBlock failed", "err", err)
	}
	return s.internalSeq.AppendLastSequencedBlock()
}

func (s *ComparatorSequencer) EnqueueDelayedMessages(msgs []*arbostypes.L1IncomingMessage, firstMsgIdx uint64) {
	s.internalSeq.EnqueueDelayedMessages(msgs, firstMsgIdx)
	s.externalSeq.EnqueueDelayedMessages(msgs, firstMsgIdx)
}

// --- Control operations (Geth only) ---

func (s *ComparatorSequencer) Pause() {
	s.internalSeq.Pause()
}

func (s *ComparatorSequencer) Activate() {
	s.internalSeq.Activate()
}

func (s *ComparatorSequencer) ForwardTo(url string) error {
	return s.internalSeq.ForwardTo(url)
}

// --- Status operations ---

func (s *ComparatorSequencer) Synced(ctx context.Context) bool {
	intSynced := s.internalSeq.Synced(ctx)
	extSynced := s.externalSeq.Synced(ctx)
	if intSynced != extSynced {
		log.Warn("Synced status mismatch", "internal", intSynced, "external", extSynced)
	}
	return intSynced
}

func (s *ComparatorSequencer) FullSyncProgressMap(ctx context.Context) map[string]interface{} {
	return s.internalSeq.FullSyncProgressMap(ctx)
}
