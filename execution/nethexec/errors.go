package nethexec

import (
	"fmt"

	"github.com/offchainlabs/nitro/arbutil"
)

// ComparisonError represents an error that occurred during execution comparison
type ComparisonError struct {
	Operation string
	Internal  error
	External  error
	Diff      string
}

func (e *ComparisonError) Error() string {
	return fmt.Sprintf("comparison failed for %s: internal=%v, external=%v", e.Operation, e.Internal, e.External)
}

func (e *ComparisonError) Unwrap() []error {
	var errs []error
	if e.Internal != nil {
		errs = append(errs, e.Internal)
	}
	if e.External != nil {
		errs = append(errs, e.External)
	}
	return errs
}

// BootstrapError represents an error that occurred during client bootstrap initialization
type BootstrapError struct {
	Client string
	Cause  error
}

func (e *BootstrapError) Error() string {
	return fmt.Sprintf("bootstrap failed for %s client: %v", e.Client, e.Cause)
}

func (e *BootstrapError) Unwrap() error {
	return e.Cause
}

// SyncError represents an error that occurred during client synchronization
type SyncError struct {
	LaggingClient string
	MessageIndex  arbutil.MessageIndex
	Cause         error
}

func (e *SyncError) Error() string {
	return fmt.Sprintf("sync failed for %s client at message %d: %v", e.LaggingClient, e.MessageIndex, e.Cause)
}

func (e *SyncError) Unwrap() error {
	return e.Cause
}
