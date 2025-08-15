package nethexec

import (
	"os"
	"strings"
)

// ExecutionMode controls how the wrapper uses internal vs external EL
type ExecutionMode int

const (
	ModeInternalOnly ExecutionMode = iota // default
	ModeDualCompare                       // call both, compare results, return internal
	ModeExternalOnly                      // return external, still drive internal in bg for consistency
)

// GetExecutionModeFromEnv reads PR_EXECUTION_MODE or legacy PR_USE_EXTERNAL_EXECUTION
// Values for PR_EXECUTION_MODE: "internal" (default), "dual", "external"
func GetExecutionModeFromEnv() ExecutionMode {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("PR_EXECUTION_MODE")))
	switch mode {
	case "internal", "":
		return ModeInternalOnly
	case "dual", "compare", "both":
		return ModeDualCompare
	case "external", "nethermind":
		return ModeExternalOnly
	default:
		return ModeInternalOnly
	}
}

// No legacy helpers required; use GetExecutionModeFromEnv() directly.
