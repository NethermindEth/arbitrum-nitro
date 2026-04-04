package nethexec

import (
	"os"
)

// ExecutionMode controls how the wrapper uses internal vs external EL
type ExecutionMode uint8

const (
	ModeInternalOnly ExecutionMode = iota // default
	ModeDualCompare                       // call both, compare results
	ModeExternalOnly                      // return external only
)

// GetExecutionMode reads from the environment variable PR_EXECUTION_MODE
func GetExecutionMode() ExecutionMode {
	switch os.Getenv("PR_EXECUTION_MODE") {
	case "internal", "":
		return ModeInternalOnly
	case "compare", "dual", "both":
		return ModeDualCompare
	case "external", "nethermind":
		return ModeExternalOnly
	default:
		return ModeInternalOnly
	}
}
