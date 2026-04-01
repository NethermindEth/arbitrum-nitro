// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md
package comparison

import (
	"github.com/spf13/pflag"
)

type ComparisonExecutionConfig struct {
	Enable bool `koanf:"enable"`
}

var DefaultComparisonExecutionConfig = ComparisonExecutionConfig{
	Enable: false,
}

func ComparisonExecutionConfigAddOptions(prefix string, f *pflag.FlagSet) {
	f.Bool(prefix+".enable", DefaultComparisonExecutionConfig.Enable, "enable comparison mode: run both internal (geth) and external execution clients, compare results, and fail on mismatch")
}
