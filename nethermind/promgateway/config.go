// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package promgateway

import (
	"time"

	"github.com/spf13/pflag"
)

type Config struct {
	Enabled        bool          `koanf:"enabled"`
	URL            string        `koanf:"url"`
	JobName        string        `koanf:"job-name"`
	Prefix         string        `koanf:"prefix"`
	Instance       string        `koanf:"instance"`
	UpdateInterval time.Duration `koanf:"update-interval"`
}

var DefaultConfig = Config{
	Enabled:        false,
	URL:            "http://127.0.0.1:9091/metrics",
	JobName:        "arbitrum-nitro",
	Prefix:         "",
	Instance:       "",
	UpdateInterval: 5 * time.Second,
}

func ConfigAddOptions(prefix string, f *pflag.FlagSet) {
	f.Bool(prefix+".enabled", DefaultConfig.Enabled, "enable prometheus pushgateway")
	f.String(prefix+".url", DefaultConfig.URL, "prometheus pushgateway URL (should include /metrics path if needed)")
	f.String(prefix+".job-name", DefaultConfig.JobName, "prometheus pushgateway job name")
	f.String(prefix+".prefix", DefaultConfig.Prefix, "metric name prefix to avoid collisions")
	f.String(prefix+".instance", DefaultConfig.Instance, "instance label to distinguish multiple nodes")
	f.Duration(prefix+".update-interval", DefaultConfig.UpdateInterval, "prometheus pushgateway update interval")
}
