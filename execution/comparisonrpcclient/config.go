package comparisonrpcclient

import (
	"time"

	"github.com/spf13/pflag"

	"github.com/offchainlabs/nitro/util/rpcclient"
)

type ComparisonExecutionConfig struct {
	Enable             bool                   `json:"enable,omitempty" koanf:"enable"`
	SecondaryRPCClient rpcclient.ClientConfig `json:"secondary-rpc-client,omitempty" koanf:"secondary-rpc-client"`
}

var DefaultComparisonExecutionConfig = ComparisonExecutionConfig{
	Enable: false,
	SecondaryRPCClient: rpcclient.ClientConfig{
		URL:                       "",
		JWTSecret:                 "",
		Timeout:                   30 * time.Second,
		Retries:                   3,
		RetryErrors:               "websocket: close.*|dial tcp .*|.*i/o timeout|.*connection reset by peer|.*connection refused",
		ArgLogLimit:               2048,
		WebsocketMessageSizeLimit: 256 * 1024 * 1024,
	},
}

func ComparisonExecutionConfigAddOptions(prefix string, f *pflag.FlagSet) {
	f.Bool(prefix+".enable", DefaultComparisonExecutionConfig.Enable, "enable comparison mode to compare primary and secondary execution clients")
	rpcclient.RPCClientAddOptions(prefix+".secondary-rpc-client", f, &DefaultComparisonExecutionConfig.SecondaryRPCClient)
}
