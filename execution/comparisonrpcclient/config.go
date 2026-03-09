package comparisonrpcclient

import (
	"time"

	"github.com/spf13/pflag"

	"github.com/offchainlabs/nitro/util/rpcclient"
)

type ComparisonExecutionConfig struct {
	Enable               bool                   `json:"enable,omitempty" koanf:"enable"`
	UseInternalSequencer bool                   `json:"use-internal-sequencer,omitempty" koanf:"use-internal-sequencer"`
	SecondaryRPCClient   rpcclient.ClientConfig `json:"secondary-rpc-client,omitempty" koanf:"secondary-rpc-client"`
	ReceiptRetries       int                    `json:"receipt-retries,omitempty" koanf:"receipt-retries"`
	ReceiptRetryDelay    time.Duration          `json:"receipt-retry-delay,omitempty" koanf:"receipt-retry-delay"`
}

var DefaultComparisonExecutionConfig = ComparisonExecutionConfig{
	Enable:            false,
	ReceiptRetries:    50,
	ReceiptRetryDelay: 200 * time.Millisecond,
	SecondaryRPCClient: rpcclient.ClientConfig{
		URL:                       "",
		JWTSecret:                 "",
		Timeout:                   30 * time.Second,
		Retries:                   5,
		RetryErrors:               "websocket: close.*|dial tcp .*|.*i/o timeout|.*connection reset by peer|.*connection refused|CreateBlock mutex held",
		ArgLogLimit:               2048,
		WebsocketMessageSizeLimit: 256 * 1024 * 1024,
	},
}

func ComparisonExecutionConfigAddOptions(prefix string, f *pflag.FlagSet) {
	f.Bool(prefix+".enable", DefaultComparisonExecutionConfig.Enable, "enable comparison mode to compare primary and secondary execution clients")
	f.Bool(prefix+".use-internal-sequencer", DefaultComparisonExecutionConfig.UseInternalSequencer, "use internal execution client for sequencing while using RPC clients for comparison (enables sequencer mode in comparison)")
	f.Int(prefix+".receipt-retries", DefaultComparisonExecutionConfig.ReceiptRetries, "number of retries when fetching receipts for comparison (receipts may not be immediately available)")
	f.Duration(prefix+".receipt-retry-delay", DefaultComparisonExecutionConfig.ReceiptRetryDelay, "delay between receipt fetch retries")
	rpcclient.RPCClientAddOptions(prefix+".secondary-rpc-client", f, &DefaultComparisonExecutionConfig.SecondaryRPCClient)
}
