package comparisonrpcclient

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"reflect"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/google/go-cmp/cmp"

	"github.com/offchainlabs/nitro/arbutil"
	"github.com/offchainlabs/nitro/execution"
	executionrpcclient "github.com/offchainlabs/nitro/execution/rpcclient"
	"github.com/offchainlabs/nitro/util/containers"
)

// Comparator handles comparison of execution results between primary and secondary clients
type Comparator struct {
	fatalErrChan chan<- error
	primary      *executionrpcclient.Client
	secondary    *executionrpcclient.Client
}

// NewComparator creates a new Comparator
func NewComparator(fatalErrChan chan<- error, primary, secondary *executionrpcclient.Client) *Comparator {
	return &Comparator{
		fatalErrChan: fatalErrChan,
		primary:      primary,
		secondary:    secondary,
	}
}

// diffReporter is a custom cmp.Reporter that formats Ethereum types (Hash, Address) as hex strings
// and produces clean, human-readable diff output.
// See: https://pkg.go.dev/github.com/google/go-cmp/cmp#Reporter
type diffReporter struct {
	path  cmp.Path
	diffs []string
}

func (r *diffReporter) PushStep(ps cmp.PathStep) {
	r.path = append(r.path, ps)
}

func (r *diffReporter) PopStep() {
	r.path = r.path[:len(r.path)-1]
}

func (r *diffReporter) Report(rs cmp.Result) {
	if rs.Equal() {
		return
	}
	vx, vy := r.path.Last().Values()
	r.diffs = append(r.diffs, fmt.Sprintf("  %s:\n    - %s\n    + %s",
		r.pathString(), r.formatValue(vx), r.formatValue(vy)))
}

// pathString returns a clean path representation
func (r *diffReporter) pathString() string {
	var parts []string
	for _, step := range r.path {
		switch s := step.(type) {
		case cmp.StructField:
			parts = append(parts, s.Name())
		case cmp.SliceIndex:
			parts = append(parts, fmt.Sprintf("[%d]", s.Key()))
		case cmp.MapIndex:
			parts = append(parts, fmt.Sprintf("[%v]", s.Key()))
		}
	}
	if len(parts) == 0 {
		return "root"
	}
	return strings.Join(parts, ".")
}

// formatValue formats a reflect.Value, converting Ethereum types to hex strings
func (r *diffReporter) formatValue(v reflect.Value) string {
	if !v.IsValid() {
		return "<nil>"
	}
	if !v.CanInterface() {
		return fmt.Sprintf("%v", v)
	}

	iface := v.Interface()

	// Format common.Hash as hex
	if hash, ok := iface.(common.Hash); ok {
		return hash.Hex()
	}
	// Format common.Address as hex
	if addr, ok := iface.(common.Address); ok {
		return addr.Hex()
	}
	// Format errors
	if err, ok := iface.(error); ok {
		if err == nil {
			return "<nil>"
		}
		return fmt.Sprintf("%q", err.Error())
	}

	return fmt.Sprintf("%+v", iface)
}

func (r *diffReporter) String() string {
	if len(r.diffs) == 0 {
		return ""
	}
	return strings.Join(r.diffs, "\n")
}

// cmpOptions makes common.Hash and common.Address atomic comparison units
// so the Reporter sees them as whole values, not byte-by-byte
var cmpOptions = cmp.Options{
	// Treat common.Hash as atomic unit (compare by value, not byte-by-byte)
	cmp.Comparer(func(x, y common.Hash) bool {
		return x == y
	}),
	// Treat common.Address as atomic unit
	cmp.Comparer(func(x, y common.Address) bool {
		return x == y
	}),
}

// compare uses cmp.Equal with a custom Reporter for clean, readable diffs
func compare[T any](primary, secondary T) error {
	var reporter diffReporter
	if cmp.Equal(primary, secondary, cmpOptions, cmp.Reporter(&reporter)) {
		return nil
	}
	return errors.New(reporter.String())
}

// compareErrors compares two errors by their message strings only (ignoring pointer addresses)
func compareErrors(primary, secondary error) error {
	// Both nil - equal
	if primary == nil && secondary == nil {
		return nil
	}
	// One nil, one not - not equal
	if (primary == nil) != (secondary == nil) {
		return fmt.Errorf("error presence mismatch:\n  - %v\n  + %v", primary, secondary)
	}
	// Compare error message strings only (ignores pointer addresses and type differences)
	if primary.Error() != secondary.Error() {
		return fmt.Errorf("error message mismatch:\n  - %q\n  + %q", primary.Error(), secondary.Error())
	}
	return nil
}

// handleMismatch logs mismatch and sends error to fatalErrChan.
// The diff is printed directly to stderr for human readability since structured
// logging escapes newlines. A short log entry is also created for log aggregation.
func (c *Comparator) handleMismatch(method string, msgIdx *arbutil.MessageIndex, err, primaryErr, secondaryErr error) {
	// Print human-readable diff to stderr (structured logging escapes newlines)
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "=============== EXECUTION CLIENT COMPARISON MISMATCH ===============")
	if msgIdx != nil {
		fmt.Fprintf(os.Stderr, "Method: %s | Message Index: %d\n", method, *msgIdx)
	} else {
		fmt.Fprintf(os.Stderr, "Method: %s\n", method)
	}
	if primaryErr != nil || secondaryErr != nil {
		fmt.Fprintf(os.Stderr, "Primary Error:   %v\n", primaryErr)
		fmt.Fprintf(os.Stderr, "Secondary Error: %v\n", secondaryErr)
	}
	fmt.Fprintln(os.Stderr, "Diff (- primary, + secondary):")
	fmt.Fprintln(os.Stderr, err.Error())
	fmt.Fprintln(os.Stderr, "===================================================================")
	fmt.Fprintln(os.Stderr, "")

	// Also log a short structured entry for log aggregation
	if msgIdx != nil {
		log.Error("Execution client comparison mismatch", "method", method, "msgIdx", *msgIdx)
	} else {
		log.Error("Execution client comparison mismatch", "method", method)
	}

	// Send simple error to fatalErrChan (details already printed to stderr)
	if c.fatalErrChan != nil {
		if msgIdx != nil {
			c.fatalErrChan <- fmt.Errorf("%w in %s at msgIdx %d", ErrMismatch, method, *msgIdx)
		} else {
			c.fatalErrChan <- fmt.Errorf("%w in %s", ErrMismatch, method)
		}
	}
}

// comparePromises awaits two promises, compares results AND errors using cmp
// Sends to fatalErrChan on mismatch, always returns a primary result as a new promise
// Note: This is a standalone function because Go doesn't support generic methods
func comparePromises[T any](
	ctx context.Context,
	c *Comparator,
	method string,
	msgIdx *arbutil.MessageIndex,
	primary containers.PromiseInterface[T],
	secondary containers.PromiseInterface[T],
) containers.PromiseInterface[T] {
	return containers.DoPromise(ctx, func(ctx context.Context) (T, error) {
		primaryResult, primaryErr := primary.Await(ctx)
		secondaryResult, secondaryErr := secondary.Await(ctx)

		// Compare errors
		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			c.handleMismatch(method, msgIdx, err, primaryErr, secondaryErr)
		}

		// Compare results (only if both succeeded)
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				c.handleMismatch(method, msgIdx, err, primaryErr, secondaryErr)
			}
		}

		// Always return a primary result
		return primaryResult, primaryErr
	})
}

// CompareMessageResult compares *execution.MessageResult promises with deep block comparison on mismatch
func (c *Comparator) CompareMessageResult(
	ctx context.Context,
	method string,
	msgIdx arbutil.MessageIndex,
	primary, secondary containers.PromiseInterface[*execution.MessageResult],
) containers.PromiseInterface[*execution.MessageResult] {
	return containers.DoPromise(ctx, func(ctx context.Context) (*execution.MessageResult, error) {
		primaryResult, primaryErr := primary.Await(ctx)
		secondaryResult, secondaryErr := secondary.Await(ctx)

		// Compare errors
		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			c.handleMismatch(method, &msgIdx, err, primaryErr, secondaryErr)
		}

		// Compare results (only if both succeeded)
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				c.handleMismatch(method, &msgIdx, err, primaryErr, secondaryErr)
				// Perform deep block comparison when there's a result mismatch
				c.performDeepBlockComparison(ctx, primaryResult, secondaryResult, msgIdx)
			}
		}

		// Always return a primary result
		return primaryResult, primaryErr
	})
}

// CompareMessageResults compares []*execution.MessageResult promises with deep block comparison on mismatch
func (c *Comparator) CompareMessageResults(
	ctx context.Context,
	method string,
	msgIdxStart arbutil.MessageIndex,
	primary, secondary containers.PromiseInterface[[]*execution.MessageResult],
) containers.PromiseInterface[[]*execution.MessageResult] {
	return containers.DoPromise(ctx, func(ctx context.Context) ([]*execution.MessageResult, error) {
		primaryResults, primaryErr := primary.Await(ctx)
		secondaryResults, secondaryErr := secondary.Await(ctx)

		// Compare errors
		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			c.handleMismatch(method, &msgIdxStart, err, primaryErr, secondaryErr)
		}

		// Compare results (only if both succeeded)
		if primaryErr == nil && secondaryErr == nil {
			// First compare overall slice
			if err := compare(primaryResults, secondaryResults); err != nil {
				c.handleMismatch(method, &msgIdxStart, err, primaryErr, secondaryErr)

				// Perform deep comparison for each mismatched result
				minLen := len(primaryResults)
				if len(secondaryResults) < minLen {
					minLen = len(secondaryResults)
				}
				for i := 0; i < minLen; i++ {
					if compareErr := compare(primaryResults[i], secondaryResults[i]); compareErr != nil {
						msgIdx := msgIdxStart + arbutil.MessageIndex(i)
						c.performDeepBlockComparison(ctx, primaryResults[i], secondaryResults[i], msgIdx)
					}
				}
			}
		}

		// Always return primary results
		return primaryResults, primaryErr
	})
}

// CompareMessageIndex compares arbutil.MessageIndex promises
func (c *Comparator) CompareMessageIndex(
	ctx context.Context,
	method string,
	primary, secondary containers.PromiseInterface[arbutil.MessageIndex],
) containers.PromiseInterface[arbutil.MessageIndex] {
	return comparePromises(ctx, c, method, nil, primary, secondary)
}

// CompareEmpty compares only errors for void methods (struct{} results are always equal)
func (c *Comparator) CompareEmpty(
	ctx context.Context,
	method string,
	primary, secondary containers.PromiseInterface[struct{}],
) containers.PromiseInterface[struct{}] {
	return containers.DoPromise(ctx, func(ctx context.Context) (struct{}, error) {
		_, primaryErr := primary.Await(ctx)
		_, secondaryErr := secondary.Await(ctx)

		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			c.handleMismatch(method, nil, err, primaryErr, secondaryErr)
		}

		return struct{}{}, primaryErr
	})
}

// CompareEmptyWithMsgIdx compares only errors for void methods that have a message index
func (c *Comparator) CompareEmptyWithMsgIdx(
	ctx context.Context,
	method string,
	msgIdx arbutil.MessageIndex,
	primary, secondary containers.PromiseInterface[struct{}],
) containers.PromiseInterface[struct{}] {
	return containers.DoPromise(ctx, func(ctx context.Context) (struct{}, error) {
		_, primaryErr := primary.Await(ctx)
		_, secondaryErr := secondary.Await(ctx)

		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			c.handleMismatch(method, &msgIdx, err, primaryErr, secondaryErr)
		}

		return struct{}{}, primaryErr
	})
}

// CompareBool compares bool promises
func (c *Comparator) CompareBool(
	ctx context.Context,
	method string,
	primary, secondary containers.PromiseInterface[bool],
) containers.PromiseInterface[bool] {
	return comparePromises(ctx, c, method, nil, primary, secondary)
}

// CompareMaintenanceStatus compares *execution.MaintenanceStatus promises
func (c *Comparator) CompareMaintenanceStatus(
	ctx context.Context,
	method string,
	primary, secondary containers.PromiseInterface[*execution.MaintenanceStatus],
) containers.PromiseInterface[*execution.MaintenanceStatus] {
	return comparePromises(ctx, c, method, nil, primary, secondary)
}

// CompareUint64 compares uint64 promises
func (c *Comparator) CompareUint64(
	ctx context.Context,
	method string,
	msgIdx arbutil.MessageIndex,
	primary, secondary containers.PromiseInterface[uint64],
) containers.PromiseInterface[uint64] {
	return comparePromises(ctx, c, method, &msgIdx, primary, secondary)
}

// CompareRecordResult compares *execution.RecordResult promises
func (c *Comparator) CompareRecordResult(
	ctx context.Context,
	method string,
	msgIdx arbutil.MessageIndex,
	primary, secondary containers.PromiseInterface[*execution.RecordResult],
) containers.PromiseInterface[*execution.RecordResult] {
	return comparePromises(ctx, c, method, &msgIdx, primary, secondary)
}

// CompareHeader compares *types.Header promises
func (c *Comparator) CompareHeader(
	ctx context.Context,
	method string,
	blockNum *big.Int,
	primary, secondary containers.PromiseInterface[*types.Header],
) containers.PromiseInterface[*types.Header] {
	return containers.DoPromise(ctx, func(ctx context.Context) (*types.Header, error) {
		primaryResult, primaryErr := primary.Await(ctx)
		secondaryResult, secondaryErr := secondary.Await(ctx)

		// Compare errors
		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			c.handleMismatchWithBlockNum(method, blockNum, err, primaryResult, secondaryResult, primaryErr, secondaryErr)
		}

		// Compare results (only if both succeeded)
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				c.handleMismatchWithBlockNum(method, blockNum, err, primaryResult, secondaryResult, primaryErr, secondaryErr)
			}
		}

		return primaryResult, primaryErr
	})
}

// CompareHeaderByHash compares *types.Header promises fetched by hash
func (c *Comparator) CompareHeaderByHash(
	ctx context.Context,
	method string,
	hash common.Hash,
	primary, secondary containers.PromiseInterface[*types.Header],
) containers.PromiseInterface[*types.Header] {
	return containers.DoPromise(ctx, func(ctx context.Context) (*types.Header, error) {
		primaryResult, primaryErr := primary.Await(ctx)
		secondaryResult, secondaryErr := secondary.Await(ctx)

		// Compare errors
		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			c.handleMismatchWithHash(method, hash, err, primaryResult, secondaryResult, primaryErr, secondaryErr)
		}

		// Compare results (only if both succeeded)
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				c.handleMismatchWithHash(method, hash, err, primaryResult, secondaryResult, primaryErr, secondaryErr)
			}
		}

		return primaryResult, primaryErr
	})
}

// CompareReceipts compares []*types.Receipt promises
func (c *Comparator) CompareReceipts(
	ctx context.Context,
	method string,
	blockNum *big.Int,
	primary, secondary containers.PromiseInterface[[]*types.Receipt],
) containers.PromiseInterface[[]*types.Receipt] {
	return containers.DoPromise(ctx, func(ctx context.Context) ([]*types.Receipt, error) {
		primaryResult, primaryErr := primary.Await(ctx)
		secondaryResult, secondaryErr := secondary.Await(ctx)

		// Compare errors
		if err := compareErrors(primaryErr, secondaryErr); err != nil {
			c.handleMismatchWithBlockNum(method, blockNum, err, primaryResult, secondaryResult, primaryErr, secondaryErr)
		}

		// Compare results (only if both succeeded)
		if primaryErr == nil && secondaryErr == nil {
			if err := compare(primaryResult, secondaryResult); err != nil {
				c.handleMismatchWithBlockNum(method, blockNum, err, primaryResult, secondaryResult, primaryErr, secondaryErr)
			}
		}

		return primaryResult, primaryErr
	})
}

// handleMismatchWithBlockNum logs mismatch with block number context
func (c *Comparator) handleMismatchWithBlockNum(method string, blockNum *big.Int, err error, primary, secondary any, primaryErr, secondaryErr error) {
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "=============== EXECUTION CLIENT COMPARISON MISMATCH ===============")
	fmt.Fprintf(os.Stderr, "Method: %s | Block Number: %s\n", method, blockNum.String())
	if primaryErr != nil || secondaryErr != nil {
		fmt.Fprintf(os.Stderr, "Primary Error:   %v\n", primaryErr)
		fmt.Fprintf(os.Stderr, "Secondary Error: %v\n", secondaryErr)
	}
	fmt.Fprintln(os.Stderr, "Diff (- primary, + secondary):")
	fmt.Fprintln(os.Stderr, err.Error())
	fmt.Fprintln(os.Stderr, "===================================================================")
	fmt.Fprintln(os.Stderr, "")

	log.Error("Execution client comparison mismatch", "method", method, "blockNum", blockNum.String())

	if c.fatalErrChan != nil {
		c.fatalErrChan <- fmt.Errorf("%w in %s at block %s", ErrMismatch, method, blockNum.String())
	}
}

// handleMismatchWithHash logs mismatch with hash context
func (c *Comparator) handleMismatchWithHash(method string, hash common.Hash, err error, primary, secondary any, primaryErr, secondaryErr error) {
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "=============== EXECUTION CLIENT COMPARISON MISMATCH ===============")
	fmt.Fprintf(os.Stderr, "Method: %s | Hash: %s\n", method, hash.Hex())
	if primaryErr != nil || secondaryErr != nil {
		fmt.Fprintf(os.Stderr, "Primary Error:   %v\n", primaryErr)
		fmt.Fprintf(os.Stderr, "Secondary Error: %v\n", secondaryErr)
	}
	fmt.Fprintln(os.Stderr, "Diff (- primary, + secondary):")
	fmt.Fprintln(os.Stderr, err.Error())
	fmt.Fprintln(os.Stderr, "===================================================================")
	fmt.Fprintln(os.Stderr, "")

	log.Error("Execution client comparison mismatch", "method", method, "hash", hash.Hex())

	if c.fatalErrChan != nil {
		c.fatalErrChan <- fmt.Errorf("%w in %s for hash %s", ErrMismatch, method, hash.Hex())
	}
}

// performDeepBlockComparison fetches full block data from both clients and performs detailed comparison
func (c *Comparator) performDeepBlockComparison(ctx context.Context, primaryResult, secondaryResult *execution.MessageResult, msgIdx arbutil.MessageIndex) {
	if c.primary == nil || c.secondary == nil {
		return
	}

	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "=============== DEEP BLOCK COMPARISON ===============")
	fmt.Fprintf(os.Stderr, "Message Index: %d\n", msgIdx)
	fmt.Fprintln(os.Stderr, "")

	// Fetch headers using BlockHash
	var primaryHeader, secondaryHeader *types.Header
	var primaryHeaderErr, secondaryHeaderErr error

	if primaryResult != nil && primaryResult.BlockHash != (common.Hash{}) {
		primaryHeader, primaryHeaderErr = c.primary.GetHeaderByHash(primaryResult.BlockHash).Await(ctx)
	}
	if secondaryResult != nil && secondaryResult.BlockHash != (common.Hash{}) {
		secondaryHeader, secondaryHeaderErr = c.secondary.GetHeaderByHash(secondaryResult.BlockHash).Await(ctx)
	}

	// Compare headers
	fmt.Fprintln(os.Stderr, "=== HEADER COMPARISON ===")
	if primaryHeaderErr != nil || secondaryHeaderErr != nil {
		fmt.Fprintf(os.Stderr, "  Primary Header Error:   %v\n", primaryHeaderErr)
		fmt.Fprintf(os.Stderr, "  Secondary Header Error: %v\n", secondaryHeaderErr)
	} else if primaryHeader == nil && secondaryHeader == nil {
		fmt.Fprintln(os.Stderr, "  Both headers are nil")
	} else if primaryHeader == nil {
		fmt.Fprintln(os.Stderr, "  Primary header is nil, secondary is not")
	} else if secondaryHeader == nil {
		fmt.Fprintln(os.Stderr, "  Secondary header is nil, primary is not")
	} else {
		c.compareHeaders(primaryHeader, secondaryHeader)
	}
	fmt.Fprintln(os.Stderr, "")

	// Fetch receipts by block number (we need block number from header)
	if primaryHeader != nil && secondaryHeader != nil {
		blockNum := primaryHeader.Number
		fmt.Fprintf(os.Stderr, "=== RECEIPTS COMPARISON (Block %d) ===\n", blockNum)

		primaryReceipts, primaryReceiptsErr := c.primary.GetBlockReceipts(blockNum).Await(ctx)
		secondaryReceipts, secondaryReceiptsErr := c.secondary.GetBlockReceipts(blockNum).Await(ctx)

		if primaryReceiptsErr != nil || secondaryReceiptsErr != nil {
			fmt.Fprintf(os.Stderr, "  Primary Receipts Error:   %v\n", primaryReceiptsErr)
			fmt.Fprintf(os.Stderr, "  Secondary Receipts Error: %v\n", secondaryReceiptsErr)
		} else {
			c.compareReceipts(primaryReceipts, secondaryReceipts)
		}
	}

	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "=====================================================")
}

// compareHeaders compares two block headers field by field
func (c *Comparator) compareHeaders(primary, secondary *types.Header) {
	fields := []struct {
		name string
		pv   interface{}
		sv   interface{}
	}{
		{"ParentHash", primary.ParentHash, secondary.ParentHash},
		{"UncleHash", primary.UncleHash, secondary.UncleHash},
		{"Coinbase", primary.Coinbase, secondary.Coinbase},
		{"StateRoot", primary.Root, secondary.Root},
		{"TxHash", primary.TxHash, secondary.TxHash},
		{"ReceiptHash", primary.ReceiptHash, secondary.ReceiptHash},
		{"Difficulty", primary.Difficulty, secondary.Difficulty},
		{"Number", primary.Number, secondary.Number},
		{"GasLimit", primary.GasLimit, secondary.GasLimit},
		{"GasUsed", primary.GasUsed, secondary.GasUsed},
		{"Time", primary.Time, secondary.Time},
		{"Extra", primary.Extra, secondary.Extra},
		{"MixDigest", primary.MixDigest, secondary.MixDigest},
		{"Nonce", primary.Nonce, secondary.Nonce},
		{"BaseFee", primary.BaseFee, secondary.BaseFee},
	}

	hasMismatch := false
	for _, f := range fields {
		if !c.valuesEqual(f.pv, f.sv) {
			if !hasMismatch {
				hasMismatch = true
			}
			fmt.Fprintf(os.Stderr, "  %s: MISMATCH\n", f.name)
			fmt.Fprintf(os.Stderr, "    - %s\n", c.formatFieldValue(f.pv))
			fmt.Fprintf(os.Stderr, "    + %s\n", c.formatFieldValue(f.sv))
		}
	}

	if !hasMismatch {
		fmt.Fprintln(os.Stderr, "  All header fields match")
	}
}

// compareReceipts compares two receipt slices
func (c *Comparator) compareReceipts(primary, secondary []*types.Receipt) {
	if len(primary) != len(secondary) {
		fmt.Fprintf(os.Stderr, "  Receipt count mismatch: primary=%d, secondary=%d\n", len(primary), len(secondary))
	}

	minLen := len(primary)
	if len(secondary) < minLen {
		minLen = len(secondary)
	}

	for i := 0; i < minLen; i++ {
		c.compareSingleReceipt(i, primary[i], secondary[i])
	}

	// Report extra receipts
	for i := minLen; i < len(primary); i++ {
		fmt.Fprintf(os.Stderr, "  Receipt[%d]: Only in primary (TxHash: %s)\n", i, primary[i].TxHash.Hex())
	}
	for i := minLen; i < len(secondary); i++ {
		fmt.Fprintf(os.Stderr, "  Receipt[%d]: Only in secondary (TxHash: %s)\n", i, secondary[i].TxHash.Hex())
	}
}

// compareSingleReceipt compares two receipts
func (c *Comparator) compareSingleReceipt(idx int, primary, secondary *types.Receipt) {
	fields := []struct {
		name string
		pv   interface{}
		sv   interface{}
	}{
		{"Type", primary.Type, secondary.Type},
		{"Status", primary.Status, secondary.Status},
		{"CumulativeGasUsed", primary.CumulativeGasUsed, secondary.CumulativeGasUsed},
		{"GasUsed", primary.GasUsed, secondary.GasUsed},
		{"GasUsedForL1", primary.GasUsedForL1, secondary.GasUsedForL1},
		{"TxHash", primary.TxHash, secondary.TxHash},
		{"ContractAddress", primary.ContractAddress, secondary.ContractAddress},
		{"BlockHash", primary.BlockHash, secondary.BlockHash},
		{"BlockNumber", primary.BlockNumber, secondary.BlockNumber},
		{"TransactionIndex", primary.TransactionIndex, secondary.TransactionIndex},
	}

	hasMismatch := false
	var mismatches []string

	for _, f := range fields {
		if !c.valuesEqual(f.pv, f.sv) {
			hasMismatch = true
			mismatches = append(mismatches, fmt.Sprintf("    %s:\n      - %s\n      + %s",
				f.name, c.formatFieldValue(f.pv), c.formatFieldValue(f.sv)))
		}
	}

	// Compare logs count
	if len(primary.Logs) != len(secondary.Logs) {
		hasMismatch = true
		mismatches = append(mismatches, fmt.Sprintf("    Logs count: %d vs %d",
			len(primary.Logs), len(secondary.Logs)))
	}

	// Compare Bloom
	if primary.Bloom != secondary.Bloom {
		hasMismatch = true
		mismatches = append(mismatches, "    Bloom: MISMATCH")
	}

	if hasMismatch {
		fmt.Fprintf(os.Stderr, "  Receipt[%d] (TxHash: %s): MISMATCH\n", idx, primary.TxHash.Hex())
		for _, m := range mismatches {
			fmt.Fprintln(os.Stderr, m)
		}
	}
}

// valuesEqual compares two interface values for equality
func (c *Comparator) valuesEqual(a, b interface{}) bool {
	// Handle nil cases
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Handle *big.Int
	if aBig, ok := a.(*big.Int); ok {
		if bBig, ok := b.(*big.Int); ok {
			if aBig == nil && bBig == nil {
				return true
			}
			if aBig == nil || bBig == nil {
				return false
			}
			return aBig.Cmp(bBig) == 0
		}
	}

	// Handle []byte
	if aBytes, ok := a.([]byte); ok {
		if bBytes, ok := b.([]byte); ok {
			if len(aBytes) != len(bBytes) {
				return false
			}
			for i := range aBytes {
				if aBytes[i] != bBytes[i] {
					return false
				}
			}
			return true
		}
	}

	return reflect.DeepEqual(a, b)
}

// formatFieldValue formats a field value for display
func (c *Comparator) formatFieldValue(v interface{}) string {
	if v == nil {
		return "<nil>"
	}

	switch val := v.(type) {
	case common.Hash:
		return val.Hex()
	case common.Address:
		return val.Hex()
	case *big.Int:
		if val == nil {
			return "<nil>"
		}
		return val.String()
	case []byte:
		if len(val) == 0 {
			return "0x"
		}
		return fmt.Sprintf("0x%x", val)
	case types.BlockNonce:
		return fmt.Sprintf("0x%x", val[:])
	default:
		return fmt.Sprintf("%v", val)
	}
}
