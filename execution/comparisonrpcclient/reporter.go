package comparisonrpcclient

import (
	"fmt"
	"math/big"
	"reflect"
	"strings"

	"github.com/google/go-cmp/cmp"

	"github.com/ethereum/go-ethereum/common"

	"github.com/offchainlabs/nitro/arbutil"
)

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

// MismatchReport contains details about a comparison mismatch
type MismatchReport struct {
	Method       string
	MsgIdx       *arbutil.MessageIndex
	BlockNum     *big.Int
	Hash         *common.Hash
	Diff         error
	PrimaryErr   error
	SecondaryErr error
}
