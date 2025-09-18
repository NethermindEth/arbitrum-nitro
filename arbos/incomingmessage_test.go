// Copyright 2021-2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package arbos

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/params"

	"github.com/offchainlabs/nitro/arbos/arbostypes"
)

func TestMyL1Message(t *testing.T) {
	chainId := big.NewInt(100)
	header := arbostypes.L1IncomingMessageHeader{
		Kind:        arbostypes.L1MessageType_L2Message,
		Poster:      common.HexToAddress("0xa4b000000000000000000073657175656e636572"),
		BlockNumber: 4899309,
		Timestamp:   1702751136,
		RequestId:   nil,
		L1BaseFee:   big.NewInt(0),
	}
	message := "03000000000000007b0402f87783066eee821cf68459682f00846553f10083011a6894587a22412baf06461b5527c3f299bc9f6afa50b2872386f26fc1000080c001a0f65c119f4fe91593441067401f6a4eb732c142907288d2b2b97647480c8ac334a01282533be53f2fb245a9187fbe6139a64c4f39d495b28b9c79f4bb7fd069c67800000000000000f104f8ee821d788405f5e1008302534794b4e481324f4c581f8f0ae49f46024943b3486cba80b884d318486c000000000000000000000000000000000000000000000000000000000000002a0000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000000b3c994842ae3ea7b5f8bd93000000000000000000000000000000000000000000830cddffa0307083a4c8cbf387a9933327c892f866c063697e4e559fdde4d606086d3deec6a00e9498147d3c6c96edf7d8f0d479e97f4faa846a5868a8ba6c12d111c878e48c"
	data := common.Hex2Bytes(message)
	msg := arbostypes.L1IncomingMessage{
		Header:       &header,
		L2msg:        data,
		BatchGasCost: nil,
	}

	txes, _ := ParseL2Transactions(&msg, chainId)

	if len(txes) == 0 {
		Fail(t, "unexpected tx count")
	}
}

func TestSerializeAndParseL1Message(t *testing.T) {
	chainId := big.NewInt(6345634)
	requestId := common.BigToHash(big.NewInt(3))
	header := arbostypes.L1IncomingMessageHeader{
		Kind:        arbostypes.L1MessageType_EndOfBlock,
		Poster:      common.BigToAddress(big.NewInt(4684)),
		BlockNumber: 864513,
		Timestamp:   8794561564,
		RequestId:   &requestId,
		L1BaseFee:   big.NewInt(10000000000000),
	}
	msg := arbostypes.L1IncomingMessage{
		Header:             &header,
		L2msg:              []byte{3, 2, 1},
		LegacyBatchGasCost: nil,
		BatchDataStats:     nil,
	}
	serialized, err := msg.Serialize()
	if err != nil {
		t.Error(err)
	}
	newMsg, err := arbostypes.ParseIncomingL1Message(bytes.NewReader(serialized), nil)
	if err != nil {
		t.Error(err)
	}
	txes, err := ParseL2Transactions(newMsg, chainId, params.MaxDebugArbosVersionSupported)
	if err != nil {
		t.Error(err)
	}
	if len(txes) != 0 {
		Fail(t, "unexpected tx count")
	}
}
