# Arbitrum Nitro Architecture Guide

## Overview

Arbitrum Nitro is the canonical implementation of the Arbitrum L2 optimistic rollup system. It's a complete Layer 2 blockchain that runs Ethereum-compatible transactions while providing fraud proofs, cross-chain communication, and advanced calldata compression.

**Key Innovation**: Nitro can prove fraud over WASM-compiled code, allowing the core execution engine (Geth, the EVM) and ArbOS to be written in standard languages (Go/Rust) instead of custom instruction sets, then compiled to WASM for dispute resolution.

## High-Level Architecture

```
L1 Ethereum Network
    │
    ├─► Inbox Contract ────► arbnode/inbox_reader.go
    │                          (reads L1 messages)
    │
    ├─► SequencerInbox ────► arbnode/transaction_streamer.go
    │   (batches)             (converts messages to L2 blocks)
    │
    └─► RollupCore ◄────── staker/
        (assertions,          (posts assertions, challenges)
         challenges)          
            │
            └─► Fraud Proofs
                 (arbitrator/prover/)
                 (Go↔Rust interop)
                 
         ↓
    
    L2 State Machine
    ┌─────────────────────────────────┐
    │  arbnode/                       │
    │  ├─ inbox_tracker.go            │
    │  ├─ batch_poster.go             │
    │  ├─ transaction_streamer.go      │
    │  └─ sequencer_inbox.go          │
    │                                 │
    │  arbos/                         │
    │  ├─ block_processor.go          │
    │  ├─ tx_processor.go             │
    │  ├─ arbosState/                 │
    │  ├─ l1pricing/                  │
    │  ├─ l2pricing/                  │
    │  ├─ programs/ (Stylus WASM)     │
    │  └─ precompiles/                │
    │                                 │
    │  execution/gethexec/            │
    │  └─ Geth EVM Integration        │
    │                                 │
    │  validator/                     │
    │  └─ block_validator.go          │
    │     (validates blocks)          │
    └─────────────────────────────────┘
            │
            ├─► State Root
            ├─► Block Hash
            └─► Message Commitments
```

## Core Components

### 1. Node Layer (arbnode/)

The node layer bridges L1 and L2, handling message ingestion and batch posting.

**Key Components:**

- **inbox_tracker.go**: Watches Ethereum L1 for messages posted to the Inbox and SequencerInbox contracts. Tracks the message count and creates the canonical ordering of L1→L2 messages.

- **transaction_streamer.go**: The critical deterministic engine that converts a stream of L1 messages into L2 blocks. This is the source of truth for block creation—same inputs must produce identical blocks across all nodes.
  - Creates L2 blocks from messages
  - Handles block boundaries (EndOfBlock messages)
  - Manages delayed messages (force-inclusion from L1)

- **batch_poster.go**: Takes accumulated L2 transactions and posts them back to L1 in compressed batches via the SequencerInbox contract. Handles:
  - Calldata compression (Brotli)
  - EIP-4844 blob support
  - L1 cost estimation
  - Transaction ordering

- **sequencer_inbox.go**: Reads delayed messages from the Inbox contract for force-inclusion, enabling censorship resistance.

- **delayed_sequencer.go**: Handles the delayed message queue and force-inclusion deadlines.

**Critical Invariant**: Message→Block conversion is **deterministic** and must be reproducible exactly. This is essential for:
- Validator agreement
- Fraud proof generation
- Execution client compatibility (e.g., Nethermind)

### 2. ArbOS Layer (arbos/)

ArbOS is the Arbitrum Operating System—a Go implementation of system-level operations needed for an L2 chain.

**Architecture:**

```
arbosState/arbosstate.go (Root State Object)
    ├─ L1PricingState        → L1 data cost tracking
    ├─ L2PricingState        → L2 gas pricing
    ├─ RetryableState        → Retryable tickets
    ├─ AddressTable          → Address compression
    ├─ AddressSet            → Set operations
    ├─ BlockhashState        → Block hash storage
    ├─ ProgramsState         → Stylus WASM programs
    └─ [other subspaces]
```

**Key Components:**

- **block_processor.go**: Entry point for block execution. Orchestrates:
  1. Block header creation
  2. Transaction processing loop
  3. State updates
  4. Block finalization

- **tx_processor.go**: Processes individual transactions:
  - Validates transaction format
  - Applies ArbOS transaction types (deposits, retryables, internal txs)
  - Delegates to EVM for standard EVM txs
  - Tracks L1 gas costs

- **l1pricing/**: Calculates costs for L1 data posted on-chain:
  - Tracks base fee (L1 gas price)
  - Tracks surplus (demand above base fee)
  - Updates batch poster spending
  - Charges users for their contribution to batch data

- **l2pricing/**: Standard EVM-like gas pricing for L2 execution:
  - Base fee adjusts with congestion
  - Gas per opcode follows Geth
  - ArbOS system transactions have specific costs

- **programs/**: Interface to Stylus WASM execution:
  - Activation and pricing of WASM contracts
  - Memory management and metering
  - Ink (WASM gas) to L2 gas conversion
  - Native code compilation via JIT

**Transaction Types** (arbos/internal_tx.go, parse_l2.go):
- Type 0: EVM standard tx
- Type 100 (ArbitrumDepositTx): L1→L2 value transfer
- Type 101 (SubmitRetryable): Create retryable ticket
- Type 102 (ArbitrumUnsignedTx): System tx (no sig)
- Type 104 (ArbitrumContractTx): Contract deployment
- Type 105 (ArbitrumRetryTx): Redeem retryable
- Type 106 (ArbitrumInternalTx): ArbOS internal operations

**Critical Invariant**: The order of state operations affects gas consumption. When implementing in other clients (e.g., Nethermind), you must call ArbOS methods in the **exact same order** as Nitro to ensure identical gas consumption.

### 3. Precompiles (precompiles/)

Arbitrum-specific precompiled contracts accessible from EVM code:

| Address | Name | Purpose |
|---------|------|---------|
| 0x64    | ArbSys | Block numbers, L2→L1 messaging, withdrawals |
| 0x65    | ArbInfo | Network parameters |
| 0x66    | ArbAddressTable | Address compression/decompression |
| 0x6C    | ArbGasInfo | Gas price queries (L1 base fee, current tx costs) |
| 0x6E    | ArbRetryableTx | Retryable management (redeem, lifetime) |
| 0x70    | ArbOwner | Chain governance (ArbOS upgrades, pricing) |
| 0x71    | ArbWasm | Stylus program activation |

**Implementation Pattern**: Each precompile is a Go struct implementing the `ArbosPrecompile` interface:
```go
type ArbosPrecompile interface {
    Call(input []byte, precompileAddr, actingAsAddr, caller common.Address, 
         value *big.Int, readOnly bool, gasSupplied uint64, evm *vm.EVM) 
         (output []byte, gasLeft uint64, err error)
    Precompile() *Precompile
}
```

### 4. Stylus WASM Execution (arbitrator/, arbos/programs/)

Arbitrum's smart contract system supports two execution environments:
1. **EVM** (default): Traditional Solidity via Geth
2. **Stylus** (optional): WASM-based contracts in Rust, C++, etc.

**Architecture:**

```
User WASM Contract (Rust/C++)
    │
    ▼
arbitrator/stylus/src/ (Rust WASM Runtime)
    ├─ JIT Compilation (arbitrator/jit/)
    │   └─ Native x86 code generation for speed
    └─ Execution Engine
        ├─ Memory management
        ├─ Ink metering (WASM gas)
        ├─ Host I/O operations
        └─ State access

    ▼
arbos/programs/ (Go bindings)
    ├─ programs.go: Activation, lifecycle
    ├─ wasm.go: Execution and gas conversion
    ├─ native.go: JIT compilation interface
    └─ memory.go: Memory access helpers

    ▼
EVM Precompile (ArbWasm)
    └─ Solidity contracts interact via precompile
```

**Fraud Proof Integration**: 
- Stylus programs can also be proved in WASM form
- Validator: arbitrator/prover/src/machine.rs implements WASM machine
- Go calls Rust via FFI (arbitrator/prover/src/lib.rs) with cgo

### 5. Execution Engine (execution/)

Arbitrum integrates **go-ethereum** (Geth) as the execution engine via a fork.

**gethexec/** provides:
- **executionengine.go**: Core EVM execution with Arbitrum hooks
- **blockchain.go**: Block validation and chain building
- **sequencer.go**: Sequencer-specific logic
- Transaction queue management
- Block building pipeline

**Key Integration Points:**
- Custom `Engine` consensus algorithm (arbos/engine.go) that validates Arbitrum blocks
- Hooks in transaction processing for ArbOS system transactions
- Custom precompile registry pointing to Arbitrum precompiles
- State trie snapshots for fraud proofs

### 6. Validators & Stakers (staker/)

Two parallel validation protocols:

**Legacy Protocol (staker/legacy/):**
- Original Arbitrum challenge system
- Single-step interactive fraud proofs via OneStepProof contracts
- `challenge_manager.go`: Manages challenge state machine
- `l1_validator.go`: Submits assertions and validates blocks

**BOLD Protocol (staker/bold/):**
- New efficient challenge protocol with history commitments
- Multi-level edge-based disputes
- Reduced gas costs and faster finality
- `bold_staker.go`: BOLD-specific assertion and challenge logic
- `bold_state_provider.go`: Provides L2 state to BOLD challenge manager

**Block Validator (staker/block_validator.go):**
- Continuously validates L2 blocks using the arbitrator
- Communicates with validator via RPC or Redis
- Manages validation record queue
- Triggers dispute if validator detects invalid blocks

### 7. Arbitrator & Fraud Proofs (arbitrator/)

The fraud proof engine that can prove Arbitrum execution in WASM format.

**Rust Components:**

- **arbitrator/prover/src/machine.rs**: Virtual machine for executing Arbitrum instructions
  - WAVM instruction set (Arbitrum's custom WASM-like ISA)
  - State management and stepping
  - Preimage resolution interface

- **arbitrator/prover/src/main.rs**: CLI tool for generating proofs
  - Loads WASM binary and libraries
  - Adds messages and preimages
  - Steps machine and generates fraud proofs

- **arbitrator/jit/src/**: JIT compilation for native execution
  - Converts WAVM to x86-64
  - Massive speedup for dispute resolution

**Go Integration (validator/server_arb/):**

- **prover_interface.go**: FFI bindings to Rust via cgo
- **validator_spawner.go**: Launches validation tasks
  - Creates Machine instances
  - Sets up preimage resolver
  - Executes validation runs
  - Returns global state (batch, pos, blockhash, sendroot)

- **execution_run.go**: Manages proof generation
  - GetStepAt: Retrieve machine state at step N
  - GetProofAt: Generate proof for a step
  - GetMachineHashesWithStepSize: Bisection for challenges

**WAVM (Arbitrum Virtual Machine):**
- Custom instruction set designed for proving
- Supports:
  - Arithmetic operations
  - Memory access
  - Function calls
  - Cross-module calls (for libraries)
  - Host I/O operations

### 8. Smart Contracts (contracts/src/)

Solidity contracts deployed on L1 that manage Arbitrum's rollup logic.

**Key Contracts:**

- **RollupCore.sol**: Core rollup logic
  - Tracks assertions (state commitments)
  - Manages validator bonds
  - Handles challenge protocol
  - Legacy and BOLD implementations

- **Bridge.sol**: Manages Arbitrum bridge
  - L1→L2 deposit queue
  - L2→L1 message outbox

- **SequencerInbox.sol**: Batch submission
  - Accepts compressed batches
  - EIP-4844 blob support
  - Delay buffer for force-inclusion

- **OneStepProver*.sol**: Verifies single-step proofs
  - OneStepProverMemory.sol
  - OneStepProverHostIo.sol
  - OneStepProverMath.sol
  - Verifies WAVM execution steps

## Data Flow & Message Processing

### Block Creation Flow

```
1. L1 Message Posted
   (SequencerInbox or Inbox contract)
        │
        ▼
2. inbox_reader.go Detects Message
   (polls L1 every block)
        │
        ▼
3. inbox_tracker.go Stores Message
   (in database)
        │
        ▼
4. transaction_streamer.go Converts to L2 Blocks
   (deterministic message→block conversion)
   - Breaks messages into transactions
   - Creates block boundaries at EndOfBlock messages
   - Handles delayed messages and retryables
        │
        ▼
5. execution/gethexec Creates Block
   (calls Geth)
        │
        ▼
6. arbos/block_processor Executes Block
   - Processes each transaction via tx_processor
   - Updates ArbOS state (pricing, retryables, etc.)
   - Returns state root
        │
        ▼
7. blockchain.go Stores Block & State
   (leveldb)
        │
        ▼
8. batch_poster.go Accumulates Blocks
   (when threshold reached or time elapsed)
        │
        ▼
9. Compressed Batch Posted to L1
   (SequencerInbox contract)
```

### Message Types

Defined in `arbos/arbostypes/`:

| Type | Name | Purpose |
|------|------|---------|
| 0 | L2Message | Standard L2 transaction |
| 1 | EndOfBlock | Marks L2 block boundary |
| 2 | L2FundedByL1 | L1→L2 deposit |
| 3 | RollupEvent | System event |
| 4 | SubmitRetryable | Create retryable ticket |

### Retryable Tickets

Mechanism for L1→L2 atomic actions:
1. User calls SequencerInbox.submitRetryable (on L1)
2. Message enters L2 as SubmitRetryable type
3. ArbRetryableTx precompile creates ticket
4. Ticket redeemable on L2 with `redeem()` within lifetime
5. Automatic redemption attempt at creation

## Gas Model & Pricing

### L1 Data Costs (l1pricing/)

Users pay for their share of batch posting costs:

```
L1 Gas Cost = (BatchSize / TotalUsers) × (GasPrice × GasUsed)
                + Surplus (if batch expensive compared to baseline)
```

**Calculation:**
1. Batch poster submits transaction with data
2. Actual L1 gas cost recorded
3. Basis of surcharge calculated based on target vs actual
4. Users charged in their transactions' execution

### L2 Execution Costs (l2pricing/)

Standard EVM gas model with Arbitrum adjustments:
- **Base Fee**: Adjusts with L2 congestion (typical EVM)
- **Gas per Opcode**: Matches Geth (slightly lower than Ethereum)
- **ArbOS Operations**: Custom gas costs for precompiles
- **Stylus Ink**: WASM gas metered separately, converted to L2 gas

### Ink to Gas Conversion

Stylus contracts use "ink" (WASM gas):
```
L2 Gas = InkUsed / WasmGasToL2Gas  (typically ~100 ink per gas)
```

## Testing Infrastructure

### System Tests (system_tests/)

End-to-end tests that:
- Create a local L1 (anvil/hardhat) and L2 network
- Deploy Arbitrum contracts
- Send transactions through full stack
- Verify state consistency

**Test Patterns:**
```go
type NodeBuilder struct {
    // Creates full test node
}

// Usage:
builder := NewNodeBuilder(t)
l2Node := builder.Build(t)
l2Client := l2Node.Client  // ethclient.Client
```

### Block Validator Tests (staker/)

- unit tests for block validation
- fraud proof generation tests
- challenge protocol tests

### Unit Tests

Located in component directories:
- `arbos/*_test.go`: ArbOS logic
- `precompiles/*_test.go`: Precompile behavior
- `arbnode/*_test.go`: Node coordination

## Architectural Patterns & Design Decisions

### 1. Deterministic Execution

**Pattern**: Same inputs → identical outputs, always
**Why**: Enables offline fraud proof generation without L2 execution
**How**: 
- No random numbers
- No timestamps except at specific boundaries
- No floating-point math (use big.Int)
- Deterministic message ordering from L1

### 2. Storage Abstraction

**Pattern**: Storage-backed types (`storage.StorageBackedBigInt`, etc.)
**Why**: Maps Solidity storage layout to Go structs for state management
**How**: Wrappers implement `storage.Storage` interface, persist to blockchain state

### 3. Gas Order Sensitivity

**Pattern**: Order of operations affects gas consumption
**Why**: Precompiles and ArbOS changes must track pricing state changes
**Example**: Calling `UpdateForBatchPosterSpending()` before a state check changes the result
**Implication**: Implementation order matters, must match Nitro exactly

### 4. Layered Validation

**Pattern**: Validation happens at multiple levels:
1. **Mempool Level**: `execution/gethexec/txPreChecker.go` pre-validates
2. **Block Level**: `block_processor.go` validates during execution
3. **Validator Level**: `block_validator.go` re-validates after execution
**Why**: Early rejection saves resources; post-execution validation catches bugs

### 5. FFI Bridge (Go ↔ Rust)

**Pattern**: Rust prover exposed via cgo to Go validator
**How**:
- C header in `validator/server_arb/prover_interface.go`
- Machine lifecycle: create → load preimages → step → serialize proof
- Memory management: Rust allocates, Go frees via `free_rust_bytes`

### 6. Message Determinism with Delayed Messages

**Pattern**: Two inboxes for ordering flexibility:
- Sequencer Inbox: Fast, sequencer-ordered
- Delayed Inbox: Slow, force-include after delay
**Why**: Sequencer can post transactions, but L1 can force-include delayed txs after timeout
**How**: `delayed_sequencer.go` ensures deadlines respected

### 7. Fraud Proof via WASM

**Pattern**: Proof by re-execution in WASM
**Why**: WASM is portable and verifiable on-chain
**How**:
1. Arbitrator compiles Nitro to WASM (geth + ArbOS + libs)
2. Validator/prover steps through WASM execution
3. One-step proof generated for specific step
4. Solidity contract verifies proof on-chain

### 8. Precompile Pattern

**Pattern**: Dynamic method registry with ABI reflection
**Implementation**: `precompiles/precompile.go`
```go
type ArbosPrecompile interface {
    Call(...) (output []byte, gasLeft uint64, err error)
}
// Registry built via reflection on implementer type
```
**Why**: Extensible without contract changes, auto-generates ABI

## Critical Invariants & Constraints

1. **Deterministic Message Processing**: `transaction_streamer.go` output must be identical across all nodes and clients
2. **Gas Order Dependency**: State changes that affect gas must happen in the correct sequence
3. **State Consistency**: All nodes must produce identical state roots for identical message sequences
4. **Precompile Exactness**: Return values, gas costs, and event logs must match across implementations
5. **Fraud Proof Completeness**: Arbitrator must be able to prove any block execution step

## Build & Compilation

### Building the Node

```bash
make build              # Builds arbitrator (Rust) and nitro binary (Go)
make build-wasm-libs   # Compiles WASM libraries for arbitrator
```

### Building the Arbitrator

```bash
cd arbitrator
cargo build --release --features native
# Generates target/lib/libstylus.a and target/include/arbitrator.h
```

### WASM Compilation for Fraud Proofs

```
ArbOS + Geth + Libraries  (Go/C code)
    ↓
WASM target (via wasm32-unknown-unknown)
    ↓
WAVM format (Arbitrum's custom WASM ISA)
    ↓
WAVM binary (ready for fraud proofs)
```

## Cross-Client Compatibility

Arbitrum has multiple implementations:
- **Nitro** (Go + Rust): Reference implementation
- **Nethermind** (C#): Ethereum client plugin
- **Erigon** (Go): Lightweight client

**Critical**: All clients must produce identical results for:
- Block creation
- State transitions
- Gas consumption
- Fraud proofs

This is achieved by:
1. Deterministic message processing
2. Exact ArbOS reimplementation
3. Matching precompile behavior
4. Identical transaction ordering

## Entry Points

### Node Startup
- `cmd/nitro/main.go`: CLI entry point
- `arbnode/node.go`: Node initialization
- `execution/gethexec/executionengine.go`: Geth integration startup

### Transaction Processing
- `transaction_streamer.go`: Message ingestion
- `block_processor.go`: Block execution
- `tx_processor.go`: Transaction processing

### Validation
- `staker/block_validator.go`: Block validation loop
- `validator/server_arb/validator_spawner.go`: Spawns validation tasks

### Fraud Proofs
- `arbitrator/prover/src/machine.rs`: Machine execution
- `validator/server_arb/execution_run.go`: Runs machine for proofs

## Key External Dependencies

- **go-ethereum** (Geth): EVM execution
- **geth fork**: Custom modifications for Arbitrum
- **bold repository**: BOLD challenge protocol
- **Wasmer/Wasmtime**: WASM runtime (in arbitrator/tools/)

## Performance Optimizations

1. **JIT Compilation** (arbitrator/jit): Converts WAVM to native x86 for 100x+ speedup
2. **Calldata Compression**: Brotli compression in batch_poster
3. **EIP-4844 Blob Support**: Reduced rollup costs via proto-danksharding
4. **Address Table**: 20-byte addresses compressed to 4 bytes in calldata
5. **Preimage Caching**: Blob hash preimages cached during fraud proofs

## Future Directions

1. **BOLD Integration**: Gradual migration to BOLD protocol for cheaper disputes
2. **Stylus Expansion**: Broader WASM contract support
3. **Consensus Modularity**: Potential separation of consensus from execution
4. **Vertical Scaling**: Further calldata optimizations and proof compression

---

## Quick Reference: File Locations

| Feature | File |
|---------|------|
| L1 Message Reading | arbnode/inbox_reader.go |
| Message → Block | arbnode/transaction_streamer.go |
| Block Execution | arbos/block_processor.go |
| Transaction Processing | arbos/tx_processor.go |
| L1 Data Pricing | arbos/l1pricing/ |
| L2 Gas Pricing | arbos/l2pricing/ |
| Precompiles | precompiles/ |
| Stylus Runtime | arbitrator/stylus/ |
| Fraud Proofs | arbitrator/prover/, validator/server_arb/ |
| Node Init | arbnode/node.go |
| Validation | staker/block_validator.go |
| Batch Posting | arbnode/batch_poster.go |
| Smart Contracts | contracts/src/ |
| System Tests | system_tests/ |

---

*Last updated: October 2024*
*Reference: Arbitrum Nitro repository*
