# Copyright 2021-2026, Offchain Labs, Inc.
# For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

# Docker builds mess up file timestamps. Then again, in docker builds we never
# have to update an existing file. So - for docker, convert all dependencies
# to order-only dependencies (timestamps ignored).
# WARNING: when using this trick, you cannot use the $< automatic variable

ifeq ($(origin NITRO_BUILD_IGNORE_TIMESTAMPS),undefined)
 DEP_PREDICATE:=
 ORDER_ONLY_PREDICATE:=|
else
 DEP_PREDICATE:=|
 ORDER_ONLY_PREDICATE:=
endif


ifneq ($(origin NITRO_VERSION),undefined)
 GOLANG_LDFLAGS += -X github.com/offchainlabs/nitro/cmd/util/confighelpers.version=$(NITRO_VERSION)
endif

ifneq ($(origin NITRO_DATETIME),undefined)
 GOLANG_LDFLAGS += -X github.com/offchainlabs/nitro/cmd/util/confighelpers.datetime=$(NITRO_DATETIME)
endif

ifneq ($(origin NITRO_MODIFIED),undefined)
 GOLANG_LDFLAGS += -X github.com/offchainlabs/nitro/cmd/util/confighelpers.modified=$(NITRO_MODIFIED)
endif

ifneq ($(origin GOLANG_LDFLAGS),undefined)
 GOLANG_PARAMS = -ldflags="-extldflags '-ldl' $(GOLANG_LDFLAGS)"
endif

UNAME_S := $(shell uname -s)

# In Mac OSX, there are a lot of warnings emitted if these environment variables aren't set.
ifeq ($(UNAME_S), Darwin)
  export MACOSX_DEPLOYMENT_TARGET := $(shell sw_vers -productVersion)
  export CGO_LDFLAGS := -Wl,-no_warn_duplicate_libraries
endif

precompile_names = AddressTable Aggregator BLS Debug FunctionTable GasInfo Info osTest Owner RetryableTx Statistics Sys
precompiles = $(patsubst %,./solgen/generated/%.go, $(precompile_names))

output_root=target
output_latest=$(output_root)/machines/latest

repo_dirs = arbos arbcompress arbnode arbutil arbstate cmd das precompiles solgen system_tests util validator wavmio
go_source.go = $(wildcard $(patsubst %,%/*.go, $(repo_dirs)) $(patsubst %,%/*/*.go, $(repo_dirs)))
go_source.s  = $(wildcard $(patsubst %,%/*.s, $(repo_dirs)) $(patsubst %,%/*/*.s, $(repo_dirs)))
go_source = $(go_source.go) $(go_source.s)

color_pink = "\e[38;5;161;1m"
color_reset = "\e[0;0m"

done = "%bdone!%b\n" $(color_pink) $(color_reset)

replay_wasm=$(output_latest)/replay.wasm

arb_brotli_files = $(wildcard crates/brotli/src/*.* crates/brotli/src/*/*.* crates/brotli/*.toml crates/brotli/*.rs) .make/cbrotli-lib .make/cbrotli-wasm

arbitrator_generated_header=$(output_root)/include/arbitrator.h
arbitrator_wasm_libs=$(patsubst %, $(output_root)/machines/latest/%.wasm, forward wasi_stub host_io soft-float arbcompress arbcrypto user_host program_exec)
arbitrator_stylus_lib=$(output_root)/lib/libstylus.a
prover_bin=$(output_root)/bin/prover
arbitrator_jit=$(output_root)/bin/jit
validation_server=$(output_root)/bin/validator

arbitrator_cases=crates/prover/test-cases

arbitrator_tests_wat=$(wildcard $(arbitrator_cases)/*.wat)
arbitrator_tests_rust=$(wildcard $(arbitrator_cases)/rust/src/bin/*.rs)

arbitrator_test_wasms=$(patsubst %.wat,%.wasm, $(arbitrator_tests_wat)) $(patsubst $(arbitrator_cases)/rust/src/bin/%.rs,$(arbitrator_cases)/rust/target/wasm32-wasip1/release/%.wasm, $(arbitrator_tests_rust)) $(arbitrator_cases)/go/testcase.wasm

arbitrator_tests_link_info = $(shell cat $(arbitrator_cases)/link.txt | xargs)
arbitrator_tests_link_deps = $(patsubst %,$(arbitrator_cases)/%.wasm, $(arbitrator_tests_link_info))

arbitrator_tests_forward_wats = $(wildcard $(arbitrator_cases)/forward/*.wat)
arbitrator_tests_forward_deps = $(arbitrator_tests_forward_wats:wat=wasm)

WASI_SYSROOT?=/opt/wasi-sdk/wasi-sysroot

arbitrator_wasm_lib_flags=$(patsubst %, -l %, $(arbitrator_wasm_libs))

rust_arbutil_files = $(wildcard crates/arbutil/src/*.* crates/arbutil/src/*/*.* crates/arbutil/*.toml crates/caller-env/src/*.* crates/caller-env/src/*/*.* crates/caller-env/*.toml) .make/cbrotli-lib

prover_direct_includes = $(patsubst %,$(output_latest)/%.wasm, forward forward_stub)
prover_dir = crates/prover/
prover_ffi_dir = crates/prover-ffi/
rust_prover_files = $(wildcard $(prover_dir)/src/*.* $(prover_dir)/src/*/*.* $(prover_dir)/*.toml $(prover_dir)/*.rs) $(wildcard $(prover_ffi_dir)/src/*.rs $(prover_ffi_dir)/*.toml) $(rust_arbutil_files) $(prover_direct_includes) $(arb_brotli_files)

wasm_lib = crates/wasm-libraries
wasm_lib_cargo = $(wasm_lib)/.cargo/config.toml
wasm_lib_deps = $(wildcard $(wasm_lib)/$(1)/*.toml $(wasm_lib)/$(1)/src/*.rs $(wasm_lib)/$(1)/*.rs) $(wasm_lib_cargo) $(rust_arbutil_files) $(arb_brotli_files) .make/machines
wasm_lib_go_abi = $(call wasm_lib_deps,go-abi)
wasm_lib_forward = $(call wasm_lib_deps,forward)
wasm_lib_user_host_trait = $(call wasm_lib_deps,user-host-trait)
wasm_lib_user_host = $(call wasm_lib_deps,user-host) $(wasm_lib_user_host_trait)

forward_dir = $(wasm_lib)/forward

stylus_files = $(wildcard $(stylus_dir)/*.toml $(stylus_dir)/src/*.rs) $(wasm_lib_user_host_trait) $(rust_prover_files)

jit_dir = crates/jit
jit_files = $(wildcard $(jit_dir)/*.toml $(jit_dir)/*.rs $(jit_dir)/src/*.rs $(jit_dir)/src/*/*.rs) $(stylus_files)

validation_crate_dir = crates/validation
validation_files = $(wildcard $(validation_crate_dir)/*.toml $(validation_crate_dir)/src/*.rs $(validation_crate_dir)/src/*/*.rs) $(rust_arbutil_files)

validator_dir = crates/validator
validator_files = $(wildcard $(validator_dir)/*.toml $(validator_dir)/src/*.rs $(validator_dir)/src/*/*.rs) $(jit_files) $(validation_files)

wasm32_wasi = target/wasm32-wasip1/release
wasm32_unknown = target/wasm32-unknown-unknown/release

stylus_dir = crates/stylus
stylus_test_dir = crates/stylus/tests
stylus_cargo = crates/stylus/tests/.cargo/config.toml

rust_sdk = crates/langs/rust
c_sdk = crates/langs/c
stylus_lang_rust = $(wildcard $(rust_sdk)/*/src/*.rs $(rust_sdk)/*/src/*/*.rs $(rust_sdk)/*/*.toml)
stylus_lang_c    = $(wildcard $(c_sdk)/*/*.c $(c_sdk)/*/*.h)
stylus_lang_bf   = $(wildcard crates/langs/bf/src/*.* crates/langs/bf/src/*.toml)

get_stylus_test_wasm = $(stylus_test_dir)/$(wasm32_unknown)/$(1).wasm
get_stylus_test_rust = $(wildcard $(stylus_test_dir)/$(1)/*.toml $(stylus_test_dir)/$(1)/src/*.rs) $(stylus_cargo) $(stylus_lang_rust)
get_stylus_test_c    = $(wildcard $(c_sdk)/examples/$(1)/*.c $(c_sdk)/examples/$(1)/*.h) $(stylus_lang_c)
stylus_test_bfs      = $(wildcard $(stylus_test_dir)/bf/*.b)

stylus_test_keccak_wasm           = $(call get_stylus_test_wasm,keccak)
stylus_test_keccak_src            = $(call get_stylus_test_rust,keccak)
stylus_test_keccak-100_wasm       = $(call get_stylus_test_wasm,keccak-100)
stylus_test_keccak-100_src        = $(call get_stylus_test_rust,keccak-100)
stylus_test_fallible_wasm         = $(call get_stylus_test_wasm,fallible)
stylus_test_fallible_src          = $(call get_stylus_test_rust,fallible)
stylus_test_storage_wasm          = $(call get_stylus_test_wasm,storage)
stylus_test_storage_src           = $(call get_stylus_test_rust,storage)
stylus_test_multicall_wasm        = $(call get_stylus_test_wasm,multicall)
stylus_test_multicall_src         = $(call get_stylus_test_rust,multicall)
stylus_test_log_wasm              = $(call get_stylus_test_wasm,log)
stylus_test_log_src               = $(call get_stylus_test_rust,log)
stylus_test_create_wasm           = $(call get_stylus_test_wasm,create)
stylus_test_create_src            = $(call get_stylus_test_rust,create)
stylus_test_math_wasm             = $(call get_stylus_test_wasm,math)
stylus_test_math_src              = $(call get_stylus_test_rust,math)
stylus_test_evm-data_wasm         = $(call get_stylus_test_wasm,evm-data)
stylus_test_evm-data_src          = $(call get_stylus_test_rust,evm-data)
stylus_test_sdk-storage_wasm      = $(call get_stylus_test_wasm,sdk-storage)
stylus_test_sdk-storage_src       = $(call get_stylus_test_rust,sdk-storage)
stylus_test_erc20_wasm            = $(call get_stylus_test_wasm,erc20)
stylus_test_erc20_src             = $(call get_stylus_test_rust,erc20)
stylus_test_read-return-data_wasm = $(call get_stylus_test_wasm,read-return-data)
stylus_test_read-return-data_src  = $(call get_stylus_test_rust,read-return-data)
stylus_test_hostio-test_wasm      = $(call get_stylus_test_wasm,hostio-test)
stylus_test_hostio-test_src       = $(call get_stylus_test_rust,hostio-test)

stylus_test_wasms = $(stylus_test_keccak_wasm) $(stylus_test_keccak-100_wasm) $(stylus_test_fallible_wasm) $(stylus_test_storage_wasm) $(stylus_test_multicall_wasm) $(stylus_test_log_wasm) $(stylus_test_create_wasm) $(stylus_test_math_wasm) $(stylus_test_sdk-storage_wasm) $(stylus_test_erc20_wasm) $(stylus_test_read-return-data_wasm) $(stylus_test_evm-data_wasm) $(stylus_test_hostio-test_wasm) $(stylus_test_bfs:.b=.wasm)
stylus_benchmarks = $(wildcard $(stylus_dir)/*.toml $(stylus_dir)/src/*.rs) $(stylus_test_wasms)

CBROTLI_WASM_BUILD_ARGS ?=-d

# ============================================================================
# Development Targets (Comparison Execution Mode)
# ============================================================================

# Data directory (persistent, in repo)
DATA_DIR := $(CURDIR)/.data

# Database paths (default, overridden by network-specific targets)
NITRO_EL_DB ?= $(DATA_DIR)/nitro/default/el
NITRO_CL_DB ?= $(DATA_DIR)/nitro/default/cl
JWT_SECRET ?= $(HOME)/.arbitrum/jwt.hex

# Snapshot cache (shared across projects)
SNAPSHOT_CACHE_DIR := $(HOME)/.cache/blockchain-snapshots/nitro
MAINNET_GENESIS_SNAPSHOT := nitro-genesis-pebble.tar
MAINNET_GENESIS_SNAPSHOT_URL := https://snapshot.arbitrum.foundation/arb1/$(MAINNET_GENESIS_SNAPSHOT)

# Network defaults (can be overridden)
NETWORK ?= sepolia

# Sepolia configuration
ifeq ($(NETWORK),sepolia)
  CHAIN_ID := 421614
  PARENT_CHAIN_ID := 11155111
  L1_RPC ?= wss://sepolia.drpc.org
  L1_BEACON ?= https://ethereum-sepolia-beacon-api.publicnode.com
  GENESIS_HASH := 0x77194da4010e549a7028a9c3c51c3e277823be6ac7d138d0bb8a70197b5c004c
endif

# Mainnet configuration
ifeq ($(NETWORK),mainnet)
  CHAIN_ID := 42161
  PARENT_CHAIN_ID := 1
  L1_RPC ?= wss://ethereum.drpc.org
  L1_BEACON ?= https://ethereum-beacon-api.publicnode.com
  GENESIS_HASH := 0x7ee576b35482195fc49205cec9af72ce14f003b9ae69f6ba0faef4514be8b442
endif

.PHONY: clean-el-db
clean-el-db: ## Clean Nitro EL database
	@echo "Cleaning Nitro EL database at $(NITRO_EL_DB)..."
	@rm -rf $(NITRO_EL_DB)/*
	@mkdir -p $(NITRO_EL_DB)

.PHONY: clean-cl-db
clean-cl-db: ## Clean Nitro CL database
	@echo "Cleaning Nitro CL database at $(NITRO_CL_DB)..."
	@rm -rf $(NITRO_CL_DB)/*
	@mkdir -p $(NITRO_CL_DB)

.PHONY: clean-dbs
clean-dbs: clean-el-db clean-cl-db ## Clean both EL and CL databases
	@echo "All databases cleaned"

.PHONY: init-el
init-el: $(output_root)/bin/nitro ## Initialize EL with genesis from L1 (then quit)
	@echo "Initializing Nitro EL for $(NETWORK) (Chain ID: $(CHAIN_ID))..."
	@echo "  L1 RPC: $(L1_RPC)"
	@echo "  Expected genesis: $(GENESIS_HASH)"
	@rm -rf $(NITRO_EL_DB)/*
	@mkdir -p $(NITRO_EL_DB)
	$(output_root)/bin/nitro \
		--chain.id=$(CHAIN_ID) \
		--parent-chain.id=$(PARENT_CHAIN_ID) \
		--persistent.global-config=$(NITRO_EL_DB) \
		--parent-chain.connection.url=$(L1_RPC) \
		--parent-chain.blob-client.beacon-url=$(L1_BEACON) \
		--init.latest=genesis \
		--init.then-quit=true \
		--init.validate-genesis-assertion=false \
		--node.sequencer=false \
		--node.batch-poster.enable=false \
		--node.staker.enable=false \
		--node.feed.input.url="" \
		--auth.jwtsecret=$(JWT_SECRET)

.PHONY: run-el
run-el: $(output_root)/bin/nitro ## Run EL in execution-only mode (waits for CL)
	@echo "Starting Nitro EL in execution-only mode for $(NETWORK)..."
	@echo "  Chain ID: $(CHAIN_ID)"
	@echo "  WebSocket: ws://localhost:20552"
	@echo "  HTTP: http://localhost:8547"
	$(output_root)/bin/nitro \
		--chain.id=$(CHAIN_ID) \
		--parent-chain.id=$(PARENT_CHAIN_ID) \
		--persistent.global-config=$(NITRO_EL_DB) \
		--init.empty=true \
		--init.validate-genesis-assertion=false \
		--node.dangerous.no-l1-listener=true \
		--node.parent-chain-reader.enable=false \
		--node.sequencer=false \
		--node.batch-poster.enable=false \
		--node.staker.enable=false \
		--node.feed.input.url="" \
		--execution.rpc-server.enable=true \
		--execution.rpc-server.authenticated=false \
		--execution.rpc-server.public=true \
		--auth.jwtsecret=$(JWT_SECRET) \
		--auth.addr=0.0.0.0 \
		--auth.port=8551 \
		--ws.addr=0.0.0.0 \
		--ws.port=20552 \
		--ws.api=net,web3,eth,arb,nitroexecution \
		--http.addr=0.0.0.0 \
		--http.port=8547

.PHONY: run-cl
run-cl: $(output_root)/bin/nitro ## Run CL connecting to EL at ws://localhost:20552
	@echo "Starting Nitro CL for $(NETWORK)..."
	@echo "  Chain ID: $(CHAIN_ID)"
	@echo "  EL URL: ws://localhost:20552"
	@rm -rf $(NITRO_CL_DB)/*
	@mkdir -p $(NITRO_CL_DB)
	$(output_root)/bin/nitro \
		--chain.id=$(CHAIN_ID) \
		--parent-chain.id=$(PARENT_CHAIN_ID) \
		--persistent.global-config=$(NITRO_CL_DB) \
		--parent-chain.connection.url=$(L1_RPC) \
		--parent-chain.blob-client.beacon-url=$(L1_BEACON) \
		--node.execution-rpc-client.url=ws://localhost:20552 \
		--init.empty=true \
		--init.validate-genesis-assertion=false \
		--node.sequencer=false \
		--node.batch-poster.enable=false \
		--node.staker.enable=false \
		--node.feed.input.url="" \
		--auth.jwtsecret=$(JWT_SECRET) \
		--ws.addr=0.0.0.0 \
		--ws.port=8559 \
		--http.addr=0.0.0.0 \
		--http.port=8558

.PHONY: run-cl-comparison
run-cl-comparison: $(output_root)/bin/nitro ## Run CL in comparison mode (Nitro EL + Nethermind EL)
	@echo "Starting Nitro CL in COMPARISON MODE for $(NETWORK)..."
	@echo "  Chain ID: $(CHAIN_ID)"
	@echo "  Primary EL (Nitro): ws://localhost:20552"
	@echo "  Secondary EL (Nethermind): http://localhost:20551"
	@rm -rf $(NITRO_CL_DB)/*
	@mkdir -p $(NITRO_CL_DB)
	$(output_root)/bin/nitro \
		--chain.id=$(CHAIN_ID) \
		--parent-chain.id=$(PARENT_CHAIN_ID) \
		--persistent.global-config=$(NITRO_CL_DB) \
		--parent-chain.connection.url=$(L1_RPC) \
		--parent-chain.blob-client.beacon-url=$(L1_BEACON) \
		--node.execution-rpc-client.url=ws://localhost:20552 \
		--node.comparison-execution.enable=true \
		--node.comparison-execution.secondary-rpc-client.url=http://localhost:20551 \
		--node.comparison-execution.secondary-rpc-client.jwtsecret=$(JWT_SECRET) \
		--init.empty=true \
		--init.validate-genesis-assertion=false \
		--node.sequencer=false \
		--node.batch-poster.enable=false \
		--node.staker.enable=false \
		--node.feed.input.url="" \
		--auth.jwtsecret=$(JWT_SECRET) \
		--ws.addr=0.0.0.0 \
		--ws.port=8559 \
		--http.addr=0.0.0.0 \
		--http.port=8558

.PHONY: dev-help
dev-help: ## Show development targets help
	@echo "Nitro Development Targets"
	@echo ""
	@echo "Usage: make <target> or make <target>-sepolia / <target>-mainnet"
	@echo ""
	@echo "Database Management:"
	@echo "  clean-el-db[-sepolia|-mainnet]   Clean EL database"
	@echo "  clean-cl-db[-sepolia|-mainnet]   Clean CL database"
	@echo "  clean-dbs[-sepolia|-mainnet]     Clean both databases"
	@echo ""
	@echo "Initialization:"
	@echo "  init-el[-sepolia|-mainnet]       Initialize EL (mainnet uses cache if available)"
	@echo ""
	@echo "Snapshot Cache (~/.cache/blockchain-snapshots/nitro):"
	@echo "  download-snapshot-mainnet        Download mainnet genesis snapshot (~32GB)"
	@echo "  snapshot-cache-status            Show cache status"
	@echo "  clean-snapshot-cache             Remove cached snapshots"
	@echo ""
	@echo "Running:"
	@echo "  run-el[-sepolia|-mainnet]        Run EL in execution-only mode (port 20552)"
	@echo "  run-cl[-sepolia|-mainnet]        Run CL connecting to EL"
	@echo "  run-cl-comparison[-sepolia|-mainnet]  Compare Nitro EL vs Nethermind EL"
	@echo ""
	@echo "Quick Start (Comparison Mode - Sepolia):"
	@echo "  Terminal 1: make clean-dbs-sepolia && make init-el-sepolia && make run-el-sepolia"
	@echo "  Terminal 2: (in nethermind-arbitrum) make run-sepolia"
	@echo "  Terminal 3: make run-cl-comparison-sepolia"
	@echo ""
	@echo "Quick Start (Comparison Mode - Mainnet):"
	@echo "  First time: make download-snapshot-mainnet  # ~32GB, one-time download (optional)"
	@echo "  Terminal 1: make clean-dbs-mainnet && make init-el-mainnet && make run-el-mainnet"
	@echo "  Terminal 2: (in nethermind-arbitrum) make run-mainnet"
	@echo "  Terminal 3: make run-cl-comparison-mainnet"

# ============================================================================
# Explicit Sepolia Targets
# ============================================================================

.PHONY: clean-el-db-sepolia
clean-el-db-sepolia: ## Clean Sepolia EL database
	@$(MAKE) clean-el-db NETWORK=sepolia NITRO_EL_DB=$(DATA_DIR)/nitro/sepolia/el

.PHONY: clean-cl-db-sepolia
clean-cl-db-sepolia: ## Clean Sepolia CL database
	@$(MAKE) clean-cl-db NETWORK=sepolia NITRO_CL_DB=$(DATA_DIR)/nitro/sepolia/cl

.PHONY: clean-dbs-sepolia
clean-dbs-sepolia: ## Clean both Sepolia databases
	@$(MAKE) clean-dbs NETWORK=sepolia NITRO_EL_DB=$(DATA_DIR)/nitro/sepolia/el NITRO_CL_DB=$(DATA_DIR)/nitro/sepolia/cl

.PHONY: init-el-sepolia
init-el-sepolia: $(output_root)/bin/nitro ## Initialize Sepolia EL with genesis from L1
	@$(MAKE) init-el NETWORK=sepolia NITRO_EL_DB=$(DATA_DIR)/nitro/sepolia/el

.PHONY: run-el-sepolia
run-el-sepolia: $(output_root)/bin/nitro ## Run Sepolia EL in execution-only mode
	@$(MAKE) run-el NETWORK=sepolia NITRO_EL_DB=$(DATA_DIR)/nitro/sepolia/el

.PHONY: run-cl-sepolia
run-cl-sepolia: $(output_root)/bin/nitro ## Run Sepolia CL connecting to EL
	@$(MAKE) run-cl NETWORK=sepolia NITRO_CL_DB=$(DATA_DIR)/nitro/sepolia/cl

.PHONY: run-cl-comparison-sepolia
run-cl-comparison-sepolia: $(output_root)/bin/nitro ## Run Sepolia CL in comparison mode
	@$(MAKE) run-cl-comparison NETWORK=sepolia NITRO_CL_DB=$(DATA_DIR)/nitro/sepolia/cl

# ============================================================================
# Explicit Mainnet Targets
# ============================================================================

.PHONY: clean-el-db-mainnet
clean-el-db-mainnet: ## Clean Mainnet EL database
	@$(MAKE) clean-el-db NETWORK=mainnet NITRO_EL_DB=$(DATA_DIR)/nitro/mainnet/el

.PHONY: clean-cl-db-mainnet
clean-cl-db-mainnet: ## Clean Mainnet CL database
	@$(MAKE) clean-cl-db NETWORK=mainnet NITRO_CL_DB=$(DATA_DIR)/nitro/mainnet/cl

.PHONY: clean-dbs-mainnet
clean-dbs-mainnet: ## Clean both Mainnet databases
	@$(MAKE) clean-dbs NETWORK=mainnet NITRO_EL_DB=$(DATA_DIR)/nitro/mainnet/el NITRO_CL_DB=$(DATA_DIR)/nitro/mainnet/cl

.PHONY: init-el-mainnet
init-el-mainnet: $(output_root)/bin/nitro ## Initialize Mainnet EL (uses cache if available)
	@if [ -d "$(DATA_DIR)/nitro/mainnet/el/arb1/nitro/l2chaindata" ]; then \
		echo "Mainnet EL database already exists at $(DATA_DIR)/nitro/mainnet/el"; \
		echo "Run 'make clean-el-db-mainnet' first to reinitialize."; \
	elif [ -f "$(SNAPSHOT_CACHE_DIR)/$(MAINNET_GENESIS_SNAPSHOT)" ]; then \
		echo "Restoring mainnet genesis from cached snapshot..."; \
		mkdir -p "$(DATA_DIR)/nitro/mainnet/el/arb1/nitro"; \
		tar -xf "$(SNAPSHOT_CACHE_DIR)/$(MAINNET_GENESIS_SNAPSHOT)" -C "$(DATA_DIR)/nitro/mainnet/el/arb1/nitro"; \
		echo "Mainnet EL initialized from cache."; \
	else \
		echo "No cached snapshot. Downloading from L1 (run 'make download-snapshot-mainnet' to cache for future use)..."; \
		$(MAKE) init-el NETWORK=mainnet NITRO_EL_DB=$(DATA_DIR)/nitro/mainnet/el; \
	fi

.PHONY: run-el-mainnet
run-el-mainnet: $(output_root)/bin/nitro ## Run Mainnet EL in execution-only mode
	@$(MAKE) run-el NETWORK=mainnet NITRO_EL_DB=$(DATA_DIR)/nitro/mainnet/el

.PHONY: run-cl-mainnet
run-cl-mainnet: $(output_root)/bin/nitro ## Run Mainnet CL connecting to EL
	@$(MAKE) run-cl NETWORK=mainnet NITRO_CL_DB=$(DATA_DIR)/nitro/mainnet/cl

.PHONY: run-cl-comparison-mainnet
run-cl-comparison-mainnet: $(output_root)/bin/nitro ## Run Mainnet CL in comparison mode
	@$(MAKE) run-cl-comparison NETWORK=mainnet NITRO_CL_DB=$(DATA_DIR)/nitro/mainnet/cl

# ============================================================================
# Snapshot Cache Management
# ============================================================================

.PHONY: download-snapshot-mainnet
download-snapshot-mainnet: ## Download mainnet genesis snapshot to cache (~32GB)
	@echo "Downloading mainnet genesis snapshot to cache..."
	@echo "  URL: $(MAINNET_GENESIS_SNAPSHOT_URL)"
	@echo "  Size: ~32GB - this may take a while"
	@mkdir -p "$(SNAPSHOT_CACHE_DIR)"
	@if command -v aria2c > /dev/null 2>&1; then \
		echo "Using aria2c (16 connections for faster download)..."; \
		aria2c -x 16 -s 16 -k 1M -c \
			-d "$(SNAPSHOT_CACHE_DIR)" -o "$(MAINNET_GENESIS_SNAPSHOT)" "$(MAINNET_GENESIS_SNAPSHOT_URL)"; \
	else \
		echo "Using curl (install aria2 for faster downloads: brew install aria2)..."; \
		curl -L -C - --retry 5 --retry-delay 5 --progress-bar \
			-o "$(SNAPSHOT_CACHE_DIR)/$(MAINNET_GENESIS_SNAPSHOT)" "$(MAINNET_GENESIS_SNAPSHOT_URL)"; \
	fi
	@echo "Verifying download..."
	@if tar -tf "$(SNAPSHOT_CACHE_DIR)/$(MAINNET_GENESIS_SNAPSHOT)" > /dev/null 2>&1; then \
		echo "Snapshot downloaded and cached successfully at $(SNAPSHOT_CACHE_DIR)/$(MAINNET_GENESIS_SNAPSHOT)"; \
		ls -lh "$(SNAPSHOT_CACHE_DIR)/$(MAINNET_GENESIS_SNAPSHOT)"; \
	else \
		echo "ERROR: Download corrupted. Removing and try again."; \
		rm -f "$(SNAPSHOT_CACHE_DIR)/$(MAINNET_GENESIS_SNAPSHOT)"; \
		exit 1; \
	fi

.PHONY: snapshot-cache-status
snapshot-cache-status: ## Show snapshot cache status
	@echo "=== Nitro Snapshot Cache Status ==="
	@echo "Cache directory: $(SNAPSHOT_CACHE_DIR)"
	@echo ""
	@if [ -f "$(SNAPSHOT_CACHE_DIR)/$(MAINNET_GENESIS_SNAPSHOT)" ]; then \
		echo "Mainnet Genesis: CACHED"; \
		ls -lh "$(SNAPSHOT_CACHE_DIR)/$(MAINNET_GENESIS_SNAPSHOT)"; \
	else \
		echo "Mainnet Genesis: NOT CACHED"; \
		echo "  Run 'make download-snapshot-mainnet' to download (~32GB)"; \
	fi
	@echo ""
	@echo "Note: Sepolia does not have a genesis snapshot (syncs from L1)"

.PHONY: clean-snapshot-cache
clean-snapshot-cache: ## Remove snapshot cache
	@rm -rf "$(SNAPSHOT_CACHE_DIR)"
	@echo "Snapshot cache cleared."

# ============================================================================
# user targets

.PHONY: push
push: lint test-go .make/fmt
	@printf "%bdone building %s%b\n" $(color_pink) $$(expr $$(echo $? | wc -w) - 1) $(color_reset)
	@printf "%bready for push!%b\n" $(color_pink) $(color_reset)

.PHONY: all
all: build build-replay-env test-gen-proofs
	@touch .make/all

.PHONY: build
build: $(patsubst %,$(output_root)/bin/%, nitro deploy relay daprovider anytrustserver autonomous-auctioneer bidder-client anytrusttool blobtool el-proxy mockexternalsigner seq-coordinator-invalidate nitro-val seq-coordinator-manager dbconv genesis-generator transaction-filterer filtering-report)
	@printf $(done)

.PHONY: build-node-deps
build-node-deps: $(go_source) build-prover-header build-prover-lib build-jit .make/solgen .make/cbrotli-lib

.PHONY: test-go-deps
test-go-deps: \
	build-replay-env \
	build-validation-server \
	$(stylus_test_wasms) \
	$(arbitrator_stylus_lib) \
	$(arbitrator_generated_header) \
	$(patsubst %,$(arbitrator_cases)/%.wasm, global-state read-inboxmsg-10 global-state-wrapper const)

.PHONY: build-prover-header
build-prover-header: $(arbitrator_generated_header)

.PHONY: build-prover-lib
build-prover-lib: $(arbitrator_stylus_lib)

.PHONY: build-prover-bin
build-prover-bin: $(prover_bin)

.PHONY: build-jit
build-jit: $(arbitrator_jit)

.PHONY: build-validation-server
build-validation-server: $(validation_server)

.PHONY: build-replay-env
build-replay-env: $(prover_bin) $(arbitrator_jit) $(arbitrator_wasm_libs) $(replay_wasm) $(output_latest)/machine.v2.wavm.br

.PHONY: build-wasm-libs
build-wasm-libs: $(arbitrator_wasm_libs)

.PHONY: build-wasm-bin
build-wasm-bin: $(replay_wasm)

.PHONY: build-solidity
build-solidity: .make/solidity

.PHONY: contracts
contracts: .make/solgen
	@printf $(done)

.PHONY: format fmt
format fmt: .make/fmt
	@printf $(done)

.PHONY: lint
lint: .make/lint
	@printf $(done)

.PHONY: stylus-benchmarks
stylus-benchmarks: $(stylus_benchmarks)
	cargo test --manifest-path $< --release --features benchmark benchmark_ -- --nocapture
	@printf $(done)

.PHONY: test-go
test-go: .make/test-go
	@printf $(done)

.PHONY: test-go-challenge
test-go-challenge: test-go-deps
	.github/workflows/gotestsum.sh --timeout 120m --run TestChallenge --tags challengetest --nolog
	@printf $(done)

.PHONY: test-go-stylus
test-go-stylus: test-go-deps
	.github/workflows/gotestsum.sh --timeout 120m --run TestProgramArbitrator --tags stylustest --nolog
	@printf $(done)

.PHONY: test-go-redis
test-go-redis: test-go-deps
	.github/workflows/gotestsum.sh --timeout 120m --run TestRedis --nolog -- --test_redis=redis://localhost:6379/0
	@printf $(done)

.PHONY: test-gen-proofs
test-gen-proofs: \
        $(arbitrator_test_wasms) \
	$(patsubst $(arbitrator_cases)/%.wat,contracts/test/prover/proofs/%.json, $(arbitrator_tests_wat)) \
	$(patsubst $(arbitrator_cases)/rust/src/bin/%.rs,contracts/test/prover/proofs/rust-%.json, $(arbitrator_tests_rust)) \
	contracts/test/prover/proofs/go.json
	@printf $(done)

.PHONY: test-rust
test-rust: .make/test-rust
	@printf $(done)

# Runs the fastest and most reliable and high-value tests.
.PHONY: tests
tests: test-go test-rust
	@printf $(done)

# Runs all tests, including slow and unreliable tests.
#  Currently, NOT including:
#  - test-go-redis (These testts require additional setup and are not as reliable)
.PHONY: tests-all
tests-all: tests test-go-challenge test-go-stylus test-gen-proofs
	@printf $(done)

.PHONY: wasm-ci-build
wasm-ci-build: $(arbitrator_wasm_libs) $(arbitrator_test_wasms) $(stylus_test_wasms) $(output_latest)/user_test.wasm
	@printf $(done)

.PHONY: clean
clean:
	go clean -testcache
	rm -rf $(arbitrator_cases)/rust/target
	rm -f $(arbitrator_cases)/*.wasm $(arbitrator_cases)/go/testcase.wasm
	rm -rf crates/wasm-testsuite/tests
	rm -rf $(output_root)
	rm -f contracts/test/prover/proofs/*.json contracts/test/prover/spec-proofs/*.json
	rm -f contracts-legacy/test/prover/proofs/*.json contracts-legacy/test/prover/spec-proofs/*.json
	rm -f crates/wasm-libraries/soft-float/soft-float.wasm
	rm -f crates/wasm-libraries/soft-float/*.o
	rm -f crates/wasm-libraries/soft-float/SoftFloat/build/Wasm-Clang/*.o
	rm -f crates/wasm-libraries/soft-float/SoftFloat/build/Wasm-Clang/*.a
	rm -f crates/wasm-libraries/forward/*.wat
	rm -rf crates/stylus/tests/target/ crates/stylus/tests/*/*.wasm
	rm -rf brotli/buildfiles
	@rm -rf contracts/build contracts/cache solgen/go/
	@rm -rf contracts-legacy/build contracts-legacy/cache
	@rm -rf contracts-local/out contracts-local/forge-cache
	@rm -f .make/*

.PHONY: docker
docker:
	docker build -t nitro-node-slim --target nitro-node-slim .
	docker build -t nitro-node --target nitro-node .
	docker build -t nitro-node-dev --target nitro-node-dev .

.PHONY: check-license-headers
check-license-headers:
	@go run ./scripts/licenser.go

# regular build rules

$(output_root)/bin/nitro: $(DEP_PREDICATE) build-node-deps
	go build $(GOLANG_PARAMS) -o $@ "$(CURDIR)/cmd/nitro"

$(output_root)/bin/deploy: $(DEP_PREDICATE) build-node-deps
	go build $(GOLANG_PARAMS) -o $@ "$(CURDIR)/cmd/deploy"

$(output_root)/bin/relay: $(DEP_PREDICATE) build-node-deps
	go build $(GOLANG_PARAMS) -o $@ "$(CURDIR)/cmd/relay"

$(output_root)/bin/daprovider: $(DEP_PREDICATE) build-node-deps
	go build $(GOLANG_PARAMS) -o $@ "$(CURDIR)/cmd/daprovider"

$(output_root)/bin/anytrustserver: $(DEP_PREDICATE) build-node-deps
	go build $(GOLANG_PARAMS) -o $@ "$(CURDIR)/cmd/anytrustserver"

$(output_root)/bin/autonomous-auctioneer: $(DEP_PREDICATE) build-node-deps
	go build $(GOLANG_PARAMS) -o $@ "$(CURDIR)/cmd/autonomous-auctioneer"

$(output_root)/bin/bidder-client: $(DEP_PREDICATE) build-node-deps
	go build $(GOLANG_PARAMS) -o $@ "$(CURDIR)/cmd/bidder-client"

$(output_root)/bin/el-proxy: $(DEP_PREDICATE) build-node-deps
	go build $(GOLANG_PARAMS) -o $@ "$(CURDIR)/cmd/el-proxy"

$(output_root)/bin/anytrusttool: $(DEP_PREDICATE) build-node-deps
	go build $(GOLANG_PARAMS) -o $@ "$(CURDIR)/cmd/anytrusttool"

$(output_root)/bin/blobtool: $(DEP_PREDICATE) build-node-deps
	go build $(GOLANG_PARAMS) -o $@ "$(CURDIR)/cmd/blobtool"

$(output_root)/bin/genesis-generator: $(DEP_PREDICATE) build-node-deps
	go build $(GOLANG_PARAMS) -o $@ "$(CURDIR)/cmd/genesis-generator"

$(output_root)/bin/mockexternalsigner: $(DEP_PREDICATE) build-node-deps
	go build $(GOLANG_PARAMS) -o $@ "$(CURDIR)/cmd/mockexternalsigner"

$(output_root)/bin/seq-coordinator-invalidate: $(DEP_PREDICATE) build-node-deps
	go build $(GOLANG_PARAMS) -o $@ "$(CURDIR)/cmd/seq-coordinator-invalidate"

$(output_root)/bin/nitro-val: $(DEP_PREDICATE) build-node-deps
	go build $(GOLANG_PARAMS) -o $@ "$(CURDIR)/cmd/nitro-val"

$(output_root)/bin/seq-coordinator-manager: $(DEP_PREDICATE) build-node-deps
	go build $(GOLANG_PARAMS) -o $@ "$(CURDIR)/cmd/seq-coordinator-manager"

$(output_root)/bin/dbconv: $(DEP_PREDICATE) build-node-deps
	go build $(GOLANG_PARAMS) -o $@ "$(CURDIR)/cmd/dbconv"

$(output_root)/bin/transaction-filterer: $(DEP_PREDICATE) build-node-deps
	go build $(GOLANG_PARAMS) -o $@ "$(CURDIR)/cmd/transaction-filterer"

$(output_root)/bin/filtering-report: $(DEP_PREDICATE) build-node-deps
	go build $(GOLANG_PARAMS) -o $@ "$(CURDIR)/cmd/filtering-report"

# recompile wasm, but don't change timestamp unless files differ
$(replay_wasm): $(DEP_PREDICATE) $(go_source) .make/solgen
	mkdir -p `dirname $(replay_wasm)`
	GOOS=wasip1 GOARCH=wasm go build -o $@ ./cmd/replay/...
	./scripts/remove_reference_types.sh $@

$(prover_bin): $(DEP_PREDICATE) $(rust_prover_files)
	mkdir -p `dirname $(prover_bin)`
	cargo build --release --bin prover ${CARGOFLAGS}
	install target/release/prover $@

$(arbitrator_stylus_lib): $(DEP_PREDICATE) $(stylus_files)
	mkdir -p `dirname $(arbitrator_stylus_lib)`
	cargo build --release --lib -p stylus ${CARGOFLAGS}
	install target/release/libstylus.a $@

$(arbitrator_jit): $(DEP_PREDICATE) $(jit_files)
	mkdir -p `dirname $(arbitrator_jit)`
	cargo build --release -p jit ${CARGOFLAGS}
	install target/release/jit $@

$(validation_server): $(DEP_PREDICATE) $(validator_files)
	mkdir -p `dirname $(validation_server)`
	cargo build --release -p validator ${CARGOFLAGS}
	install target/release/validator $@

$(arbitrator_cases)/rust/$(wasm32_wasi)/%.wasm: $(arbitrator_cases)/rust/src/bin/%.rs $(arbitrator_cases)/rust/src/lib.rs $(arbitrator_cases)/rust/.cargo/config.toml
	cargo build --manifest-path $(arbitrator_cases)/rust/Cargo.toml --release --target wasm32-wasip1 --config $(arbitrator_cases)/rust/.cargo/config.toml --bin $(patsubst $(arbitrator_cases)/rust/$(wasm32_wasi)/%.wasm,%, $@)
	./scripts/remove_reference_types.sh $@

$(arbitrator_cases)/go/testcase.wasm: $(arbitrator_cases)/go/*.go .make/solgen
	cd $(arbitrator_cases)/go && GOOS=wasip1 GOARCH=wasm go build -o testcase.wasm

$(arbitrator_generated_header): $(DEP_PREDICATE) $(stylus_files)
	@echo creating ${PWD}/$(arbitrator_generated_header)
	mkdir -p `dirname $(arbitrator_generated_header)`
	cd crates/stylus && cbindgen --config cbindgen.toml --crate stylus --output ../../$(arbitrator_generated_header)
	@touch -c $@ # cargo might decide to not rebuild the header

$(output_latest)/wasi_stub.wasm: $(DEP_PREDICATE) $(call wasm_lib_deps,wasi-stub)
	cargo build --release --target wasm32-unknown-unknown --config $(wasm_lib_cargo) --package wasi-stub
	install $(wasm32_unknown)/wasi_stub.wasm $@
	./scripts/remove_reference_types.sh $@

crates/wasm-libraries/soft-float/SoftFloat/build/Wasm-Clang/softfloat.a: $(DEP_PREDICATE) \
		crates/wasm-libraries/soft-float/SoftFloat/build/Wasm-Clang/Makefile \
		crates/wasm-libraries/soft-float/SoftFloat/build/Wasm-Clang/platform.h \
		crates/wasm-libraries/soft-float/SoftFloat/source/*.c \
		crates/wasm-libraries/soft-float/SoftFloat/source/include/*.h \
		crates/wasm-libraries/soft-float/SoftFloat/source/8086/*.c \
		crates/wasm-libraries/soft-float/SoftFloat/source/8086/*.h
	cd crates/wasm-libraries/soft-float/SoftFloat/build/Wasm-Clang && make $(MAKEFLAGS)

crates/wasm-libraries/soft-float/bindings32.o: $(DEP_PREDICATE) crates/wasm-libraries/soft-float/bindings32.c
	clang crates/wasm-libraries/soft-float/bindings32.c --sysroot $(WASI_SYSROOT) -I crates/wasm-libraries/soft-float/SoftFloat/source/include -target wasm32-wasip1 -Wconversion -c -o $@

crates/wasm-libraries/soft-float/bindings64.o: $(DEP_PREDICATE) crates/wasm-libraries/soft-float/bindings64.c
	clang crates/wasm-libraries/soft-float/bindings64.c --sysroot $(WASI_SYSROOT) -I crates/wasm-libraries/soft-float/SoftFloat/source/include -target wasm32-wasip1 -Wconversion -c -o $@

$(output_latest)/soft-float.wasm: $(DEP_PREDICATE) \
		crates/wasm-libraries/soft-float/bindings32.o \
		crates/wasm-libraries/soft-float/bindings64.o \
		crates/wasm-libraries/soft-float/SoftFloat/build/Wasm-Clang/softfloat.a \
		.make/wasm-lib .make/machines
	wasm-ld \
		crates/wasm-libraries/soft-float/bindings32.o \
		crates/wasm-libraries/soft-float/bindings64.o \
		crates/wasm-libraries/soft-float/SoftFloat/build/Wasm-Clang/*.o \
		--no-entry -o $@ \
		$(patsubst %,--export wavm__f32_%, abs neg ceil floor trunc nearest sqrt add sub mul div min max) \
		$(patsubst %,--export wavm__f32_%, copysign eq ne lt le gt ge) \
		$(patsubst %,--export wavm__f64_%, abs neg ceil floor trunc nearest sqrt add sub mul div min max) \
		$(patsubst %,--export wavm__f64_%, copysign eq ne lt le gt ge) \
		$(patsubst %,--export wavm__i32_trunc_%,     f32_s f32_u f64_s f64_u) \
		$(patsubst %,--export wavm__i32_trunc_sat_%, f32_s f32_u f64_s f64_u) \
		$(patsubst %,--export wavm__i64_trunc_%,     f32_s f32_u f64_s f64_u) \
		$(patsubst %,--export wavm__i64_trunc_sat_%, f32_s f32_u f64_s f64_u) \
		$(patsubst %,--export wavm__f32_convert_%, i32_s i32_u i64_s i64_u) \
		$(patsubst %,--export wavm__f64_convert_%, i32_s i32_u i64_s i64_u) \
		--export wavm__f32_demote_f64 \
		--export wavm__f64_promote_f32

$(output_latest)/host_io.wasm: $(DEP_PREDICATE) $(call wasm_lib_deps,host-io) $(wasm_lib_go_abi)
	cargo build --release --target wasm32-wasip1 --config $(wasm_lib_cargo) --package host-io
	install $(wasm32_wasi)/host_io.wasm $@
	./scripts/remove_reference_types.sh $@

$(output_latest)/user_host.wasm: $(DEP_PREDICATE) $(wasm_lib_user_host) $(rust_prover_files) $(output_latest)/forward_stub.wasm .make/machines
	cargo build --release --target wasm32-wasip1 --config $(wasm_lib_cargo) --package user-host
	install $(wasm32_wasi)/user_host.wasm $@
	./scripts/remove_reference_types.sh $@

$(output_latest)/program_exec.wasm: $(DEP_PREDICATE) $(call wasm_lib_deps,program-exec) $(rust_prover_files) .make/machines
	cargo build --release --target wasm32-wasip1 --config $(wasm_lib_cargo) --package program-exec
	install $(wasm32_wasi)/program_exec.wasm $@
	./scripts/remove_reference_types.sh $@

$(output_latest)/user_test.wasm: $(DEP_PREDICATE) $(call wasm_lib_deps,user-test) $(rust_prover_files) .make/machines
	cargo build --release --target wasm32-wasip1 --config $(wasm_lib_cargo) --package user-test
	install $(wasm32_wasi)/user_test.wasm $@
	./scripts/remove_reference_types.sh $@

$(output_latest)/arbcompress.wasm: $(DEP_PREDICATE) $(call wasm_lib_deps,brotli) $(wasm_lib_go_abi)
	cargo build --release --target wasm32-wasip1 --config $(wasm_lib_cargo) --package arbcompress
	install $(wasm32_wasi)/arbcompress.wasm $@
	./scripts/remove_reference_types.sh $@

$(output_latest)/arbcrypto.wasm: $(DEP_PREDICATE) $(call wasm_lib_deps) $(wasm_lib_go_abi)
	cargo build --release --target wasm32-wasip1 --config $(wasm_lib_cargo) --package arbcrypto
	install $(wasm32_wasi)/arbcrypto.wasm $@
	./scripts/remove_reference_types.sh $@

$(output_latest)/forward.wasm: $(DEP_PREDICATE) $(wasm_lib_forward) .make/machines
	cargo run --release --package forward -- --path $(forward_dir)/forward.wat
	wat2wasm $(wasm_lib)/forward/forward.wat -o $@

$(output_latest)/forward_stub.wasm: $(DEP_PREDICATE) $(wasm_lib_forward) .make/machines
	cargo run --release --package forward -- --path $(forward_dir)/forward_stub.wat --stub
	wat2wasm $(wasm_lib)/forward/forward_stub.wat -o $@

$(output_latest)/machine.v2.wavm.br: $(DEP_PREDICATE) $(prover_bin) $(arbitrator_wasm_libs) $(replay_wasm)
	$(prover_bin) $(replay_wasm) --generate-binaries $(output_latest) \
	$(patsubst %,-l $(output_latest)/%.wasm, forward soft-float wasi_stub host_io user_host arbcompress arbcrypto program_exec)

$(arbitrator_cases)/%.wasm: $(arbitrator_cases)/%.wat
	wat2wasm $< -o $@

$(stylus_test_dir)/%.wasm: $(stylus_test_dir)/%.b $(stylus_lang_bf)
	cargo run --manifest-path crates/langs/bf/Cargo.toml $< -o $@

$(stylus_test_keccak_wasm): $(stylus_test_keccak_src)
	cargo build --manifest-path $< --release --config $(stylus_cargo)
	./scripts/remove_reference_types.sh $@
	@touch -c $@ # cargo might decide to not rebuild the binary

$(stylus_test_keccak-100_wasm): $(stylus_test_keccak-100_src)
	cargo build --manifest-path $< --release --config $(stylus_cargo)
	./scripts/remove_reference_types.sh $@
	@touch -c $@ # cargo might decide to not rebuild the binary

$(stylus_test_fallible_wasm): $(stylus_test_fallible_src)
	cargo build --manifest-path $< --release --config $(stylus_cargo)
	./scripts/remove_reference_types.sh $@
	@touch -c $@ # cargo might decide to not rebuild the binary

$(stylus_test_storage_wasm): $(stylus_test_storage_src)
	cargo build --manifest-path $< --release --config $(stylus_cargo)
	./scripts/remove_reference_types.sh $@
	@touch -c $@ # cargo might decide to not rebuild the binary

$(stylus_test_multicall_wasm): $(stylus_test_multicall_src)
	cargo build --manifest-path $< --release --config $(stylus_cargo)
	./scripts/remove_reference_types.sh $@
	@touch -c $@ # cargo might decide to not rebuild the binary

$(stylus_test_log_wasm): $(stylus_test_log_src)
	cargo build --manifest-path $< --release --config $(stylus_cargo)
	./scripts/remove_reference_types.sh $@
	@touch -c $@ # cargo might decide to not rebuild the binary

$(stylus_test_create_wasm): $(stylus_test_create_src)
	cargo build --manifest-path $< --release --config $(stylus_cargo)
	./scripts/remove_reference_types.sh $@
	@touch -c $@ # cargo might decide to not rebuild the binary

$(stylus_test_math_wasm): $(stylus_test_math_src)
	cargo build --manifest-path $< --release --config $(stylus_cargo)
	./scripts/remove_reference_types.sh $@
	@touch -c $@ # cargo might decide to not rebuild the binary

$(stylus_test_evm-data_wasm): $(stylus_test_evm-data_src)
	cargo build --manifest-path $< --release --config $(stylus_cargo)
	./scripts/remove_reference_types.sh $@
	@touch -c $@ # cargo might decide to not rebuild the binary

$(stylus_test_read-return-data_wasm): $(stylus_test_read-return-data_src)
	cargo build --manifest-path $< --release --config $(stylus_cargo)
	./scripts/remove_reference_types.sh $@
	@touch -c $@ # cargo might decide to not rebuild the binary

$(stylus_test_sdk-storage_wasm): $(stylus_test_sdk-storage_src)
	cargo build --manifest-path $< --release --config $(stylus_cargo)
	./scripts/remove_reference_types.sh $@
	@touch -c $@ # cargo might decide to not rebuild the binary

$(stylus_test_erc20_wasm): $(stylus_test_erc20_src)
	cargo build --manifest-path $< --release --config $(stylus_cargo)
	./scripts/remove_reference_types.sh $@
	@touch -c $@ # cargo might decide to not rebuild the binary

$(stylus_test_hostio-test_wasm): $(stylus_test_hostio-test_src)
	cargo build --manifest-path $< --release --config $(stylus_cargo)
	./scripts/remove_reference_types.sh $@
	@touch -c $@ # cargo might decide to not rebuild the binary

contracts/test/prover/proofs/float%.json: $(arbitrator_cases)/float%.wasm $(prover_bin) $(output_latest)/soft-float.wasm
	$(prover_bin) $< -l $(output_latest)/soft-float.wasm -o $@ -b --allow-hostapi --require-success

contracts/test/prover/proofs/no-stack-pollution.json: $(arbitrator_cases)/no-stack-pollution.wasm $(prover_bin)
	$(prover_bin) $< -o $@ --allow-hostapi --require-success

target/testdata/preimages.bin:
	mkdir -p `dirname $@`
	python3 scripts/create-test-preimages.py $@

contracts/test/prover/proofs/rust-%.json: $(arbitrator_cases)/rust/$(wasm32_wasi)/%.wasm $(prover_bin) $(arbitrator_wasm_libs) target/testdata/preimages.bin
	$(prover_bin) $< $(arbitrator_wasm_lib_flags) -o $@ -b --allow-hostapi --require-success --inbox-add-stub-headers --inbox $(arbitrator_cases)/rust/data/msg0.bin --inbox $(arbitrator_cases)/rust/data/msg1.bin --delayed-inbox $(arbitrator_cases)/rust/data/msg0.bin --delayed-inbox $(arbitrator_cases)/rust/data/msg1.bin --preimages target/testdata/preimages.bin

contracts/test/prover/proofs/go.json: $(arbitrator_cases)/go/testcase.wasm $(prover_bin) $(arbitrator_wasm_libs) target/testdata/preimages.bin $(arbitrator_tests_link_deps) $(arbitrator_cases)/user.wasm
	$(prover_bin) $< $(arbitrator_wasm_lib_flags) -o $@ -b --require-success --preimages target/testdata/preimages.bin  --stylus-modules $(arbitrator_cases)/user.wasm

# avoid testing user.wasm in onestepproofs. It can only run as stylus program.
contracts/test/prover/proofs/user.json:
	echo "[]" > $@

# avoid testing read-inboxmsg-10 in onestepproofs. It's used for go challenge testing.
contracts/test/prover/proofs/read-inboxmsg-10.json:
	echo "[]" > $@

contracts/test/prover/proofs/global-state.json:
	echo "[]" > $@

contracts/test/prover/proofs/forward-test.json: $(arbitrator_cases)/forward-test.wasm $(arbitrator_tests_forward_deps) $(prover_bin)
	$(prover_bin) $< -o $@ --allow-hostapi $(patsubst %,-l %, $(arbitrator_tests_forward_deps))

contracts/test/prover/proofs/link.json: $(arbitrator_cases)/link.wasm $(arbitrator_tests_link_deps) $(prover_bin)
	$(prover_bin) $< -o $@ --allow-hostapi --stylus-modules $(arbitrator_tests_link_deps) --require-success

contracts/test/prover/proofs/dynamic.json: $(patsubst %,$(arbitrator_cases)/%.wasm, dynamic user) $(prover_bin)
	$(prover_bin) $< -o $@ --allow-hostapi --stylus-modules $(arbitrator_cases)/user.wasm --require-success

contracts/test/prover/proofs/bulk-memory.json: $(patsubst %,$(arbitrator_cases)/%.wasm, bulk-memory) $(prover_bin)
	$(prover_bin) $< -o $@ --allow-hostapi --stylus-modules $(arbitrator_cases)/user.wasm -b

contracts/test/prover/proofs/%.json: $(arbitrator_cases)/%.wasm $(prover_bin)
	$(prover_bin) $< -o $@ --allow-hostapi

# strategic rules to minimize dependency building

.make/lint: $(DEP_PREDICATE) build-node-deps $(ORDER_ONLY_PREDICATE) .make
	go run ./linters ./...
	golangci-lint run --fix
	yarn --cwd contracts solhint
	@touch $@

.make/fmt: $(DEP_PREDICATE) build-node-deps .make/yarndeps $(ORDER_ONLY_PREDICATE) .make
	golangci-lint fmt
	cargo +nightly fmt -- --check
	cargo +nightly fmt --manifest-path crates/wasm-testsuite/Cargo.toml -- --check
	forge fmt --root contracts-local
	@touch $@

.make/test-go: $(DEP_PREDICATE) $(go_source) build-node-deps test-go-deps $(ORDER_ONLY_PREDICATE) .make
	.github/workflows/gotestsum.sh --timeout 120m --nolog
	@touch $@

.make/test-rust: $(DEP_PREDICATE) wasm-ci-build $(ORDER_ONLY_PREDICATE) .make
	cargo test --release
	@touch $@

.make/solgen: $(DEP_PREDICATE) solgen/gen.go .make/solidity $(ORDER_ONLY_PREDICATE) .make
	mkdir -p solgen/go/
	go run solgen/gen.go
	@touch $@

.make/solidity: $(DEP_PREDICATE) safe-smart-account/contracts/*/*.sol safe-smart-account/contracts/*.sol contracts/src/*/*.sol contracts-legacy/src/*/*.sol contracts-local/src/*/*.sol .make/yarndeps $(ORDER_ONLY_PREDICATE) .make
	npm --prefix safe-smart-account run build
	yarn --cwd contracts build
	yarn --cwd contracts build:forge:yul
	yarn --cwd contracts-legacy build
	yarn --cwd contracts-legacy build:forge:yul
	+make -C contracts-local build
	@touch $@

.make/yarndeps: $(DEP_PREDICATE) */package.json */yarn.lock $(ORDER_ONLY_PREDICATE) .make
	npm --prefix safe-smart-account install
	yarn --cwd contracts install
	yarn --cwd contracts-legacy install
	+make -C contracts-local install
	@touch $@

.make/cbrotli-lib: $(DEP_PREDICATE) $(ORDER_ONLY_PREDICATE) .make
	test -f target/include/brotli/encode.h || ./scripts/build-brotli.sh -l
	test -f target/include/brotli/decode.h || ./scripts/build-brotli.sh -l
	test -f target/lib/libbrotlicommon-static.a || ./scripts/build-brotli.sh -l
	test -f target/lib/libbrotlienc-static.a || ./scripts/build-brotli.sh -l
	test -f target/lib/libbrotlidec-static.a || ./scripts/build-brotli.sh -l
	@touch $@

.make/cbrotli-wasm: $(DEP_PREDICATE) $(ORDER_ONLY_PREDICATE) .make
	test -f target/lib-wasm/libbrotlicommon-static.a || ./scripts/build-brotli.sh -w $(CBROTLI_WASM_BUILD_ARGS)
	test -f target/lib-wasm/libbrotlienc-static.a || ./scripts/build-brotli.sh -w $(CBROTLI_WASM_BUILD_ARGS)
	test -f target/lib-wasm/libbrotlidec-static.a || ./scripts/build-brotli.sh -w $(CBROTLI_WASM_BUILD_ARGS)
	@touch $@

.make/wasm-lib: $(DEP_PREDICATE) crates/wasm-libraries/soft-float/SoftFloat/build/Wasm-Clang/softfloat.a  $(ORDER_ONLY_PREDICATE) .make
	test -f crates/wasm-libraries/soft-float/bindings32.o || ./scripts/build-brotli.sh -f -d -t ..
	test -f crates/wasm-libraries/soft-float/bindings64.o || ./scripts/build-brotli.sh -f -d -t ..
	@touch $@

.make/machines: $(DEP_PREDICATE) $(ORDER_ONLY_PREDICATE) .make
	mkdir -p $(output_latest)
	touch $@

.make:
	mkdir .make


# Makefile settings

always:              # use this to force other rules to always build
.DELETE_ON_ERROR:    # causes a failure to delete its target
