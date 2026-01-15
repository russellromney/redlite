# Oracle Test Makefile
# Cross-SDK consistency testing for Redlite

PYTHON_VENV := ../redlite-python/.venv/bin/python
SPEC_DIR := spec
RUST_RUNNER := runners/rust_runner

.PHONY: help test test-python test-ts test-rust test-go test-verbose clean build-rust

help:
	@echo "Oracle Test Commands:"
	@echo "  make test           - Run all oracle tests (Python + TypeScript)"
	@echo "  make test-all       - Run all including Rust baseline and Go"
	@echo "  make test-python    - Run oracle tests against Python SDK"
	@echo "  make test-ts        - Run oracle tests against TypeScript SDK"
	@echo "  make test-rust      - Run oracle tests against Rust core (baseline)"
	@echo "  make test-go        - Run oracle tests against Go SDK"
	@echo "  make test-verbose   - Run all tests with verbose output"
	@echo "  make build-rust     - Build Rust runner"
	@echo ""
	@echo "Spec files:"
	@ls -1 $(SPEC_DIR)/*.yaml 2>/dev/null || echo "  (none found)"

# Run Python + TypeScript SDK tests
test: test-python test-ts
	@echo ""
	@echo "All oracle tests complete!"

# Run all including Rust baseline and Go
test-all: test-rust test-go test-python test-ts
	@echo ""
	@echo "All oracle tests complete!"

# Build Rust runner
build-rust:
	cd $(RUST_RUNNER) && cargo build --release

# Rust core tests (baseline reference)
test-rust: build-rust
	@echo "Running Oracle tests against Rust core (baseline)..."
	cd $(RUST_RUNNER) && cargo run --release -- ../../spec

# Go SDK tests
test-go:
	@echo "Running Oracle tests against Go SDK..."
	cd runners && go run go_runner.go

# Go SDK tests (verbose)
test-go-verbose:
	cd runners && go run go_runner.go -v

# Python SDK tests
test-python:
	@echo "Running Oracle tests against Python SDK..."
	$(PYTHON_VENV) runners/python_runner.py

# Python SDK tests (verbose)
test-python-verbose:
	$(PYTHON_VENV) runners/python_runner.py -v

# TypeScript SDK tests
test-ts:
	@echo "Running Oracle tests against TypeScript SDK..."
	node runners/ts_runner.js

# TypeScript SDK tests (verbose)
test-ts-verbose:
	node runners/ts_runner.js -v

# Verbose output for all
test-verbose: test-python-verbose test-ts-verbose

# Run a single spec file against Python
test-spec-python:
	@if [ -z "$(SPEC)" ]; then echo "Usage: make test-spec-python SPEC=strings.yaml"; exit 1; fi
	$(PYTHON_VENV) runners/python_runner.py $(SPEC_DIR)/$(SPEC)

# Run a single spec file against TypeScript
test-spec-ts:
	@if [ -z "$(SPEC)" ]; then echo "Usage: make test-spec-ts SPEC=strings.yaml"; exit 1; fi
	node runners/ts_runner.js $(SPEC_DIR)/$(SPEC)

clean:
	rm -rf node_modules package-lock.json
