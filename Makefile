.PHONY: build run test test-unit test-integration clean fmt lint check release help bench-scale bench-scale-fly bench-realistic

# Default target
all: check build

# Build debug binary
build:
	cargo build

# Build release binary
release:
	cargo build --release

# Run the server (debug mode)
run:
	cargo run -- --db redlite.db --addr 127.0.0.1:6379

# Run with in-memory database
run-memory:
	cargo run -- --db :memory: --addr 127.0.0.1:6379

# Run all tests
test: test-unit test-integration

# Run unit tests only
test-unit:
	cargo test --lib

# Run integration tests (requires redis-cli)
test-integration:
	cargo test --test integration -- --test-threads=1

# Format code
fmt:
	cargo fmt

# Check formatting without modifying
fmt-check:
	cargo fmt -- --check

# Run clippy lints
lint:
	cargo clippy -- -D warnings

# Run all checks (fmt + lint + test)
check: fmt-check lint test-unit

# Clean build artifacts
clean:
	cargo clean
	rm -f redlite.db redlite.db-wal redlite.db-shm

# Clean test databases
clean-test:
	rm -f /tmp/redlite_test_*.db*

# Watch and run tests on change (requires cargo-watch)
watch:
	cargo watch -x 'test --lib'

# Generate docs
docs:
	cargo doc --no-deps --open

# Run benchmarks (Session 18+)
bench:
	cargo bench --bench redlite_benchmarks

# Run quick benchmarks with reduced timing
bench-quick:
	cargo bench --bench redlite_benchmarks -- --warm-up-time 1 --measurement-time 1

# Run full comparison (all 5 scenarios)
# Requires: redis-server on 6379, redlite on 6380 (file), redlite on 6381 (memory)
bench-compare-all:
	@echo "=== Full Comparison Benchmark ==="
	@echo "Make sure these servers are running:"
	@echo "  redis-server (port 6379)"
	@echo "  cargo run --release -- --db /tmp/redlite_bench.db --addr 127.0.0.1:6380"
	@echo "  cargo run --release -- --db :memory: --addr 127.0.0.1:6381"
	@echo ""
	cargo bench --bench redlite_benchmarks -- full_comparison

# Compare benchmarks against baseline.json
bench-compare:
	@python3 scripts/compare_baseline.py 15

# Run benchmarks and compare against baseline
bench-check: bench bench-compare

# Save current benchmark results as new baseline
bench-save-baseline:
	@echo "Saving current benchmark results to baseline..."
	@python3 scripts/update_baseline.py

# Run scaling benchmarks (tests performance at 1K, 10K, 100K, 1M keys)
# WARNING: 1M keys requires significant memory
bench-scale:
	@echo "=== Scaling Benchmarks ==="
	@echo "Testing performance degradation with dataset size..."
	cargo bench --bench redlite_benchmarks -- scaling

# Run scaling benchmarks on Fly.io (8GB RAM, 4 CPU)
bench-scale-fly:
	@echo "Building and deploying benchmark to Fly.io..."
	fly apps create redlite-bench --org personal 2>/dev/null || true
	fly deploy -c fly.bench.toml --dockerfile Dockerfile.bench --now
	@echo "Waiting for benchmark to complete..."
	fly logs -c fly.bench.toml
	fly apps destroy redlite-bench -y

# Run realistic comparison benchmarks
bench-realistic:
	@echo "=== Realistic Comparison Benchmarks ==="
	cargo bench --bench redlite_benchmarks -- realistic_comparison

# Show binary size
size: release
	@ls -lh target/release/redlite | awk '{print "Binary size:", $$5}'

# Quick smoke test with redis-cli
smoke:
	@echo "Starting server..."
	@cargo run --release -- --db :memory: &
	@sleep 1
	@echo "Running smoke test..."
	@redis-cli -p 6379 PING
	@redis-cli -p 6379 SET foo bar
	@redis-cli -p 6379 GET foo
	@redis-cli -p 6379 QUIT
	@echo "Smoke test passed!"

# SDK Generation
sdk-commands:
	@echo "# Redlite Supported Commands" > sdks/COMMANDS.md
	@echo "" >> sdks/COMMANDS.md
	@echo "Auto-generated from \`crates/redlite/src/server/mod.rs\`" >> sdks/COMMANDS.md
	@echo "" >> sdks/COMMANDS.md
	@echo "## Command List" >> sdks/COMMANDS.md
	@echo "" >> sdks/COMMANDS.md
	@grep -oE '"[A-Z][A-Z0-9._]+" => cmd_' crates/redlite/src/server/mod.rs \
		| sed 's/" => cmd_//' | sed 's/"//' | sort -u \
		| while read cmd; do echo "- \`$$cmd\`"; done >> sdks/COMMANDS.md
	@echo "" >> sdks/COMMANDS.md
	@echo "**Total:** $$(grep -c '`' sdks/COMMANDS.md) commands" >> sdks/COMMANDS.md
	@echo "Generated sdks/COMMANDS.md"

sdk-update:
	@test -n "$(lang)" || (echo "Usage: make sdk-update lang=<python|go|...>" && exit 1)
	@echo "Updating $(lang) SDK..."
	claude -c "Update sdks/$(lang)/ to implement all commands in sdks/COMMANDS.md. Follow sdks/TEMPLATE.md for README style. Generate idiomatic $(lang) code that wraps a Redis client."

sdk-sync: sdk-commands
	@echo "Syncing all SDKs..."
	@make sdk-update lang=python || true
	@make sdk-update lang=go || true

help:
	@echo "Available targets:"
	@echo "  build             - Build debug binary"
	@echo "  release           - Build release binary"
	@echo "  run               - Run server with file DB"
	@echo "  run-memory        - Run server with in-memory DB"
	@echo "  test              - Run all tests"
	@echo "  test-unit         - Run unit tests only"
	@echo "  test-integration  - Run integration tests"
	@echo "  bench             - Run full benchmark suite"
	@echo "  bench-quick       - Run benchmarks with reduced timing"
	@echo "  bench-scale       - Run scaling benchmarks (1K-1M keys)"
	@echo "  bench-scale-fly   - Run scaling benchmarks on Fly.io (8GB RAM)"
	@echo "  bench-realistic   - Run realistic comparison benchmarks"
	@echo "  bench-compare     - Compare benchmarks against baseline"
	@echo "  bench-check       - Run benchmarks and compare against baseline"
	@echo "  bench-save-baseline - Save current results as new baseline"
	@echo "  fmt               - Format code"
	@echo "  lint              - Run clippy"
	@echo "  check             - Run fmt-check + lint + test-unit"
	@echo "  clean             - Clean build artifacts and DB files"
	@echo "  docs              - Generate and open documentation"
	@echo "  sdk-commands      - Generate sdks/COMMANDS.md from Rust source"
	@echo "  sdk-update lang=X - Update SDK for language X using Claude"
	@echo "  sdk-sync          - Update all SDKs"
	@echo "  help              - Show this help"
