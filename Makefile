.PHONY: build run test test-unit test-integration clean fmt lint check release help

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

help:
	@echo "Available targets:"
	@echo "  build          - Build debug binary"
	@echo "  release        - Build release binary"
	@echo "  run            - Run server with file DB"
	@echo "  run-memory     - Run server with in-memory DB"
	@echo "  test           - Run all tests"
	@echo "  test-unit      - Run unit tests only"
	@echo "  test-integration - Run integration tests"
	@echo "  fmt            - Format code"
	@echo "  lint           - Run clippy"
	@echo "  check          - Run fmt-check + lint + test-unit"
	@echo "  clean          - Clean build artifacts and DB files"
	@echo "  docs           - Generate and open documentation"
	@echo "  help           - Show this help"
