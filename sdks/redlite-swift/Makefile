.PHONY: all build test clean build-ffi build-xcframework help

# Default target
all: build

# Build the FFI library first, then build Swift package
build: build-ffi
	swift build

# Build in release mode
release: build-ffi
	swift build -c release

# Run tests
test: build-ffi
	swift test

# Build the Rust FFI library for local development
build-ffi:
	cd ../../crates/redlite-ffi && cargo build --release

# Build XCFramework for all Apple platforms
build-xcframework:
	./scripts/create-xcframework.sh

# Clean build artifacts
clean:
	swift package clean
	rm -rf .build
	rm -rf Frameworks/*.xcframework

# Update the header from FFI crate
update-header:
	cp ../../crates/redlite-ffi/redlite.h Sources/CRedlite/include/redlite.h

# Generate documentation
docs:
	swift package generate-documentation

# Format code
format:
	swift-format format -i -r Sources Tests

# Lint
lint:
	swift-format lint -r Sources Tests

help:
	@echo "Redlite Swift SDK"
	@echo ""
	@echo "Usage:"
	@echo "  make build           - Build the Swift package"
	@echo "  make release         - Build in release mode"
	@echo "  make test            - Run unit tests"
	@echo "  make build-ffi       - Build the Rust FFI library"
	@echo "  make build-xcframework - Build XCFramework for distribution"
	@echo "  make clean           - Clean build artifacts"
	@echo "  make update-header   - Copy latest FFI header"
	@echo "  make docs            - Generate documentation"
	@echo "  make format          - Format code with swift-format"
	@echo "  make lint            - Lint code with swift-format"
