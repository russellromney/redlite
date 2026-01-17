.PHONY: build test clean ffi example check help

# Default target
all: build

# Build the FFI library first
ffi:
	@echo "Building FFI library..."
	cd ../../crates/redlite-ffi && cargo build --release

# Build the Zig library
build: ffi
	@echo "Building Zig SDK..."
	zig build

# Run unit tests
test: ffi
	@echo "Running tests..."
	zig build test

# Run the example
example: ffi
	@echo "Running example..."
	zig build example

# Type check only (no build)
check:
	zig build check

# Clean build artifacts
clean:
	rm -rf zig-cache zig-out .zig-cache

help:
	@echo "Available targets:"
	@echo "  build   - Build the SDK (default)"
	@echo "  test    - Run all unit tests"
	@echo "  example - Run the basic example"
	@echo "  ffi     - Build just the FFI library"
	@echo "  check   - Type check without building"
	@echo "  clean   - Remove build artifacts"
	@echo "  help    - Show this help"
	@echo ""
	@echo "Note: Building requires the Redlite FFI library to be compiled."
	@echo "      Run 'make ffi' first if you haven't already."
