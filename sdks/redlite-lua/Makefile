# Redlite Lua SDK Makefile

.PHONY: all build test clean install oracle help

# Default target
all: build

# Build the FFI library (from crates/redlite-ffi)
build:
	@echo "Building Redlite FFI library..."
	cd ../../crates/redlite-ffi && cargo build --release
	@echo "Library built at: ../../crates/redlite-ffi/target/release/libredlite_ffi.dylib"

# Run busted tests
test: build
	@echo "Running Lua SDK tests..."
	REDLITE_LIB_PATH=../../crates/redlite-ffi/target/release/libredlite_ffi.dylib \
		busted spec/

# Run oracle tests
oracle: build
	@echo "Running oracle tests..."
	cd ../oracle/runners && \
		REDLITE_LIB_PATH=../../../crates/redlite-ffi/target/release/libredlite_ffi.dylib \
		luajit lua_runner.lua -v

# Install via LuaRocks
install:
	luarocks make

# Clean build artifacts
clean:
	rm -rf *.rock

# Help
help:
	@echo "Redlite Lua SDK"
	@echo ""
	@echo "Targets:"
	@echo "  build   - Build the Redlite FFI library (cargo build --release)"
	@echo "  test    - Run busted unit tests"
	@echo "  oracle  - Run oracle compatibility tests"
	@echo "  install - Install via LuaRocks"
	@echo "  clean   - Clean build artifacts"
	@echo "  help    - Show this help"
	@echo ""
	@echo "Prerequisites:"
	@echo "  - LuaJIT (required for FFI support)"
	@echo "  - Rust toolchain (for building FFI library)"
	@echo "  - busted (luarocks install busted)"
	@echo "  - lyaml (luarocks install lyaml) - for oracle tests"
	@echo ""
	@echo "Environment Variables:"
	@echo "  REDLITE_LIB_PATH - Path to libredlite_ffi.dylib/.so"
