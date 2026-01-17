.PHONY: build test clean ffi help

# Build directory
BUILD_DIR := build

# Default target
all: build

# Build the FFI library first
ffi:
	@echo "Building FFI library..."
	cd ../../crates/redlite-ffi && cargo build --release

# Configure and build
build: ffi
	@mkdir -p $(BUILD_DIR)
	cd $(BUILD_DIR) && cmake -DCMAKE_BUILD_TYPE=Release ..
	cd $(BUILD_DIR) && cmake --build .

# Build debug
debug: ffi
	@mkdir -p $(BUILD_DIR)
	cd $(BUILD_DIR) && cmake -DCMAKE_BUILD_TYPE=Debug ..
	cd $(BUILD_DIR) && cmake --build .

# Run tests
test: build
	cd $(BUILD_DIR) && ctest --output-on-failure

# Run example
example: build
	./$(BUILD_DIR)/basic_example

# Clean build artifacts
clean:
	rm -rf $(BUILD_DIR)

# Install
install: build
	cd $(BUILD_DIR) && cmake --install . --prefix /usr/local

help:
	@echo "Available targets:"
	@echo "  build    - Build the SDK and tests (default)"
	@echo "  debug    - Build with debug symbols"
	@echo "  ffi      - Build just the FFI library"
	@echo "  test     - Run all tests"
	@echo "  example  - Run the basic example"
	@echo "  clean    - Remove build artifacts"
	@echo "  install  - Install to /usr/local"
	@echo "  help     - Show this help"
