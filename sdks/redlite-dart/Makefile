.PHONY: help build codegen test clean setup check

help:
	@echo "Redlite Dart SDK"
	@echo ""
	@echo "Prerequisites:"
	@echo "  - Flutter SDK: https://flutter.dev/docs/get-started/install"
	@echo "  - Rust toolchain: https://rustup.rs"
	@echo ""
	@echo "Commands:"
	@echo "  make setup     - Install dependencies and setup environment"
	@echo "  make codegen   - Generate Dart/Rust bindings"
	@echo "  make build     - Build for current platform"
	@echo "  make check     - Check Rust code compiles"
	@echo "  make test      - Run tests"
	@echo "  make clean     - Clean build artifacts"
	@echo ""
	@echo "Platform builds:"
	@echo "  make build-ios     - Build for iOS"
	@echo "  make build-macos   - Build for macOS"
	@echo "  make build-android - Build for Android (requires NDK)"

setup:
	@echo "Checking prerequisites..."
	@which flutter > /dev/null || (echo "Error: Flutter not found. Install from https://flutter.dev" && exit 1)
	@which cargo > /dev/null || (echo "Error: Cargo not found. Install from https://rustup.rs" && exit 1)
	@echo "Installing flutter_rust_bridge_codegen..."
	cargo install flutter_rust_bridge_codegen
	@echo "Getting Flutter dependencies..."
	flutter pub get
	@echo "Setup complete!"

codegen:
	@echo "Generating Dart/Rust bindings..."
	flutter_rust_bridge_codegen generate

check:
	@echo "Checking Rust code..."
	cd rust && cargo check

build: codegen
	@echo "Building Rust library..."
	cd rust && cargo build --release

build-ios:
	@echo "Building for iOS..."
	rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios
	cd rust && cargo build --release --target aarch64-apple-ios
	cd rust && cargo build --release --target aarch64-apple-ios-sim
	cd rust && cargo build --release --target x86_64-apple-ios

build-macos:
	@echo "Building for macOS..."
	rustup target add aarch64-apple-darwin x86_64-apple-darwin
	cd rust && cargo build --release --target aarch64-apple-darwin
	cd rust && cargo build --release --target x86_64-apple-darwin

build-android:
	@echo "Building for Android..."
	cargo install cargo-ndk
	rustup target add aarch64-linux-android armv7-linux-androideabi x86_64-linux-android i686-linux-android
	cd rust && cargo ndk -t armeabi-v7a -t arm64-v8a -t x86 -t x86_64 build --release

test:
	@echo "Running Dart tests..."
	flutter test

clean:
	@echo "Cleaning build artifacts..."
	cd rust && cargo clean
	flutter clean 2>/dev/null || true
	rm -rf build/
