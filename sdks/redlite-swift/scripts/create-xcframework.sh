#!/bin/bash
set -e

# XCFramework Build Script for Redlite
# This script builds libredlite_ffi for all Apple platforms and creates an XCFramework

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SDK_DIR="$(dirname "$SCRIPT_DIR")"
FFI_DIR="$SDK_DIR/../../crates/redlite-ffi"
FRAMEWORKS_DIR="$SDK_DIR/Frameworks"
BUILD_DIR="$SDK_DIR/.build/xcframework"

echo "=== Redlite XCFramework Builder ==="
echo "FFI source: $FFI_DIR"
echo "Output: $FRAMEWORKS_DIR"

# Create directories
mkdir -p "$BUILD_DIR"
mkdir -p "$FRAMEWORKS_DIR"

# Install required targets if not present
rustup target add aarch64-apple-darwin 2>/dev/null || true
rustup target add x86_64-apple-darwin 2>/dev/null || true
rustup target add aarch64-apple-ios 2>/dev/null || true
rustup target add aarch64-apple-ios-sim 2>/dev/null || true
rustup target add x86_64-apple-ios 2>/dev/null || true

cd "$FFI_DIR"

echo ""
echo "Building for macOS (arm64)..."
cargo build --release --target aarch64-apple-darwin

echo ""
echo "Building for macOS (x86_64)..."
cargo build --release --target x86_64-apple-darwin

echo ""
echo "Building for iOS (arm64)..."
cargo build --release --target aarch64-apple-ios

echo ""
echo "Building for iOS Simulator (arm64)..."
cargo build --release --target aarch64-apple-ios-sim

echo ""
echo "Building for iOS Simulator (x86_64)..."
cargo build --release --target x86_64-apple-ios

echo ""
echo "Creating fat libraries..."

# macOS fat library (arm64 + x86_64)
lipo -create \
    "$FFI_DIR/target/aarch64-apple-darwin/release/libredlite_ffi.a" \
    "$FFI_DIR/target/x86_64-apple-darwin/release/libredlite_ffi.a" \
    -output "$BUILD_DIR/libredlite_ffi-macos.a"

# iOS Simulator fat library (arm64 + x86_64)
lipo -create \
    "$FFI_DIR/target/aarch64-apple-ios-sim/release/libredlite_ffi.a" \
    "$FFI_DIR/target/x86_64-apple-ios/release/libredlite_ffi.a" \
    -output "$BUILD_DIR/libredlite_ffi-ios-simulator.a"

# Copy iOS device library
cp "$FFI_DIR/target/aarch64-apple-ios/release/libredlite_ffi.a" \
   "$BUILD_DIR/libredlite_ffi-ios.a"

# Copy header
mkdir -p "$BUILD_DIR/include"
cp "$FFI_DIR/redlite.h" "$BUILD_DIR/include/"

# Create module.modulemap for the xcframework
cat > "$BUILD_DIR/include/module.modulemap" << EOF
module CRedlite {
    header "redlite.h"
    export *
}
EOF

echo ""
echo "Creating XCFramework..."

# Remove old xcframework if exists
rm -rf "$FRAMEWORKS_DIR/libredlite_ffi.xcframework"

# Create XCFramework
xcodebuild -create-xcframework \
    -library "$BUILD_DIR/libredlite_ffi-macos.a" \
    -headers "$BUILD_DIR/include" \
    -library "$BUILD_DIR/libredlite_ffi-ios.a" \
    -headers "$BUILD_DIR/include" \
    -library "$BUILD_DIR/libredlite_ffi-ios-simulator.a" \
    -headers "$BUILD_DIR/include" \
    -output "$FRAMEWORKS_DIR/libredlite_ffi.xcframework"

echo ""
echo "=== XCFramework created successfully ==="
echo "Location: $FRAMEWORKS_DIR/libredlite_ffi.xcframework"

# Cleanup
rm -rf "$BUILD_DIR"

echo ""
echo "To use the XCFramework, update Package.swift to use:"
echo '  .binaryTarget(name: "CRedlite", path: "Frameworks/libredlite_ffi.xcframework")'
