#!/bin/bash

# Setup script for comprehensive benchmark
# Starts Redis, Dragonfly, and 4 Redlite server instances

set -e

echo "═══════════════════════════════════════════════════════════"
echo "  Comprehensive Benchmark - Service Setup"
echo "═══════════════════════════════════════════════════════════"
echo

# Helper function to check if a port is in use
port_in_use() {
    lsof -i :$1 > /dev/null 2>&1
}

# Start Redis
echo "[1/6] Checking Redis on port 6379..."
if port_in_use 6379; then
    echo "  ✓ Redis already running on port 6379"
elif command -v redis-server &> /dev/null; then
    echo "  Starting Redis (native)..."
    redis-server --port 6379 --save "" --appendonly no --daemonize yes > /dev/null 2>&1
    echo "  ✓ Redis started (native)"
elif command -v docker &> /dev/null; then
    echo "  Starting Redis (Docker)..."
    docker run -d --name redis-bench -p 6379:6379 redis:latest --save "" --appendonly no > /dev/null 2>&1 || {
        docker rm -f redis-bench > /dev/null 2>&1 || true
        docker run -d --name redis-bench -p 6379:6379 redis:latest --save "" --appendonly no > /dev/null 2>&1
    }
    echo "  ✓ Redis started (Docker)"
else
    echo "  ⚠️  Redis not found. Install with: brew install redis"
    echo "     Or install Docker to use containerized Redis"
fi

# Start Dragonfly
echo "[2/6] Checking Dragonfly on port 6380..."
if port_in_use 6380; then
    echo "  ✓ Dragonfly already running on port 6380"
elif command -v dragonfly &> /dev/null; then
    echo "  Starting Dragonfly (native)..."
    dragonfly --port 6380 --daemonize > /dev/null 2>&1 &
    echo "  ✓ Dragonfly started (native)"
elif command -v docker &> /dev/null; then
    echo "  Starting Dragonfly (Docker)..."
    docker run -d --name dragonfly-bench -p 6380:6379 docker.dragonflydb.io/dragonflydb/dragonfly > /dev/null 2>&1 || {
        docker rm -f dragonfly-bench > /dev/null 2>&1 || true
        docker run -d --name dragonfly-bench -p 6380:6379 docker.dragonflydb.io/dragonflydb/dragonfly > /dev/null 2>&1
    }
    echo "  ✓ Dragonfly started (Docker)"
else
    echo "  ⚠️  Dragonfly not found. Install with: brew install dragonfly"
    echo "     Or install Docker to use containerized Dragonfly"
fi

# Build redlite if not already built
if [ ! -f "target/release/redlite" ]; then
    echo "Building Redlite server..."
    cargo build --release > /dev/null 2>&1
fi

# Kill any existing redlite servers
pkill -f "redlite.*--addr 127.0.0.1" > /dev/null 2>&1 || true
sleep 1

# Start Redlite Server #1: SQLite Memory
echo "[3/6] Starting Redlite Server (SQLite/Memory) on port 7381..."
RUST_LOG=error ./target/release/redlite \
    --addr 127.0.0.1:7381 \
    --backend sqlite \
    --storage memory \
    > /tmp/redlite_server_sqlite_mem.log 2>&1 &
PID1=$!
sleep 1
if ps -p $PID1 > /dev/null; then
    echo "  ✓ Redlite Server (SQLite/Memory) started (PID: $PID1)"
else
    echo "  ❌ Failed to start. Check /tmp/redlite_server_sqlite_mem.log"
fi

# Start Redlite Server #2: SQLite File
echo "[4/6] Starting Redlite Server (SQLite/File) on port 7382..."
RUST_LOG=error ./target/release/redlite \
    --addr 127.0.0.1:7382 \
    --backend sqlite \
    --storage file \
    --db /tmp/redlite_bench_sqlite_file.db \
    > /tmp/redlite_server_sqlite_file.log 2>&1 &
PID2=$!
sleep 1
if ps -p $PID2 > /dev/null; then
    echo "  ✓ Redlite Server (SQLite/File) started (PID: $PID2)"
else
    echo "  ❌ Failed to start. Check /tmp/redlite_server_sqlite_file.log"
fi

# Check if turso feature is available
if ./target/release/redlite --help | grep -q "turso"; then
    HAS_TURSO=true
else
    HAS_TURSO=false
fi

if [ "$HAS_TURSO" = true ]; then
    # Start Redlite Server #3: Turso Memory
    echo "[5/6] Starting Redlite Server (Turso/Memory) on port 7383..."
    RUST_LOG=error ./target/release/redlite \
        --addr 127.0.0.1:7383 \
        --backend turso \
        --storage memory \
        > /tmp/redlite_server_turso_mem.log 2>&1 &
    PID3=$!
    sleep 1
    if ps -p $PID3 > /dev/null; then
        echo "  ✓ Redlite Server (Turso/Memory) started (PID: $PID3)"
    else
        echo "  ❌ Failed to start. Check /tmp/redlite_server_turso_mem.log"
    fi

    # Start Redlite Server #4: Turso File
    echo "[6/6] Starting Redlite Server (Turso/File) on port 7384..."
    RUST_LOG=error ./target/release/redlite \
        --addr 127.0.0.1:7384 \
        --backend turso \
        --storage file \
        --db /tmp/redlite_bench_turso_file.db \
        > /tmp/redlite_server_turso_file.log 2>&1 &
    PID4=$!
    sleep 1
    if ps -p $PID4 > /dev/null; then
        echo "  ✓ Redlite Server (Turso/File) started (PID: $PID4)"
    else
        echo "  ❌ Failed to start. Check /tmp/redlite_server_turso_file.log"
    fi
else
    echo "[5/6] Skipping Turso servers (feature not enabled)"
    echo "[6/6] Skipping Turso servers (feature not enabled)"
fi

echo
echo "═══════════════════════════════════════════════════════════"
echo "  All services started successfully!"
echo "═══════════════════════════════════════════════════════════"
echo
echo "Services running:"
echo "  • Redis:                          127.0.0.1:6379"
echo "  • Dragonfly:                      127.0.0.1:6380"
echo "  • Redlite Server (SQLite/Memory): 127.0.0.1:7381"
echo "  • Redlite Server (SQLite/File):   127.0.0.1:7382"
if [ "$HAS_TURSO" = true ]; then
echo "  • Redlite Server (Turso/Memory):  127.0.0.1:7383"
echo "  • Redlite Server (Turso/File):    127.0.0.1:7384"
fi
echo
echo "You can now run the benchmark:"
echo "  cargo bench --bench comprehensive_comparison"
echo
echo "To stop all services, run:"
echo "  ./benches/cleanup_services.sh"
echo
