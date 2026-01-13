#!/bin/bash
set -e

# Output file for results (accessible via SSH)
OUTPUT_FILE="/tmp/benchmark_results.txt"

# Tee output to file and stdout
exec > >(tee -a "$OUTPUT_FILE") 2>&1

echo "=== Redlite Scaling Benchmarks ==="
echo "Results also saved to: $OUTPUT_FILE"
echo "Machine: $(cat /proc/cpuinfo | grep 'model name' | head -1 | cut -d: -f2)"
echo "Memory: $(free -h | grep Mem | awk '{print $2}')"
echo "CPUs: $(nproc)"
echo ""

# Function to flush Redis and delete RDB files
flush_redis() {
    echo "Flushing Redis..."
    redis-cli FLUSHALL ASYNC 2>/dev/null || true
    rm -f /var/lib/redis/dump.rdb /app/*.rdb dump.rdb 2>/dev/null || true
}

# Function to flush Redlite server
flush_redlite() {
    echo "Flushing Redlite server..."
    redis-cli -p 6381 FLUSHALL 2>/dev/null || true
}

# Start Redis in background with RDB snapshots (default)
echo "Starting Redis..."
redis-server --daemonize yes --save "60 1" --dir /tmp
sleep 1

# Verify Redis is running
redis-cli ping || { echo "Redis failed to start"; exit 1; }

# Start Redlite server (memory mode) in background
echo "Starting Redlite server (memory mode)..."
/app/redlite --db :memory: --addr 127.0.0.1:6381 &
REDLITE_PID=$!
sleep 2

# Verify Redlite is running
redis-cli -p 6381 ping || { echo "Redlite failed to start"; exit 1; }

echo ""
echo "Running scaling benchmarks..."
echo "This tests performance at 1K, 10K, 100K, and 1M keys"
echo "Note: State is cleared between each benchmark size"
echo ""

# Run the scaling benchmarks
# Use script to create a pseudo-terminal so Criterion outputs timing results
# The benchmark itself handles data setup per test, but we flush between major sections
script -q -c "/app/bench scaling --noplot --color never" /dev/null 2>&1

# Final cleanup
echo ""
echo "Cleaning up..."
flush_redis
flush_redlite
rm -f /tmp/dump.rdb 2>/dev/null || true

echo ""
echo "=== Benchmark Complete ==="
echo "Full results saved to: $OUTPUT_FILE"

# Keep machine running briefly so results can be retrieved
echo ""
echo "Sleeping 5 minutes to allow result retrieval via SSH..."
sleep 300
