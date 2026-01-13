#!/bin/bash

# Cleanup script for comprehensive benchmark
# Stops all benchmark services (both native and Docker)

echo "Stopping all benchmark services..."

# Stop Redis (both native and Docker)
echo "  Stopping Redis..."
# Try Docker first
docker stop redis-bench > /dev/null 2>&1 || true
docker rm redis-bench > /dev/null 2>&1 || true
# Try native Redis (kill any redis-server on port 6379)
pkill -f "redis-server.*6379" > /dev/null 2>&1 || true

echo "  Stopping Dragonfly..."
# Try Docker first
docker stop dragonfly-bench > /dev/null 2>&1 || true
docker rm dragonfly-bench > /dev/null 2>&1 || true
# Try native Dragonfly (kill any dragonfly on port 6380)
pkill -f "dragonfly.*6380" > /dev/null 2>&1 || true

# Stop Redlite servers
echo "  Stopping Redlite servers..."
pkill -f "redlite.*--addr 127.0.0.1" > /dev/null 2>&1 || true

# Clean up temp files
echo "  Cleaning up temp files..."
rm -f /tmp/redlite_bench*.db 2> /dev/null || true
rm -f /tmp/redlite_server*.log 2> /dev/null || true

echo "✓ All services stopped and cleaned up"
