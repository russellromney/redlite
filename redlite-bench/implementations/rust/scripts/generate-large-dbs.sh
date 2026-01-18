#!/bin/bash
# Generate large benchmark databases and upload to Tigris
# Run this on a Fly.io machine with sufficient disk space and fast network
#
# Prerequisites:
# - Rust toolchain installed
# - AWS CLI configured with Tigris credentials
# - Sufficient disk space (~50GB for all databases)
#
# Usage:
#   fly ssh console -a your-app-name
#   curl -O https://raw.githubusercontent.com/.../generate-large-dbs.sh
#   chmod +x generate-large-dbs.sh
#   ./generate-large-dbs.sh

set -euo pipefail

# Configuration
TIGRIS_BUCKET="redlite-bench"
WORK_DIR="/data/benchmark-dbs"
BENCH_BIN="./target/release/redlite-bench"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() { echo -e "${GREEN}[$(date +%H:%M:%S)]${NC} $1"; }
warn() { echo -e "${YELLOW}[$(date +%H:%M:%S)] WARNING:${NC} $1"; }
error() { echo -e "${RED}[$(date +%H:%M:%S)] ERROR:${NC} $1"; exit 1; }

# Check prerequisites
check_prereqs() {
    log "Checking prerequisites..."

    command -v cargo >/dev/null 2>&1 || error "Rust/cargo not found. Install with: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    command -v aws >/dev/null 2>&1 || error "AWS CLI not found. Install with: pip install awscli"

    if [[ -z "${AWS_ACCESS_KEY_ID:-}" ]] || [[ -z "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
        error "Tigris credentials not set. Export AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
    fi

    # Check disk space (need ~50GB)
    available=$(df -BG "${WORK_DIR%/*}" 2>/dev/null | awk 'NR==2 {print $4}' | tr -d 'G')
    if [[ "${available:-0}" -lt 50 ]]; then
        warn "Only ${available}GB available. Need ~50GB for all databases."
    fi

    log "Prerequisites OK"
}

# Build the benchmark tool
build_bench() {
    log "Building redlite-bench..."
    cargo build --release
    log "Build complete"
}

# Generate a database
generate_db() {
    local name=$1
    local keys=$2
    local output="${WORK_DIR}/${name}"

    log "Generating ${name} (${keys} keys)..."

    mkdir -p "${WORK_DIR}"

    # Remove existing if present
    rm -f "${output}" "${output}-wal" "${output}-shm"

    time ${BENCH_BIN} generate-db \
        -o "${output}" \
        --strings "${keys}" \
        --lists 0 \
        --hashes 0 \
        --sets 0 \
        --sorted-sets 0 \
        -v

    local size=$(du -h "${output}" | cut -f1)
    log "Generated ${name}: ${size}"
}

# Upload to Tigris
upload_to_tigris() {
    local name=$1
    local file="${WORK_DIR}/${name}"

    if [[ ! -f "${file}" ]]; then
        error "File not found: ${file}"
    fi

    log "Uploading ${name} to Tigris..."

    aws s3 cp "${file}" "s3://${TIGRIS_BUCKET}/${name}" \
        --endpoint-url "https://fly.storage.tigris.dev" \
        --acl public-read

    log "Uploaded: https://fly.storage.tigris.dev/${TIGRIS_BUCKET}/${name}"
}

# Main
main() {
    log "=== Redlite Benchmark Database Generator ==="

    check_prereqs
    build_bench

    # Generate databases
    generate_db "redlite-10m.db" 10000000
    generate_db "redlite-50m.db" 50000000
    generate_db "redlite-100m.db" 100000000

    # Upload to Tigris
    upload_to_tigris "redlite-10m.db"
    upload_to_tigris "redlite-50m.db"
    upload_to_tigris "redlite-100m.db"

    log "=== Complete ==="
    log "Databases available at:"
    log "  https://fly.storage.tigris.dev/${TIGRIS_BUCKET}/redlite-10m.db"
    log "  https://fly.storage.tigris.dev/${TIGRIS_BUCKET}/redlite-50m.db"
    log "  https://fly.storage.tigris.dev/${TIGRIS_BUCKET}/redlite-100m.db"
}

# Allow running individual functions
case "${1:-main}" in
    generate-10m)  generate_db "redlite-10m.db" 10000000 ;;
    generate-50m)  generate_db "redlite-50m.db" 50000000 ;;
    generate-100m) generate_db "redlite-100m.db" 100000000 ;;
    upload-10m)    upload_to_tigris "redlite-10m.db" ;;
    upload-50m)    upload_to_tigris "redlite-50m.db" ;;
    upload-100m)   upload_to_tigris "redlite-100m.db" ;;
    upload-all)    upload_to_tigris "redlite-10m.db"; upload_to_tigris "redlite-50m.db"; upload_to_tigris "redlite-100m.db" ;;
    main)          main ;;
    *)             echo "Usage: $0 [generate-10m|generate-50m|generate-100m|upload-10m|upload-50m|upload-100m|upload-all|main]" ;;
esac
