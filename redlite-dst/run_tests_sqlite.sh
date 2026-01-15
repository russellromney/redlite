#!/bin/bash
# Parallel test runner with SQLite logging
# Usage: ./run_tests_sqlite.sh [db_file] [test_filter] [parallelism]

DB_FILE="${1:-test_results.db}"
TEST_FILTER="${2:-}"
PARALLEL="${3:-8}"
TIMEOUT_SECS=30

# Create/reset database with WAL mode for concurrent writes
sqlite3 "$DB_FILE" << 'EOF'
PRAGMA journal_mode=WAL;
DROP TABLE IF EXISTS test_runs;
DROP TABLE IF EXISTS test_results;

CREATE TABLE test_runs (
    id INTEGER PRIMARY KEY,
    started_at TEXT DEFAULT (datetime('now')),
    completed_at TEXT,
    total_tests INTEGER,
    parallelism INTEGER
);

CREATE TABLE test_results (
    id INTEGER PRIMARY KEY,
    run_id INTEGER,
    test_name TEXT UNIQUE,
    status TEXT CHECK(status IN ('pending', 'running', 'passed', 'failed', 'timeout')),
    started_at TEXT,
    completed_at TEXT,
    duration_ms INTEGER,
    output TEXT,
    FOREIGN KEY (run_id) REFERENCES test_runs(id)
);

CREATE INDEX idx_test_status ON test_results(status);
EOF

echo "Database: $DB_FILE (WAL mode enabled)"

# Get list of tests
echo "Listing tests..."
if [ -n "$TEST_FILTER" ]; then
    TESTS=$(cargo test --test oracle -- --list 2>/dev/null | grep ": test$" | sed 's/: test$//' | grep "$TEST_FILTER")
else
    TESTS=$(cargo test --test oracle -- --list 2>/dev/null | grep ": test$" | sed 's/: test$//')
fi

TOTAL=$(echo "$TESTS" | wc -l | tr -d ' ')
echo "Found $TOTAL tests, running with parallelism=$PARALLEL"

# Create test run
RUN_ID=$(sqlite3 "$DB_FILE" "INSERT INTO test_runs (total_tests, parallelism) VALUES ($TOTAL, $PARALLEL); SELECT last_insert_rowid();")

# Insert all tests as pending
echo "$TESTS" | while read TEST; do
    sqlite3 "$DB_FILE" "INSERT INTO test_results (run_id, test_name, status) VALUES ($RUN_ID, '$TEST', 'pending');"
done

echo "Starting parallel execution..."
echo ""

# Export for subshells
export DB_FILE RUN_ID TIMEOUT_SECS

# Function to run a single test
run_single_test() {
    TEST="$1"

    # Mark as running
    sqlite3 "$DB_FILE" "UPDATE test_results SET status='running', started_at=datetime('now') WHERE test_name='$TEST';"

    START_MS=$(python3 -c 'import time; print(int(time.time()*1000))')

    # Run test
    OUTPUT=$(gtimeout ${TIMEOUT_SECS}s cargo test --test oracle "$TEST" -- --exact 2>&1)
    EXIT_CODE=$?

    END_MS=$(python3 -c 'import time; print(int(time.time()*1000))')
    DURATION=$((END_MS - START_MS))

    # Escape output for SQLite
    OUTPUT_ESCAPED=$(echo "$OUTPUT" | tail -30 | sed "s/'/''/g")

    if [ $EXIT_CODE -eq 124 ]; then
        STATUS="timeout"
        echo "✗ TIMEOUT: $TEST"
    elif echo "$OUTPUT" | grep -q "test result: ok"; then
        STATUS="passed"
        echo "✓ $TEST (${DURATION}ms)"
    else
        STATUS="failed"
        echo "✗ FAILED: $TEST"
    fi

    sqlite3 "$DB_FILE" "UPDATE test_results SET status='$STATUS', completed_at=datetime('now'), duration_ms=$DURATION, output='$OUTPUT_ESCAPED' WHERE test_name='$TEST';"
}
export -f run_single_test

# Run tests in parallel
echo "$TESTS" | xargs -P "$PARALLEL" -I {} bash -c 'run_single_test "$@"' _ {}

# Mark run complete
sqlite3 "$DB_FILE" "UPDATE test_runs SET completed_at=datetime('now') WHERE id=$RUN_ID;"

# Summary
echo ""
echo "=========================================="
PASSED=$(sqlite3 "$DB_FILE" "SELECT COUNT(*) FROM test_results WHERE status='passed';")
FAILED=$(sqlite3 "$DB_FILE" "SELECT COUNT(*) FROM test_results WHERE status='failed';")
TIMEOUT=$(sqlite3 "$DB_FILE" "SELECT COUNT(*) FROM test_results WHERE status='timeout';")

echo "✓ Passed:  $PASSED"
echo "✗ Failed:  $FAILED"
echo "⏱ Timeout: $TIMEOUT"
echo "  Total:   $TOTAL"
echo ""
echo "Query: ./query_tests.sh failed"
