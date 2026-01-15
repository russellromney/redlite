#!/bin/bash
# Query test results from SQLite
# Usage: ./query_tests.sh [command]

DB_FILE="${DB_FILE:-test_results.db}"

case "${1:-status}" in
    status|s)
        echo "=== Test Run Status ==="
        sqlite3 -header -column "$DB_FILE" "
            SELECT
                passed,
                failed,
                (SELECT COUNT(*) FROM test_results WHERE status='running') as running,
                total_tests - passed - failed - (SELECT COUNT(*) FROM test_results WHERE status='running') as pending,
                total_tests
            FROM test_runs
            ORDER BY id DESC LIMIT 1;
        "
        ;;

    failed|f)
        echo "=== Failed Tests ==="
        sqlite3 -header -column "$DB_FILE" "
            SELECT test_name, duration_ms
            FROM test_results
            WHERE status='failed' OR status='timeout'
            ORDER BY id;
        "
        ;;

    running|r)
        echo "=== Currently Running ==="
        sqlite3 -header -column "$DB_FILE" "
            SELECT test_name, started_at
            FROM test_results
            WHERE status='running';
        "
        ;;

    passed|p)
        echo "=== Passed Tests ==="
        sqlite3 -header -column "$DB_FILE" "
            SELECT test_name, duration_ms
            FROM test_results
            WHERE status='passed'
            ORDER BY id;
        "
        ;;

    slow)
        echo "=== Slowest Tests (top 10) ==="
        sqlite3 -header -column "$DB_FILE" "
            SELECT test_name, duration_ms, status
            FROM test_results
            WHERE duration_ms IS NOT NULL
            ORDER BY duration_ms DESC
            LIMIT 10;
        "
        ;;

    output)
        if [ -z "$2" ]; then
            echo "Usage: $0 output <test_name>"
            exit 1
        fi
        sqlite3 "$DB_FILE" "SELECT output FROM test_results WHERE test_name LIKE '%$2%' LIMIT 1;"
        ;;

    watch|w)
        echo "Watching test progress (Ctrl+C to stop)..."
        while true; do
            clear
            $0 status
            echo ""
            $0 running
            echo ""
            echo "Recent:"
            sqlite3 -header -column "$DB_FILE" "
                SELECT test_name, status, duration_ms
                FROM test_results
                ORDER BY id DESC LIMIT 5;
            "
            sleep 2
        done
        ;;

    *)
        echo "Usage: $0 [status|failed|running|passed|slow|output <name>|watch]"
        echo ""
        echo "Commands:"
        echo "  status  - Show pass/fail counts"
        echo "  failed  - List failed tests"
        echo "  running - Show currently running test"
        echo "  passed  - List passed tests"
        echo "  slow    - Show slowest tests"
        echo "  output  - Show output for a test"
        echo "  watch   - Live dashboard (refreshes every 2s)"
        ;;
esac
