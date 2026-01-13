#!/usr/bin/env python3
"""
Local Baseline Comparison Script

Compares current benchmark runs against the saved baseline.json file.
Run benchmarks first with: cargo bench --bench redlite_benchmarks

Usage:
    python3 scripts/compare_baseline.py [threshold_percent]

Example:
    python3 scripts/compare_baseline.py 15
    python3 scripts/compare_baseline.py  # Uses default 10% threshold
"""

import json
import os
import re
import sys
from pathlib import Path
from typing import Optional

# Default regression threshold
DEFAULT_THRESHOLD = 10.0

# Colors for terminal output
class Colors:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


def load_baseline(baseline_path: str) -> dict:
    """Load baseline.json metrics."""
    try:
        with open(baseline_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Baseline file not found: {baseline_path}")
        sys.exit(2)
    except json.JSONDecodeError as e:
        print(f"Error parsing baseline JSON: {e}")
        sys.exit(2)


def parse_criterion_estimates(criterion_dir: str) -> dict:
    """Parse Criterion benchmark results from target/criterion directory."""
    results = {}
    criterion_path = Path(criterion_dir)

    if not criterion_path.exists():
        return results

    # Walk through benchmark directories
    for bench_dir in criterion_path.iterdir():
        if not bench_dir.is_dir() or bench_dir.name.startswith('.'):
            continue

        # Look for estimates.json in each benchmark
        estimates_file = bench_dir / 'new' / 'estimates.json'
        if estimates_file.exists():
            try:
                with open(estimates_file, 'r') as f:
                    data = json.load(f)
                    # Extract mean time in nanoseconds
                    if 'mean' in data:
                        mean_ns = data['mean']['point_estimate']
                        results[bench_dir.name] = mean_ns
            except (json.JSONDecodeError, KeyError):
                continue

        # Check subdirectories for grouped benchmarks
        for sub_dir in bench_dir.iterdir():
            if sub_dir.is_dir():
                estimates_file = sub_dir / 'new' / 'estimates.json'
                if estimates_file.exists():
                    try:
                        with open(estimates_file, 'r') as f:
                            data = json.load(f)
                            if 'mean' in data:
                                mean_ns = data['mean']['point_estimate']
                                full_name = f"{bench_dir.name}/{sub_dir.name}"
                                results[full_name] = mean_ns
                    except (json.JSONDecodeError, KeyError):
                        continue

    return results


def ns_to_us(ns: float) -> float:
    """Convert nanoseconds to microseconds."""
    return ns / 1000.0


def format_time(us: float) -> str:
    """Format microseconds to human-readable string."""
    if us >= 1000:
        return f"{us / 1000:.2f} ms"
    else:
        return f"{us:.2f} µs"


def map_criterion_to_baseline(criterion_name: str) -> Optional[tuple[str, str]]:
    """Map Criterion benchmark name to baseline.json path."""
    # Mapping from Criterion names to baseline.json structure
    mappings = {
        # String operations
        'string_set_64b': ('string_operations', 'set_64b'),
        'string_get': ('string_operations', 'get'),
        'string_set_1024b': ('string_operations', 'set_1024b'),
        'string_set_10240b': ('string_operations', 'set_10240b'),
        'string_incr': ('string_operations', 'incr'),
        'string_append': ('string_operations', 'append'),

        # Hash operations
        'hash_hset_10_fields': ('hash_operations', 'hset_10_fields'),
        'hash_hset_100_fields': ('hash_operations', 'hset_100_fields'),
        'hash_hset_1000_fields': ('hash_operations', 'hset_1000_fields'),
        'hash_hget': ('hash_operations', 'hget'),
        'hash_hgetall_10': ('hash_operations', 'hgetall_10_fields'),
        'hash_hgetall_100': ('hash_operations', 'hgetall_100_fields'),

        # List operations
        'list_lpush': ('list_operations', 'lpush'),
        'list_lpop': ('list_operations', 'lpop'),
        'list_lrange_10': ('list_operations', 'lrange_10_items'),
        'list_lrange_100': ('list_operations', 'lrange_100_items'),
        'list_lrange_1000': ('list_operations', 'lrange_1000_items'),

        # Set operations
        'set_sadd': ('set_operations', 'sadd'),
        'set_smembers_10': ('set_operations', 'smembers_10_items'),
        'set_smembers_100': ('set_operations', 'smembers_100_items'),
        'set_smembers_1000': ('set_operations', 'smembers_1000_items'),

        # Sorted set operations
        'sorted_set_zadd': ('sorted_set_operations', 'zadd'),
        'sorted_set_zrange': ('sorted_set_operations', 'zrange'),

        # Workloads
        'workload_mixed': ('workloads', 'mixed_80_20_reads'),

        # Expiration
        'expiration_expire': ('expiration', 'expire'),
    }

    # Try direct mapping
    if criterion_name in mappings:
        return mappings[criterion_name]

    # Try normalized name (replace slashes, underscores)
    normalized = criterion_name.replace('/', '_').lower()
    for key, value in mappings.items():
        if normalized == key or normalized.endswith(key):
            return value

    return None


def compare_results(baseline: dict, current: dict, threshold: float) -> list[dict]:
    """Compare current results against baseline."""
    comparisons = []

    for criterion_name, current_ns in current.items():
        mapping = map_criterion_to_baseline(criterion_name)
        if mapping is None:
            continue

        category, metric = mapping
        if category not in baseline.get('metrics', {}):
            continue
        if metric not in baseline['metrics'][category]:
            continue

        baseline_metric = baseline['metrics'][category][metric]
        baseline_us = baseline_metric.get('mean_us', 0)

        if baseline_us == 0:
            continue

        current_us = ns_to_us(current_ns)
        change_percent = ((current_us - baseline_us) / baseline_us) * 100

        comparisons.append({
            'name': f"{category}/{metric}",
            'baseline_us': baseline_us,
            'current_us': current_us,
            'change_percent': change_percent,
            'regression': change_percent > threshold,
        })

    return comparisons


def print_comparison_table(comparisons: list[dict], threshold: float):
    """Print comparison results as a table."""
    print(f"\n{Colors.BOLD}Benchmark Comparison (threshold: {threshold}%){Colors.RESET}")
    print("=" * 80)
    print(f"{'Operation':<40} {'Baseline':>12} {'Current':>12} {'Change':>10} {'Status':>8}")
    print("-" * 80)

    regressions = []

    for comp in sorted(comparisons, key=lambda x: x['change_percent'], reverse=True):
        baseline_str = format_time(comp['baseline_us'])
        current_str = format_time(comp['current_us'])
        change_str = f"{comp['change_percent']:+.1f}%"

        if comp['regression']:
            status = f"{Colors.RED}REGRESS{Colors.RESET}"
            change_str = f"{Colors.RED}{change_str}{Colors.RESET}"
            regressions.append(comp)
        elif comp['change_percent'] < -5:
            status = f"{Colors.GREEN}FASTER{Colors.RESET}"
            change_str = f"{Colors.GREEN}{change_str}{Colors.RESET}"
        else:
            status = f"{Colors.BLUE}OK{Colors.RESET}"

        print(f"{comp['name']:<40} {baseline_str:>12} {current_str:>12} {change_str:>20} {status:>18}")

    print("-" * 80)

    return regressions


def main():
    # Parse threshold argument
    threshold = DEFAULT_THRESHOLD
    if len(sys.argv) > 1:
        try:
            threshold = float(sys.argv[1])
        except ValueError:
            print(f"Invalid threshold: {sys.argv[1]}")
            sys.exit(2)

    # Determine paths
    script_dir = Path(__file__).parent
    project_dir = script_dir.parent
    baseline_path = project_dir / 'benches' / 'baseline.json'
    criterion_dir = project_dir / 'target' / 'criterion'

    print(f"{Colors.BOLD}Redlite Benchmark Comparison{Colors.RESET}")
    print(f"Baseline: {baseline_path}")
    print(f"Criterion: {criterion_dir}")

    # Load baseline
    baseline = load_baseline(str(baseline_path))
    print(f"Baseline date: {baseline.get('baseline_date', 'unknown')}")

    # Parse current results
    current = parse_criterion_estimates(str(criterion_dir))

    if not current:
        print(f"\n{Colors.YELLOW}Warning: No Criterion results found.{Colors.RESET}")
        print("Run benchmarks first: cargo bench --bench redlite_benchmarks")
        sys.exit(0)

    print(f"Found {len(current)} current benchmark results")

    # Compare
    comparisons = compare_results(baseline, current, threshold)

    if not comparisons:
        print(f"\n{Colors.YELLOW}Warning: No matching benchmarks found for comparison.{Colors.RESET}")
        print("Benchmark names may not match baseline.json structure.")
        sys.exit(0)

    # Print results
    regressions = print_comparison_table(comparisons, threshold)

    # Summary
    print()
    if regressions:
        print(f"{Colors.RED}{Colors.BOLD}FAILED: {len(regressions)} regression(s) detected{Colors.RESET}")
        for reg in regressions:
            print(f"  - {reg['name']}: {reg['change_percent']:+.1f}%")
        sys.exit(1)
    else:
        print(f"{Colors.GREEN}{Colors.BOLD}PASSED: No regressions exceeding {threshold}% threshold{Colors.RESET}")
        sys.exit(0)


if __name__ == "__main__":
    main()
