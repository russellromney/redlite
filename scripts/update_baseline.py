#!/usr/bin/env python3
"""
Update Baseline Script

Updates benches/baseline.json with current Criterion benchmark results.
Run benchmarks first with: cargo bench --bench redlite_benchmarks

Usage:
    python3 scripts/update_baseline.py
"""

import json
import subprocess
from datetime import datetime
from pathlib import Path


def get_git_commit() -> str:
    """Get current git commit short hash."""
    try:
        result = subprocess.run(
            ['git', 'log', '-1', '--format=%h %s'],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return "unknown"


def parse_criterion_estimates(criterion_dir: Path) -> dict:
    """Parse Criterion benchmark results."""
    results = {}

    if not criterion_dir.exists():
        return results

    for bench_dir in criterion_dir.iterdir():
        if not bench_dir.is_dir() or bench_dir.name.startswith('.'):
            continue

        # Check main benchmark
        estimates_file = bench_dir / 'new' / 'estimates.json'
        if estimates_file.exists():
            try:
                with open(estimates_file, 'r') as f:
                    data = json.load(f)
                    if 'mean' in data:
                        mean_ns = data['mean']['point_estimate']
                        results[bench_dir.name] = mean_ns / 1000.0  # Convert to µs
            except (json.JSONDecodeError, KeyError):
                pass

        # Check subdirectories
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
                                results[full_name] = mean_ns / 1000.0  # Convert to µs
                    except (json.JSONDecodeError, KeyError):
                        pass

    return results


def update_baseline_json(current: dict, baseline_path: Path):
    """Update baseline.json with new results."""
    # Load existing baseline
    try:
        with open(baseline_path, 'r') as f:
            baseline = json.load(f)
    except FileNotFoundError:
        baseline = {"metrics": {}}

    # Update metadata
    baseline["session"] = baseline.get("session", 18) + 1
    baseline["baseline_date"] = datetime.now().strftime("%Y-%m-%d")
    baseline["commit"] = get_git_commit()
    baseline["notes"] = "Updated via scripts/update_baseline.py"

    # Mapping from Criterion names to baseline structure
    criterion_mappings = {
        'string_set_64b': ('string_operations', 'set_64b'),
        'string_get': ('string_operations', 'get'),
        'string_set_1024b': ('string_operations', 'set_1024b'),
        'string_set_10240b': ('string_operations', 'set_10240b'),
        'string_incr': ('string_operations', 'incr'),
        'string_append': ('string_operations', 'append'),
        'hash_hset_10_fields': ('hash_operations', 'hset_10_fields'),
        'hash_hset_100_fields': ('hash_operations', 'hset_100_fields'),
        'hash_hset_1000_fields': ('hash_operations', 'hset_1000_fields'),
        'hash_hget': ('hash_operations', 'hget'),
        'hash_hgetall_10': ('hash_operations', 'hgetall_10_fields'),
        'hash_hgetall_100': ('hash_operations', 'hgetall_100_fields'),
        'list_lpush': ('list_operations', 'lpush'),
        'list_lpop': ('list_operations', 'lpop'),
        'list_lrange_10': ('list_operations', 'lrange_10_items'),
        'list_lrange_100': ('list_operations', 'lrange_100_items'),
        'list_lrange_1000': ('list_operations', 'lrange_1000_items'),
        'set_sadd': ('set_operations', 'sadd'),
        'set_smembers_10': ('set_operations', 'smembers_10_items'),
        'set_smembers_100': ('set_operations', 'smembers_100_items'),
        'set_smembers_1000': ('set_operations', 'smembers_1000_items'),
        'sorted_set_zadd': ('sorted_set_operations', 'zadd'),
        'sorted_set_zrange': ('sorted_set_operations', 'zrange'),
        'workload_mixed': ('workloads', 'mixed_80_20_reads'),
        'expiration_expire': ('expiration', 'expire'),
    }

    # Update metrics
    updated_count = 0
    for criterion_name, mean_us in current.items():
        # Normalize name
        normalized = criterion_name.replace('/', '_').lower()

        for key, (category, metric) in criterion_mappings.items():
            if normalized == key or normalized.endswith(key):
                if category not in baseline["metrics"]:
                    baseline["metrics"][category] = {}

                qps = int(1_000_000 / mean_us) if mean_us > 0 else 0
                baseline["metrics"][category][metric] = {
                    "mean_us": round(mean_us, 2),
                    "qps_estimate": qps
                }
                updated_count += 1
                break

    # Update summary
    all_qps = []
    for category in baseline["metrics"].values():
        for metric in category.values():
            if "qps_estimate" in metric:
                all_qps.append(metric["qps_estimate"])

    if all_qps:
        baseline["summary"] = {
            "highest_qps": max(all_qps),
            "highest_qps_operation": "lpop (single-threaded)",
            "median_qps": sorted(all_qps)[len(all_qps) // 2],
            "estimated_embedded_mode_throughput": "50,000-100,000 QPS (single-threaded)",
            "note": "Full-featured SQLite backend with durability guarantees."
        }

    # Write updated baseline
    with open(baseline_path, 'w') as f:
        json.dump(baseline, f, indent=2)
        f.write('\n')

    return updated_count


def main():
    script_dir = Path(__file__).parent
    project_dir = script_dir.parent
    criterion_dir = project_dir / 'target' / 'criterion'
    baseline_path = project_dir / 'benches' / 'baseline.json'

    print("Redlite Baseline Update")
    print("=" * 40)

    # Parse current results
    current = parse_criterion_estimates(criterion_dir)

    if not current:
        print("Error: No Criterion results found.")
        print("Run benchmarks first: cargo bench --bench redlite_benchmarks")
        return 1

    print(f"Found {len(current)} benchmark results")

    # Update baseline
    updated = update_baseline_json(current, baseline_path)
    print(f"Updated {updated} metrics in baseline.json")
    print(f"Saved to: {baseline_path}")

    return 0


if __name__ == "__main__":
    exit(main())
