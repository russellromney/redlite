#!/usr/bin/env python3
"""
Benchmark Regression Detection Script

Parses Criterion benchmark output and checks for performance regressions
exceeding a specified threshold.

Usage:
    python3 check_regression.py <benchmark_output_file> <threshold_percent>

Example:
    python3 check_regression.py benchmark_output.txt 15

Exit codes:
    0 - No regressions detected
    1 - Regressions detected exceeding threshold
    2 - Error parsing benchmark output
"""

import re
import sys
from dataclasses import dataclass
from typing import Optional


@dataclass
class BenchmarkResult:
    name: str
    time_ns: float
    change_percent: Optional[float] = None
    regression: bool = False


def parse_time(time_str: str) -> float:
    """Parse time string to nanoseconds."""
    time_str = time_str.strip()

    # Match patterns like "17.47 µs", "3.17 us", "200.38 ms", "1.83 ns"
    match = re.match(r'([\d.]+)\s*(ns|µs|us|ms|s)', time_str)
    if not match:
        return 0.0

    value = float(match.group(1))
    unit = match.group(2)

    multipliers = {
        'ns': 1,
        'µs': 1000,
        'us': 1000,
        'ms': 1_000_000,
        's': 1_000_000_000,
    }

    return value * multipliers.get(unit, 1)


def parse_change(change_str: str) -> Optional[float]:
    """Parse change percentage from Criterion output."""
    # Match patterns like "+5.23%", "-2.10%", "No change"
    match = re.search(r'([+-]?\d+\.?\d*)%', change_str)
    if match:
        return float(match.group(1))
    return None


def parse_benchmark_output(filepath: str) -> list[BenchmarkResult]:
    """Parse Criterion benchmark output file."""
    results = []

    try:
        with open(filepath, 'r') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: File not found: {filepath}", file=sys.stderr)
        sys.exit(2)
    except IOError as e:
        print(f"Error reading file: {e}", file=sys.stderr)
        sys.exit(2)

    # Pattern for benchmark results
    # Example: "string_get             time:   [3.1234 µs 3.1700 µs 3.2156 µs]"
    time_pattern = re.compile(
        r'^(\S+)\s+time:\s+\[([\d.]+\s*\w+)\s+([\d.]+\s*\w+)\s+([\d.]+\s*\w+)\]',
        re.MULTILINE
    )

    # Pattern for change detection
    # Example: "change: [-2.1234% -1.5000% -0.8766%] (p = 0.00 < 0.05)"
    change_pattern = re.compile(
        r'change:\s+\[([+-]?\d+\.?\d*)%\s+([+-]?\d+\.?\d*)%\s+([+-]?\d+\.?\d*)%\]',
        re.MULTILINE
    )

    # Pattern for regression detection
    regression_pattern = re.compile(r'Performance has regressed', re.MULTILINE)

    # Split by benchmark sections
    sections = re.split(r'\n(?=\S+\s+time:)', content)

    for section in sections:
        time_match = time_pattern.search(section)
        if time_match:
            name = time_match.group(1)
            median_time = parse_time(time_match.group(3))  # Use median (middle value)

            result = BenchmarkResult(name=name, time_ns=median_time)

            # Check for change percentage
            change_match = change_pattern.search(section)
            if change_match:
                result.change_percent = float(change_match.group(2))  # Use median change

            # Check for regression flag
            if regression_pattern.search(section):
                result.regression = True

            results.append(result)

    return results


def check_regressions(results: list[BenchmarkResult], threshold: float) -> list[BenchmarkResult]:
    """Check for regressions exceeding threshold."""
    regressions = []

    for result in results:
        if result.change_percent is not None and result.change_percent > threshold:
            result.regression = True
            regressions.append(result)
        elif result.regression:
            regressions.append(result)

    return regressions


def format_time(ns: float) -> str:
    """Format nanoseconds to human-readable string."""
    if ns >= 1_000_000_000:
        return f"{ns / 1_000_000_000:.2f} s"
    elif ns >= 1_000_000:
        return f"{ns / 1_000_000:.2f} ms"
    elif ns >= 1000:
        return f"{ns / 1000:.2f} µs"
    else:
        return f"{ns:.2f} ns"


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(2)

    filepath = sys.argv[1]
    threshold = float(sys.argv[2])

    print(f"Checking for regressions > {threshold}% in {filepath}")
    print("-" * 60)

    results = parse_benchmark_output(filepath)

    if not results:
        print("Warning: No benchmark results found in output file")
        sys.exit(0)

    print(f"Found {len(results)} benchmark results\n")

    # Print all results
    print("Benchmark Results:")
    print("-" * 60)
    for result in results:
        time_str = format_time(result.time_ns)
        change_str = f"{result.change_percent:+.2f}%" if result.change_percent else "N/A"
        status = "REGRESSION" if result.regression else "OK"
        print(f"  {result.name:40} {time_str:>12} {change_str:>10} [{status}]")

    print()

    # Check for regressions
    regressions = check_regressions(results, threshold)

    if regressions:
        print(f"FAILED: {len(regressions)} regression(s) detected exceeding {threshold}% threshold:")
        print("-" * 60)
        for result in regressions:
            change_str = f"{result.change_percent:+.2f}%" if result.change_percent else "flagged"
            print(f"  {result.name}: {change_str}")
        print()
        sys.exit(1)
    else:
        print(f"PASSED: No regressions exceeding {threshold}% threshold")
        sys.exit(0)


if __name__ == "__main__":
    main()
