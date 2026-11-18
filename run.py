"""Run both solutions against both scenarios and print the timings.

Usage:
    uv run python run.py                          # all indices, default prefetch
    uv run python run.py --indices 20             # only the first 20 indices
    uv run python run.py --indices 20 --prefetch 4
"""

import argparse
from functools import partial

from solution import async_sol, pipeline_sol, sync_sol
from setup.problem import PROBLEMS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run both solutions against both scenarios.")
    parser.add_argument(
        "--prefetch",
        type=int,
        default=async_sol.DEFAULT_PREFETCH,
        help="async read-ahead depth (samples in flight); sync ignores it",
    )
    parser.add_argument(
        "--indices",
        type=int,
        default=None,
        metavar="N",
        help="number of indices to process (default: all)",
    )
    args = parser.parse_args()
    if args.prefetch < 1:
        parser.error("--prefetch must be at least 1")
    if args.indices is not None and args.indices < 1:
        parser.error("--indices must be at least 1")
    return args


def main() -> None:
    args = parse_args()

    solutions = {
        "sync": sync_sol.run,
        "async": partial(async_sol.run, prefetch=args.prefetch),
        "pipeline": partial(pipeline_sol.run, prefetch=args.prefetch),
    }

    header = f"{'scenario':<10} {'items':>5} {'solution':<8} {'total':>8} {'per item':>10} {'vs sync':>9}"
    print(f"prefetch={args.prefetch}")
    print(header)
    print("-" * len(header))

    for base in PROBLEMS:
        problem = base.with_length(args.indices) if args.indices else base
        baseline = 0.0
        for name, run in solutions.items():
            elapsed = run(problem)
            per_item = elapsed / len(problem.indices) * 1e3
            if name == "sync":
                baseline = elapsed
                speedup = "1.00x"
            else:
                speedup = f"{baseline / elapsed:.2f}x" if elapsed else "n/a"
            print(
                f"{problem.name:<10} {len(problem.indices):>5} {name:<8} "
                f"{elapsed:7.2f}s {per_item:8.1f}ms {speedup:>9}"
            )
        print()


if __name__ == "__main__":
    main()
