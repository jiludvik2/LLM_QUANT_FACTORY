"""Command-line entry point for Yahoo Finance EOD ingestion.

Kept separate from ``yahoo_source`` so importing the source registry never
re-executes CLI code and running the CLI never double-registers the source.

Usage::

    uv run python -m autoalpha.data.sources.yahoo_cli \\
        --root <data-root> --universe AAPL --universe LSE:SHEL.L --start 2020-01-01
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from autoalpha.data.sources.yahoo_source import run_yahoo_ingestion


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Ingest a Yahoo Finance EOD ticker universe (US/UK/EU) into the shared daily-panel "
            "layout. Research-grade only: capability ceiling RESEARCH_READY, no PIT claims."
        )
    )
    parser.add_argument("--root", type=Path, required=True, help="data workspace root directory")
    parser.add_argument(
        "--universe",
        action="append",
        required=True,
        metavar="TICKER|EXCHANGE:TICKER",
        help="ticker entry; repeat for each security (for example AAPL or LSE:SHEL.L)",
    )
    parser.add_argument("--start", default=None, help="start date YYYY-MM-DD (optional)")
    parser.add_argument("--end", default=None, help="end date YYYY-MM-DD (optional)")
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=(
            "replace an existing daily panel under --root; without this flag, ingestion "
            "refuses to run when a panel already exists so a smaller/different universe "
            "cannot silently drop previously ingested tickers or date ranges"
        ),
    )
    args = parser.parse_args()
    result = run_yahoo_ingestion(
        root=args.root.expanduser().resolve(),
        universe=args.universe,
        start=args.start,
        end=args.end,
        retries=args.retries,
        workers=args.workers,
        overwrite=args.overwrite,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if not result["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
