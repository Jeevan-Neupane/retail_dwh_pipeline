"""
run_pipeline.py

Pipeline orchestrator for the retail_dwh_pipeline.

Runs all loaders in strict dependency order using a single shared
SnowflakeSession (one connection for the entire pipeline rather than
one per loader).  After every step, records elapsed time and
pass/fail status, then prints a coloured summary table at the end.

Execution order
---------------
1.  Extract       — CSV → LANDING.RAW_SALES
2.  Country       — geography root
3.  Region        — depends on Country
4.  State         — depends on Region
5.  City          — depends on State
6.  Category      — product hierarchy root
7.  Sub-Category  — depends on Category
8.  Segment       — customer classification
9.  Ship Mode     — shipping method reference
10. Product       — depends on Sub-Category
11. Customer      — depends on Segment
12. Sales (fact)  — depends on all dims above

Usage
-----
    python run_pipeline.py              # full pipeline
    python run_pipeline.py --from city  # resume from a specific step
    python run_pipeline.py --list       # print step names and exit
"""

import argparse
import sys
import time

from utils.db_connector import SnowflakeSession
from utils.logger import get_logger
from loaders.extract_loader import ExtractLoader
from loaders.dim_loaders.country_loader import CountryLoader
from loaders.dim_loaders.region_loader import RegionLoader
from loaders.dim_loaders.state_loader import StateLoader
from loaders.dim_loaders.city_loader import CityLoader
from loaders.dim_loaders.category_loader import CategoryLoader
from loaders.dim_loaders.subcategory_loader import SubcategoryLoader
from loaders.dim_loaders.segment_loader import SegmentLoader
from loaders.dim_loaders.ship_mode_loader import ShipModeLoader
from loaders.dim_loaders.product_loader import ProductLoader
from loaders.dim_loaders.customer_loader import CustomerLoader
from loaders.fact_loaders.sales_loader import SalesLoader


# ---------------------------------------------------------------------------
# Step registry — single source of truth for order and names
# ---------------------------------------------------------------------------

STEPS: list[tuple[str, object]] = [
    ("extract",      ExtractLoader),
    ("country",      CountryLoader),
    ("region",       RegionLoader),
    ("state",        StateLoader),
    ("city",         CityLoader),
    ("category",     CategoryLoader),
    ("subcategory",  SubcategoryLoader),
    ("segment",      SegmentLoader),
    ("ship_mode",    ShipModeLoader),
    ("product",      ProductLoader),
    ("customer",     CustomerLoader),
    ("sales",        SalesLoader),
]

STEP_NAMES = [name for name, _ in STEPS]


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

class Pipeline:
    """
    Orchestrates all loaders in dependency order over a single connection.

    Parameters
    ----------
    start_from : str | None
        If given, skip all steps before this step name (inclusive resume).
    """

    def __init__(self, start_from: str | None = None):
        self.logger = get_logger("pipeline")
        self.start_from = start_from
        self._results: list[dict] = []   # {step, status, elapsed_s, error}

    def _header(self, total: int) -> None:
        self.logger.info("=" * 65)
        self.logger.info("  RETAIL DWH PIPELINE  —  START")
        self.logger.info(f"  Steps to run: {total}")
        self.logger.info("=" * 65)

    def _footer(self) -> None:
        self.logger.info("=" * 65)
        self.logger.info("  PIPELINE SUMMARY")
        self.logger.info("=" * 65)
        total_elapsed = sum(r["elapsed_s"] for r in self._results)
        failed_steps  = [r for r in self._results if r["status"] == "FAILED"]

        col_w = max(len(r["step"]) for r in self._results) + 2
        for r in self._results:
            status_tag = "OK  " if r["status"] == "OK" else "FAIL"
            elapsed    = f"{r['elapsed_s']:6.1f}s"
            self.logger.info(
                f"  [{status_tag}]  {r['step']:<{col_w}}  {elapsed}"
                + (f"  ← {r['error']}" if r["error"] else "")
            )

        self.logger.info("-" * 65)
        self.logger.info(
            f"  Total: {len(self._results)} step(s)  |  "
            f"Passed: {len(self._results) - len(failed_steps)}  |  "
            f"Failed: {len(failed_steps)}  |  "
            f"Elapsed: {total_elapsed:.1f}s"
        )
        self.logger.info("=" * 65)

        if failed_steps:
            self.logger.error(
                f"{len(failed_steps)} step(s) failed: "
                + ", ".join(r["step"] for r in failed_steps)
            )

    def run(self) -> bool:
        """
        Run the pipeline.  Returns True if all steps passed, False otherwise.
        Stops at the first failure to avoid loading inconsistent data into
        downstream tables.
        """
        # Filter steps if --from was supplied
        steps = STEPS
        if self.start_from:
            if self.start_from not in STEP_NAMES:
                self.logger.error(
                    f"Unknown step name '{self.start_from}'. "
                    f"Valid names: {', '.join(STEP_NAMES)}"
                )
                return False
            idx   = STEP_NAMES.index(self.start_from)
            steps = STEPS[idx:]

        self._header(len(steps))
        pipeline_ok = True

        with SnowflakeSession(self.logger) as sf:
            for step_name, LoaderClass in steps:
                loader = LoaderClass()
                self.logger.info(f"--- Step: {step_name.upper()} ---")
                t0 = time.perf_counter()
                error_msg = None
                try:
                    loader.run(sf)
                    status = "OK"
                except Exception as exc:
                    status    = "FAILED"
                    error_msg = str(exc)
                    self.logger.error(
                        f"Step '{step_name}' failed: {exc}. "
                        "Pipeline halted — fix the issue and re-run "
                        f"with --from {step_name}"
                    )
                    pipeline_ok = False

                elapsed = time.perf_counter() - t0
                self._results.append({
                    "step":      step_name,
                    "status":    status,
                    "elapsed_s": elapsed,
                    "error":     error_msg,
                })

                if not pipeline_ok:
                    break

        self._footer()
        return pipeline_ok


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the retail_dwh_pipeline end-to-end ETL."
    )
    parser.add_argument(
        "--from",
        dest="start_from",
        metavar="STEP",
        help=(
            "Resume the pipeline from this step name, skipping all earlier "
            "steps.  Useful for re-running after a failure without re-loading "
            "already-completed layers."
        ),
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Print all step names in execution order and exit.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    if args.list:
        print("Pipeline steps (in execution order):")
        for i, name in enumerate(STEP_NAMES, 1):
            print(f"  {i:>2}. {name}")
        sys.exit(0)

    log = get_logger("pipeline_main")
    pipeline = Pipeline(start_from=args.start_from)
    success  = pipeline.run()
    sys.exit(0 if success else 1)
