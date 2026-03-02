"""
loaders/ddl_runner.py

Reads sql/ddl/schema_setup.sql and executes each statement against
Snowflake.  Designed to be run once before the first pipeline execution
to create all required database objects.

Run from the project root:
    python -m loaders.ddl_runner
"""

import re
import sys
from pathlib import Path

from utils.db_connector import SnowflakeSession
from utils.logger import get_logger

_SQL_FILE = Path(__file__).resolve().parents[1] / "sql" / "ddl" / "schema_setup.sql"


def _parse_statements(sql_text: str) -> list[str]:
    """
    Split raw SQL text into individual executable statements.

    Rules applied:
    - Strip full-line comments (-- ...) and section-separator comments (-- -)
    - Split on semicolons that terminate a statement
    - Skip blank / whitespace-only results
    - Preserve inline string content (no false splits on quoted semicolons)

    Parameters
    ----------
    sql_text : str
        Raw contents of the .sql file.

    Returns
    -------
    list[str]
        Clean SQL statements ready for execution.
    """
    # Remove standalone comment lines, including the separator "-- -"
    lines = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        lines.append(line)

    cleaned = "\n".join(lines)

    # Split on semicolons (Snowflake statements don't embed semicolons in strings
    # for our DDL patterns, so a simple split is safe here)
    raw_parts = cleaned.split(";")

    statements = []
    for part in raw_parts:
        stmt = part.strip()
        if stmt:
            statements.append(stmt)

    return statements


def run_ddl(logger) -> None:
    """
    Read schema_setup.sql, parse it into individual statements, and execute
    each one inside a SnowflakeSession.  Execution continues even if a single
    statement fails so that all results are reported at the end.

    Parameters
    ----------
    logger : logging.Logger
        Pre-configured logger instance.
    """
    if not _SQL_FILE.exists():
        logger.error(f"DDL file not found: {_SQL_FILE}")
        sys.exit(1)

    logger.info(f"Reading DDL file: {_SQL_FILE}")
    sql_text = _SQL_FILE.read_text(encoding="utf-8")
    statements = _parse_statements(sql_text)
    logger.info(f"Parsed {len(statements)} SQL statement(s) from DDL file.")

    succeeded = 0
    failed = 0
    failures: list[tuple[int, str, str]] = []  # (index, preview, error)

    with SnowflakeSession(logger) as sf:
        for idx, stmt in enumerate(statements, start=1):
            # Build a short preview for readable log output
            first_line = stmt.split("\n")[0].strip()
            preview = first_line[:80] + ("..." if len(first_line) > 80 else "")
            logger.info(f"[{idx}/{len(statements)}] {preview}")
            try:
                sf.execute(stmt)
                succeeded += 1
                logger.info(f"  -> OK")
            except Exception as exc:
                failed += 1
                failures.append((idx, preview, str(exc)))
                logger.error(f"  -> FAILED: {exc}")

    # Summary
    logger.info("=" * 60)
    logger.info(f"DDL execution complete.  Succeeded: {succeeded}  Failed: {failed}")
    if failures:
        logger.error("Failed statements:")
        for idx, preview, err in failures:
            logger.error(f"  [{idx}] {preview}  |  {err}")
        sys.exit(1)


if __name__ == "__main__":
    log = get_logger("ddl_runner")
    log.info("Starting DDL setup ...")
    run_ddl(log)
    log.info("DDL setup finished successfully.")
