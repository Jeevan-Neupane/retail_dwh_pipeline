"""
loaders/extract_loader.py

Extracts raw sales data from a Snowflake internal stage into the
LANDING.RAW_SALES table.

Steps
-----
1. Pre-flight: list the stage and verify at least one CSV file is present.
2. Truncate LANDING.RAW_SALES so each pipeline run starts from a clean slate.
3. COPY INTO the landing table, using ON_ERROR = 'CONTINUE' so partial files
   are still ingested; any row-level errors are logged rather than aborting.
4. Post-load: query and log the final row count for observability.

Run standalone:
    python -m loaders.extract_loader
"""

from loaders.base_loader import BaseLoader
from utils.db_connector import SnowflakeSession


class ExtractLoader(BaseLoader):
    """
    Loads raw CSV data from the Snowflake internal stage into the
    landing layer.

    Parameters
    ----------
    landing_table : str, optional
        Override the target landing table name (default ``"RAW_SALES"``).
    """

    LANDING_TABLE = "RAW_SALES"

    def __init__(self, landing_table: str = LANDING_TABLE):
        super().__init__("extract_loader")
        self.landing_table = landing_table

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _preflight_check(self, sf: SnowflakeSession) -> int:
        """
        List the internal stage and return the number of files found.
        Raises RuntimeError if no files are present — fail fast rather
        than silently loading zero rows.

        Parameters
        ----------
        sf : SnowflakeSession

        Returns
        -------
        int
            Number of staged files detected.
        """
        stage_path = f"@{sf.landing}.{sf.file_stage}"
        self.logger.info(f"Checking stage for files: {stage_path}")

        rows = sf.fetch(f"LIST {stage_path}")
        file_count = len(rows)

        if file_count == 0:
            raise RuntimeError(
                f"No files found in stage {stage_path}. "
                "Upload the CSV before running the pipeline.\n"
                "  PUT file://path/to/sales.csv "
                f"@RETAIL_DB.{sf.landing}.{sf.file_stage};"
            )

        self.logger.info(f"Pre-flight OK — {file_count} file(s) found in stage:")
        for row in rows:
            # LIST returns: name, size, md5, last_modified
            self.logger.info(f"  {row[0]}  ({row[1]:,} bytes)")

        return file_count

    def _truncate_landing(self, sf: SnowflakeSession) -> None:
        """Truncate the landing table before loading fresh data."""
        target = f"{sf.landing}.{self.landing_table}"
        self.logger.info(f"Truncating {target} ...")
        sf.execute(f"TRUNCATE TABLE {target}")
        self.logger.info("Truncate complete.")

    def _copy_into_landing(self, sf: SnowflakeSession) -> dict:
        """
        Execute COPY INTO and return a summary dict with keys:
        ``rows_loaded``, ``rows_parsed``, ``errors_seen``.

        ON_ERROR = CONTINUE means bad rows are skipped rather than aborting
        the entire load; errors are captured in the COPY result metadata.
        """
        target = f"{sf.landing}.{self.landing_table}"
        stage_path = f"@{sf.landing}.{sf.file_stage}"

        copy_sql = f"""
            COPY INTO {target}
            FROM {stage_path}
            FILE_FORMAT = (
                TYPE                         = CSV
                FIELD_DELIMITER              = ','
                SKIP_HEADER                  = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                TRIM_SPACE                   = TRUE
                NULL_IF                      = ('NULL', 'null', '')
                EMPTY_FIELD_AS_NULL          = TRUE
            )
            ON_ERROR = 'CONTINUE'
            PURGE     = FALSE;
        """

        self.logger.info(f"Running COPY INTO {target} from {stage_path} ...")
        results = sf.fetch(copy_sql)

        # COPY INTO result columns (Snowflake doc):
        # file, status, rows_parsed, rows_loaded, error_limit,
        # errors_seen, first_error, first_error_line, ...
        total_parsed = 0
        total_loaded = 0
        total_errors = 0

        for row in results:
            file_name   = row[0]
            status      = row[1]
            rows_parsed = row[2] or 0
            rows_loaded = row[3] or 0
            errors_seen = row[5] or 0

            total_parsed += rows_parsed
            total_loaded += rows_loaded
            total_errors += errors_seen

            self.logger.info(
                f"  File: {file_name} | Status: {status} | "
                f"Parsed: {rows_parsed:,} | Loaded: {rows_loaded:,} | "
                f"Errors: {errors_seen:,}"
            )
            if errors_seen > 0:
                first_err = row[6]
                self.logger.warning(f"    First error: {first_err}")

        summary = {
            "rows_parsed": total_parsed,
            "rows_loaded": total_loaded,
            "errors_seen": total_errors,
        }
        return summary

    def _log_row_count(self, sf: SnowflakeSession) -> int:
        """Query and log the final row count of the landing table."""
        target = f"{sf.landing}.{self.landing_table}"
        rows = sf.fetch(f"SELECT COUNT(*) FROM {target}")
        count = rows[0][0] if rows else 0
        self.logger.info(f"Landing table {target} now contains {count:,} row(s).")
        return count

    # ------------------------------------------------------------------
    # BaseLoader interface
    # ------------------------------------------------------------------

    def run(self, sf: SnowflakeSession) -> None:
        """
        Full extract sequence: pre-flight → truncate → COPY → row count.

        Parameters
        ----------
        sf : SnowflakeSession
            Active Snowflake session supplied by the caller.
        """
        self._preflight_check(sf)
        self._truncate_landing(sf)
        summary = self._copy_into_landing(sf)

        self.logger.info(
            f"COPY summary — "
            f"parsed: {summary['rows_parsed']:,}  "
            f"loaded: {summary['rows_loaded']:,}  "
            f"errors: {summary['errors_seen']:,}"
        )

        if summary["errors_seen"] > 0:
            self.logger.warning(
                f"{summary['errors_seen']:,} row(s) were skipped due to errors. "
                "Check the logs above for details."
            )

        self._log_row_count(sf)


# ---------------------------------------------------------------------------
# Standalone entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    ExtractLoader().execute()
