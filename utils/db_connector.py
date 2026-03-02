"""
utils/db_connector.py

Provides `SnowflakeSession` — a context-manager wrapper around the
Snowflake Python connector.  Credentials are read exclusively from
environment variables (loaded from .env by python-dotenv), keeping
secrets out of source code.

Usage
-----
    from utils.db_connector import SnowflakeSession

    with SnowflakeSession(logger) as sf:
        rows = sf.fetch("SELECT CURRENT_TIMESTAMP()")
        sf.execute("TRUNCATE TABLE MY_SCHEMA.MY_TABLE")
"""

import os
from typing import Any

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

_REQUIRED_ENV_VARS = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
)


class SnowflakeSession:
    """
    Context manager for a Snowflake connection.

    Opens the connection on __enter__, closes it on __exit__ — even
    if an exception is raised inside the `with` block.
    """

    def __init__(self, logger):
        self.logger = logger
        self._conn = None
        self._cursor = None

        # Validate all required env vars are present before trying to connect
        missing = [v for v in _REQUIRED_ENV_VARS if not os.getenv(v)]
        if missing:
            raise EnvironmentError(
                f"Missing required environment variables: {', '.join(missing)}. "
                "Copy .env.example to .env and fill in your credentials."
            )

        # Expose schema names as attributes so loaders can reference them
        self.landing = os.getenv("LANDING_SCHEMA", "LANDING")
        self.stage = os.getenv("STAGE_SCHEMA", "STAGE")
        self.temp = os.getenv("TEMP_SCHEMA", "TEMP")
        self.target = os.getenv("TARGET_SCHEMA", "TARGET")
        self.file_stage = os.getenv("FILE_STAGE", "CSV_STAGE")

    # ------------------------------------------------------------------
    # Context-manager protocol
    # ------------------------------------------------------------------

    def __enter__(self) -> "SnowflakeSession":
        self.logger.info("Opening Snowflake connection ...")
        self._conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            client_telemetry_enabled=False,
        )
        self._cursor = self._conn.cursor()
        self.logger.info("Snowflake connection established.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.logger.error(f"Session ended with error: {exc_val}")
        if self._cursor:
            self._cursor.close()
        if self._conn:
            self._conn.close()
        self.logger.info("Snowflake connection closed.")
        return False  # re-raise any exception

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def execute(self, sql: str, params: Any = None) -> None:
        """
        Execute *sql* without returning results (DDL, DML, TRUNCATE, etc.).

        Parameters
        ----------
        sql : str
            SQL statement to execute.
        params : sequence or None
            Bind parameters (positional %s).
        """
        try:
            self.logger.debug(f"EXECUTE >> {sql.strip()}")
            if params:
                self._cursor.execute(sql, params)
            else:
                self._cursor.execute(sql)
        except Exception as exc:
            self.logger.error(f"Query failed: {exc}")
            raise

    def fetch(self, sql: str, params: Any = None) -> list:
        """
        Execute *sql* and return all rows as a list of tuples.

        Parameters
        ----------
        sql : str
            SELECT statement.
        params : sequence or None
            Bind parameters.

        Returns
        -------
        list of tuples
        """
        try:
            self.logger.debug(f"FETCH   >> {sql.strip()}")
            if params:
                self._cursor.execute(sql, params)
            else:
                self._cursor.execute(sql)
            rows = self._cursor.fetchall()
            self.logger.debug(f"Rows returned: {len(rows)}")
            return rows
        except Exception as exc:
            self.logger.error(f"Query failed: {exc}")
            raise

    def executemany(self, sql: str, params_list: list) -> None:
        """
        Execute *sql* for each parameter set in *params_list*.
        Useful for bulk inserts via Python.

        Parameters
        ----------
        sql : str
            Parameterised SQL statement.
        params_list : list of sequences
        """
        try:
            self.logger.debug(
                f"EXECUTEMANY >> {sql.strip()} | batch size: {len(params_list)}"
            )
            self._cursor.executemany(sql, params_list)
        except Exception as exc:
            self.logger.error(f"Batch query failed: {exc}")
            raise
