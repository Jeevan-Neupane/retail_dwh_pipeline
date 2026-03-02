"""
loaders/dim_loaders/scd2_loader.py

Template-method base class for SCD Type 2 dimension loaders.

Every dimension loader inherits from this class and provides the SQL for
its three logical steps by overriding abstract methods:

    _stage_sql()   — INSERT INTO <temp>  SELECT … FROM <stage view>
    _expire_sql()  — UPDATE <target>  SET IS_CURRENT = FALSE WHERE changed
    _insert_sql()  — INSERT INTO <target>  rows not already current

The orchestration sequence is fixed here in `run()` so there is no risk
of a loader accidentally skipping a step, and new loaders can be added
by writing ~30 lines of SQL without touching any control-flow logic.

Lifecycle per run
-----------------
1. _truncate_temp   — wipe the temp table
2. _stage           — populate temp from stage view
3. _expire          — close out changed records in target (SCD2 expire)
4. _insert          — insert new / changed records into target
5. _report_counts   — log before/after row counts for observability
"""

from abc import abstractmethod

from loaders.base_loader import BaseLoader
from utils.db_connector import SnowflakeSession


class Scd2DimLoader(BaseLoader):
    """
    Generic SCD2 dimension loader using the template method pattern.

    Sub-classes declare three class-level name attributes and override
    the SQL methods — that is all they need to do.

    Class attributes to set in each sub-class
    ------------------------------------------
    STAGE_VIEW : str   — name of the stage view  (inside STAGE schema)
    TMP_TABLE  : str   — name of the temp table  (inside TEMP schema)
    TGT_TABLE  : str   — name of the target table (inside TARGET schema)
    """

    STAGE_VIEW: str
    TMP_TABLE: str
    TGT_TABLE: str

    def __init__(self, loader_name: str):
        super().__init__(loader_name)

    # ------------------------------------------------------------------
    # Abstract SQL providers — each sub-class fills these in
    # ------------------------------------------------------------------

    @abstractmethod
    def _stage_sql(self, sf: SnowflakeSession) -> str:
        """
        Return the INSERT … SELECT statement that populates the temp
        table from the stage view.
        """

    @abstractmethod
    def _insert_sql(self, sf: SnowflakeSession) -> str:
        """
        Return the INSERT statement that adds new or changed rows to the
        target table.
        """

    def _expire_sql(self, sf: SnowflakeSession) -> str | None:
        """
        Return an UPDATE statement that expires (closes) stale SCD2 rows,
        or None if this dimension has no tracked attributes that can change
        (e.g. country, category, segment).

        Default: None (no expiry needed).
        """
        return None

    # ------------------------------------------------------------------
    # Fixed orchestration steps
    # ------------------------------------------------------------------

    def _truncate_temp(self, sf: SnowflakeSession) -> None:
        tbl = f"{sf.temp}.{self.TMP_TABLE}"
        self.logger.info(f"Truncating temp table {tbl}")
        sf.execute(f"TRUNCATE TABLE {tbl}")

    def _stage(self, sf: SnowflakeSession) -> None:
        sql = self._stage_sql(sf)
        self.logger.info(
            f"Staging {self.STAGE_VIEW} -> {sf.temp}.{self.TMP_TABLE}"
        )
        sf.execute(sql)
        rows = sf.fetch(f"SELECT COUNT(*) FROM {sf.temp}.{self.TMP_TABLE}")
        self.logger.info(f"  Staged {rows[0][0]:,} row(s) into temp.")

    def _expire(self, sf: SnowflakeSession) -> None:
        sql = self._expire_sql(sf)
        if sql is None:
            self.logger.info("Expire step skipped (no tracked attributes).")
            return
        self.logger.info(
            f"Expiring stale records in {sf.target}.{self.TGT_TABLE}"
        )
        sf.execute(sql)

    def _insert(self, sf: SnowflakeSession) -> None:
        sql = self._insert_sql(sf)
        self.logger.info(
            f"Inserting new/changed records into {sf.target}.{self.TGT_TABLE}"
        )
        sf.execute(sql)

    def _report_counts(self, sf: SnowflakeSession) -> None:
        tgt = f"{sf.target}.{self.TGT_TABLE}"
        total = sf.fetch(f"SELECT COUNT(*) FROM {tgt}")[0][0]
        current = sf.fetch(
            f"SELECT COUNT(*) FROM {tgt} WHERE IS_CURRENT = TRUE"
        )[0][0]
        self.logger.info(
            f"  {tgt} — total rows: {total:,}  |  current rows: {current:,}"
        )

    # ------------------------------------------------------------------
    # BaseLoader interface — fixed template, not overridden by sub-classes
    # ------------------------------------------------------------------

    def run(self, sf: SnowflakeSession) -> None:
        self._truncate_temp(sf)
        self._stage(sf)
        self._expire(sf)
        self._insert(sf)
        self._report_counts(sf)
