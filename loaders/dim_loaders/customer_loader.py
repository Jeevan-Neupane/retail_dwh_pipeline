"""
loaders/dim_loaders/customer_loader.py

SCD2 loader for TGT_D_CUSTOMER.

Natural key: CUSTOMER_ID

Tracked changes (either triggers a new SCD2 row + expiry of the old one):
  - CUSTOMER_NAME  — customer renamed
  - SEGMENT_KEY    — customer moved to a different segment

Extra steps not present in simpler dims
----------------------------------------
* _validate_fk_coverage(): before staging, confirms every SEGMENT_NAME
  in the stage view resolves to an active record in TGT_D_SEGMENT.
  Logs any dangling segment names so data quality issues are visible
  rather than silently lost via the INNER JOIN in _insert_sql.

* _change_metrics(): after expire and before insert, queries the temp
  table to split incoming rows into "changed" vs "brand new" buckets
  and logs the counts — useful for monitoring and incremental audits.

Run standalone:
    python -m loaders.dim_loaders.customer_loader
"""

from loaders.dim_loaders.scd2_loader import Scd2DimLoader
from utils.db_connector import SnowflakeSession


class CustomerLoader(Scd2DimLoader):
    STAGE_VIEW = "VW_D_CUSTOMER"
    TMP_TABLE  = "TMP_D_CUSTOMER"
    TGT_TABLE  = "TGT_D_CUSTOMER"

    def __init__(self):
        super().__init__("customer_loader")

    # ------------------------------------------------------------------
    # Extra validation step
    # ------------------------------------------------------------------

    def _validate_fk_coverage(self, sf: SnowflakeSession) -> None:
        """
        Warn if any SEGMENT_NAME in the stage view has no matching active
        record in TGT_D_SEGMENT.  These customers would be silently
        dropped by the INNER JOIN in _insert_sql.
        """
        sql = f"""
            SELECT DISTINCT stg.SEGMENT_NAME
            FROM   {sf.stage}.{self.STAGE_VIEW}  stg
            WHERE  NOT EXISTS (
                SELECT 1
                FROM   {sf.target}.TGT_D_SEGMENT seg
                WHERE  seg.SEGMENT_NAME = stg.SEGMENT_NAME
                  AND  seg.IS_CURRENT   = TRUE
            )
        """
        orphan_rows = sf.fetch(sql)
        if orphan_rows:
            names = ", ".join(r[0] for r in orphan_rows)
            self.logger.warning(
                f"FK coverage gap — {len(orphan_rows)} SEGMENT_NAME(s) in "
                f"{self.STAGE_VIEW} have no active TGT_D_SEGMENT record: {names}. "
                "Customers with these segments will be skipped on insert."
            )
        else:
            self.logger.info("FK coverage OK — all segment names resolve to TGT_D_SEGMENT.")

    # ------------------------------------------------------------------
    # Change-detection metrics
    # ------------------------------------------------------------------

    def _change_metrics(self, sf: SnowflakeSession) -> None:
        """
        After staging, split incoming temp rows into:
          - changed   : CUSTOMER_ID already exists in target but something differs
          - brand_new : CUSTOMER_ID not yet seen in target at all
        Logs both counts for pipeline monitoring.
        """
        changed_sql = f"""
            SELECT COUNT(*)
            FROM   {sf.temp}.{self.TMP_TABLE}    tmp
            JOIN   {sf.target}.TGT_D_SEGMENT     seg
                ON seg.SEGMENT_NAME = tmp.SEGMENT_NAME
               AND seg.IS_CURRENT   = TRUE
            WHERE  EXISTS (
                SELECT 1
                FROM   {sf.target}.{self.TGT_TABLE} tgt
                WHERE  tgt.CUSTOMER_ID  = tmp.CUSTOMER_ID
                  AND  tgt.IS_CURRENT   = TRUE
                  AND (
                       tgt.CUSTOMER_NAME != tmp.CUSTOMER_NAME
                    OR tgt.SEGMENT_KEY   != seg.SEGMENT_KEY
                  )
            )
        """
        new_sql = f"""
            SELECT COUNT(*)
            FROM   {sf.temp}.{self.TMP_TABLE} tmp
            WHERE  NOT EXISTS (
                SELECT 1
                FROM   {sf.target}.{self.TGT_TABLE} tgt
                WHERE  tgt.CUSTOMER_ID = tmp.CUSTOMER_ID
            )
        """
        changed = sf.fetch(changed_sql)[0][0]
        brand_new = sf.fetch(new_sql)[0][0]
        self.logger.info(
            f"Change metrics — changed (will expire+reinsert): {changed:,}  "
            f"brand new: {brand_new:,}"
        )

    # ------------------------------------------------------------------
    # SQL steps
    # ------------------------------------------------------------------

    def _stage_sql(self, sf: SnowflakeSession) -> str:
        return f"""
            INSERT INTO {sf.temp}.{self.TMP_TABLE}
                (CUSTOMER_ID, CUSTOMER_NAME, SEGMENT_NAME)
            SELECT CUSTOMER_ID, CUSTOMER_NAME, SEGMENT_NAME
            FROM   {sf.stage}.{self.STAGE_VIEW}
        """

    def _expire_sql(self, sf: SnowflakeSession) -> str:
        return f"""
            UPDATE {sf.target}.{self.TGT_TABLE} tgt
            SET    tgt.EFF_END_DATE = CURRENT_DATE(),
                   tgt.IS_CURRENT   = FALSE
            WHERE  tgt.IS_CURRENT = TRUE
              AND  EXISTS (
                SELECT 1
                FROM   {sf.temp}.{self.TMP_TABLE}  tmp
                JOIN   {sf.target}.TGT_D_SEGMENT   seg
                    ON seg.SEGMENT_NAME = tmp.SEGMENT_NAME
                   AND seg.IS_CURRENT   = TRUE
                WHERE  tmp.CUSTOMER_ID   = tgt.CUSTOMER_ID
                  AND (
                       tmp.CUSTOMER_NAME != tgt.CUSTOMER_NAME
                    OR seg.SEGMENT_KEY   != tgt.SEGMENT_KEY
                  )
              )
        """

    def _insert_sql(self, sf: SnowflakeSession) -> str:
        return f"""
            INSERT INTO {sf.target}.{self.TGT_TABLE}
                (CUSTOMER_ID, CUSTOMER_NAME, SEGMENT_KEY,
                 EFF_START_DATE, EFF_END_DATE, IS_CURRENT)
            SELECT
                 tmp.CUSTOMER_ID
                ,tmp.CUSTOMER_NAME
                ,seg.SEGMENT_KEY
                ,CURRENT_DATE()
                ,'9999-12-31'::DATE
                ,TRUE
            FROM  {sf.temp}.{self.TMP_TABLE}  tmp
            JOIN  {sf.target}.TGT_D_SEGMENT   seg
               ON seg.SEGMENT_NAME = tmp.SEGMENT_NAME
              AND seg.IS_CURRENT   = TRUE
            WHERE NOT EXISTS (
                SELECT 1
                FROM   {sf.target}.{self.TGT_TABLE} tgt
                WHERE  tgt.CUSTOMER_ID   = tmp.CUSTOMER_ID
                  AND  tgt.CUSTOMER_NAME = tmp.CUSTOMER_NAME
                  AND  tgt.SEGMENT_KEY   = seg.SEGMENT_KEY
                  AND  tgt.IS_CURRENT    = TRUE
            )
        """

    # ------------------------------------------------------------------
    # Override run() to inject the extra steps into the sequence
    # ------------------------------------------------------------------

    def run(self, sf: SnowflakeSession) -> None:
        self._truncate_temp(sf)
        self._validate_fk_coverage(sf)   # ← extra: FK gap check
        self._stage(sf)
        self._change_metrics(sf)         # ← extra: changed vs new counts
        self._expire(sf)
        self._insert(sf)
        self._report_counts(sf)


if __name__ == "__main__":
    CustomerLoader().execute()
