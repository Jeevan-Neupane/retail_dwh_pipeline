"""
loaders/dim_loaders/product_loader.py

SCD2 loader for TGT_D_PRODUCT.

Natural key: PRODUCT_ID

Tracked changes (either triggers a new SCD2 row + expiry of the old one):
  - PRODUCT_NAME     — product renamed
  - SUBCATEGORY_KEY  — product re-classified under a different sub-category

Extra steps not present in simpler dims
----------------------------------------
* _validate_fk_coverage(): before staging, confirms every SUBCATEGORY_NAME
  in the stage view resolves to an active record in TGT_D_SUBCATEGORY.
  Logs any dangling sub-category names so data quality issues surface
  rather than being silently swallowed by the INNER JOIN in _insert_sql.

* _change_metrics(): after staging, counts rows that will trigger a new
  SCD2 version (changed) versus rows that refer to a completely new
  PRODUCT_ID (brand new).

* _duplicate_product_ids(): products are sometimes duplicated in the
  source with slightly different names or sub-category labels.
  This check warns when a PRODUCT_ID appears more than once in the
  temp table so analysts can investigate the source data.

Run standalone:
    python -m loaders.dim_loaders.product_loader
"""

from loaders.dim_loaders.scd2_loader import Scd2DimLoader
from utils.db_connector import SnowflakeSession


class ProductLoader(Scd2DimLoader):
    STAGE_VIEW = "VW_D_PRODUCT"
    TMP_TABLE  = "TMP_D_PRODUCT"
    TGT_TABLE  = "TGT_D_PRODUCT"

    def __init__(self):
        super().__init__("product_loader")

    # ------------------------------------------------------------------
    # Extra validation steps
    # ------------------------------------------------------------------

    def _validate_fk_coverage(self, sf: SnowflakeSession) -> None:
        """
        Warn if any SUBCATEGORY_NAME in the stage view has no matching
        active record in TGT_D_SUBCATEGORY.  These products would be
        silently dropped by the INNER JOIN in _insert_sql.
        """
        sql = f"""
            SELECT DISTINCT stg.SUBCATEGORY_NAME
            FROM   {sf.stage}.{self.STAGE_VIEW}  stg
            WHERE  NOT EXISTS (
                SELECT 1
                FROM   {sf.target}.TGT_D_SUBCATEGORY  sc
                WHERE  sc.SUBCATEGORY_NAME = stg.SUBCATEGORY_NAME
                  AND  sc.IS_CURRENT       = TRUE
            )
        """
        orphans = sf.fetch(sql)
        if orphans:
            names = ", ".join(r[0] for r in orphans)
            self.logger.warning(
                f"FK coverage gap — {len(orphans)} SUBCATEGORY_NAME(s) in "
                f"{self.STAGE_VIEW} have no active TGT_D_SUBCATEGORY record: {names}. "
                "Products in these sub-categories will be skipped on insert."
            )
        else:
            self.logger.info(
                "FK coverage OK — all sub-category names resolve to TGT_D_SUBCATEGORY."
            )

    def _duplicate_product_ids(self, sf: SnowflakeSession) -> None:
        """
        Warn if any PRODUCT_ID appears more than once in the temp table.
        Duplicates suggest source data quality issues that may create
        multiple SCD2 rows for the same product in one run.
        """
        sql = f"""
            SELECT PRODUCT_ID, COUNT(*) AS cnt
            FROM   {sf.temp}.{self.TMP_TABLE}
            GROUP  BY PRODUCT_ID
            HAVING COUNT(*) > 1
            ORDER  BY cnt DESC
            LIMIT  10
        """
        dupes = sf.fetch(sql)
        if dupes:
            self.logger.warning(
                f"Duplicate PRODUCT_IDs in temp table ({len(dupes)} distinct ID(s) "
                f"with count > 1). Top offenders:"
            )
            for pid, cnt in dupes:
                self.logger.warning(f"  PRODUCT_ID={pid}  count={cnt}")
        else:
            self.logger.info("Duplicate check OK — all PRODUCT_IDs are unique in temp.")

    # ------------------------------------------------------------------
    # Change-detection metrics
    # ------------------------------------------------------------------

    def _change_metrics(self, sf: SnowflakeSession) -> None:
        """
        Split incoming rows into 'changed' (existing ID, different attrs)
        vs 'brand new' (ID not yet in target) and log both counts.
        """
        changed_sql = f"""
            SELECT COUNT(*)
            FROM   {sf.temp}.{self.TMP_TABLE}       tmp
            JOIN   {sf.target}.TGT_D_SUBCATEGORY    sc
                ON sc.SUBCATEGORY_NAME = tmp.SUBCATEGORY_NAME
               AND sc.IS_CURRENT       = TRUE
            WHERE  EXISTS (
                SELECT 1
                FROM   {sf.target}.{self.TGT_TABLE} tgt
                WHERE  tgt.PRODUCT_ID      = tmp.PRODUCT_ID
                  AND  tgt.IS_CURRENT      = TRUE
                  AND (
                       tgt.PRODUCT_NAME    != tmp.PRODUCT_NAME
                    OR tgt.SUBCATEGORY_KEY != sc.SUBCATEGORY_KEY
                  )
            )
        """
        new_sql = f"""
            SELECT COUNT(*)
            FROM   {sf.temp}.{self.TMP_TABLE} tmp
            WHERE  NOT EXISTS (
                SELECT 1
                FROM   {sf.target}.{self.TGT_TABLE} tgt
                WHERE  tgt.PRODUCT_ID = tmp.PRODUCT_ID
            )
        """
        changed  = sf.fetch(changed_sql)[0][0]
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
                (PRODUCT_ID, PRODUCT_NAME, SUBCATEGORY_NAME)
            SELECT PRODUCT_ID, PRODUCT_NAME, SUBCATEGORY_NAME
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
                FROM   {sf.temp}.{self.TMP_TABLE}       tmp
                JOIN   {sf.target}.TGT_D_SUBCATEGORY    sc
                    ON sc.SUBCATEGORY_NAME = tmp.SUBCATEGORY_NAME
                   AND sc.IS_CURRENT       = TRUE
                WHERE  tmp.PRODUCT_ID      = tgt.PRODUCT_ID
                  AND (
                       tmp.PRODUCT_NAME    != tgt.PRODUCT_NAME
                    OR sc.SUBCATEGORY_KEY  != tgt.SUBCATEGORY_KEY
                  )
              )
        """

    def _insert_sql(self, sf: SnowflakeSession) -> str:
        return f"""
            INSERT INTO {sf.target}.{self.TGT_TABLE}
                (PRODUCT_ID, PRODUCT_NAME, SUBCATEGORY_KEY,
                 EFF_START_DATE, EFF_END_DATE, IS_CURRENT)
            SELECT
                 tmp.PRODUCT_ID
                ,tmp.PRODUCT_NAME
                ,sc.SUBCATEGORY_KEY
                ,CURRENT_DATE()
                ,'9999-12-31'::DATE
                ,TRUE
            FROM  {sf.temp}.{self.TMP_TABLE}       tmp
            JOIN  {sf.target}.TGT_D_SUBCATEGORY    sc
               ON sc.SUBCATEGORY_NAME = tmp.SUBCATEGORY_NAME
              AND sc.IS_CURRENT       = TRUE
            WHERE NOT EXISTS (
                SELECT 1
                FROM   {sf.target}.{self.TGT_TABLE} tgt
                WHERE  tgt.PRODUCT_ID      = tmp.PRODUCT_ID
                  AND  tgt.PRODUCT_NAME    = tmp.PRODUCT_NAME
                  AND  tgt.SUBCATEGORY_KEY = sc.SUBCATEGORY_KEY
                  AND  tgt.IS_CURRENT      = TRUE
            )
        """

    # ------------------------------------------------------------------
    # Override run() to inject the extra steps into the sequence
    # ------------------------------------------------------------------

    def run(self, sf: SnowflakeSession) -> None:
        self._truncate_temp(sf)
        self._validate_fk_coverage(sf)    # ← extra: FK gap check
        self._stage(sf)
        self._duplicate_product_ids(sf)   # ← extra: dupe PRODUCT_ID check
        self._change_metrics(sf)          # ← extra: changed vs new counts
        self._expire(sf)
        self._insert(sf)
        self._report_counts(sf)


if __name__ == "__main__":
    ProductLoader().execute()
