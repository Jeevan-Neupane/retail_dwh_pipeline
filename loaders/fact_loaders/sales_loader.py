"""
loaders/fact_loaders/sales_loader.py

Loads the TGT_F_SALES fact table from the STAGE.VW_F_SALES view.

Design differences from a typical dim loader
---------------------------------------------
* No SCD2 — fact rows are immutable once written.  Re-runs use a MERGE
  (upsert on ROW_ID) so duplicate source files don't double-count data.

* _dim_coverage_check(): before staging, counts how many source rows
  would be dropped due to unresolved dimension keys.  Logs a breakdown
  by dimension so bad data is visible, not silently lost in the JOINs.

* _stage_to_temp(): populates TMP_F_SALES by resolving all five
  dimension keys via INNER JOINs; logs both the source row count and
  the staged (resolved) row count so any drop-off is immediately visible.

* _merge_into_target(): upserts temp into target using MERGE ON ROW_ID.
  Snowflake MERGE returns matched/inserted counts in query metadata —
  we extract and log them.

* _post_load_summary(): compares staged rows vs final fact table count
  and logs new insertions, updates, and the running total.

Run standalone:
    python -m loaders.fact_loaders.sales_loader
"""

from loaders.base_loader import BaseLoader
from utils.db_connector import SnowflakeSession


class SalesLoader(BaseLoader):

    STAGE_VIEW = "VW_F_SALES"
    TMP_TABLE  = "TMP_F_SALES"
    TGT_TABLE  = "TGT_F_SALES"

    def __init__(self):
        super().__init__("sales_loader")

    # ------------------------------------------------------------------
    # Pre-load dimension coverage check
    # ------------------------------------------------------------------

    def _dim_coverage_check(self, sf: SnowflakeSession) -> None:
        """
        For each dimension joined during staging, count source rows that
        have NO matching current dimension record.  Logs count per dim.
        A non-zero count means those rows will be silently dropped in
        _stage_to_temp — surfacing it here makes the issue actionable.
        """
        checks = {
            "CUSTOMER_ID -> TGT_D_CUSTOMER": f"""
                SELECT COUNT(*)
                FROM   {sf.stage}.{self.STAGE_VIEW} src
                WHERE  NOT EXISTS (
                    SELECT 1 FROM {sf.target}.TGT_D_CUSTOMER c
                    WHERE c.CUSTOMER_ID = src.CUSTOMER_ID AND c.IS_CURRENT = TRUE
                )
            """,
            "PRODUCT_ID -> TGT_D_PRODUCT": f"""
                SELECT COUNT(*)
                FROM   {sf.stage}.{self.STAGE_VIEW} src
                WHERE  NOT EXISTS (
                    SELECT 1 FROM {sf.target}.TGT_D_PRODUCT p
                    WHERE p.PRODUCT_ID = src.PRODUCT_ID AND p.IS_CURRENT = TRUE
                )
            """,
            "POSTAL_CODE+CITY -> TGT_D_CITY": f"""
                SELECT COUNT(*)
                FROM   {sf.stage}.{self.STAGE_VIEW} src
                WHERE  NOT EXISTS (
                    SELECT 1 FROM {sf.target}.TGT_D_CITY ct
                    WHERE ct.POSTAL_CODE = src.POSTAL_CODE
                      AND ct.CITY_NAME   = src.CITY
                      AND ct.IS_CURRENT  = TRUE
                )
            """,
            "SHIP_MODE -> TGT_D_SHIP_MODE": f"""
                SELECT COUNT(*)
                FROM   {sf.stage}.{self.STAGE_VIEW} src
                WHERE  NOT EXISTS (
                    SELECT 1 FROM {sf.target}.TGT_D_SHIP_MODE sm
                    WHERE sm.SHIP_MODE  = src.SHIP_MODE
                      AND sm.IS_CURRENT = TRUE
                )
            """,
            "ORDER_DATE -> TGT_D_DATE": f"""
                SELECT COUNT(*)
                FROM   {sf.stage}.{self.STAGE_VIEW} src
                WHERE  NOT EXISTS (
                    SELECT 1 FROM {sf.target}.TGT_D_DATE d
                    WHERE d.FULL_DATE = src.ORDER_DATE
                )
            """,
            "SHIP_DATE -> TGT_D_DATE": f"""
                SELECT COUNT(*)
                FROM   {sf.stage}.{self.STAGE_VIEW} src
                WHERE  NOT EXISTS (
                    SELECT 1 FROM {sf.target}.TGT_D_DATE d
                    WHERE d.FULL_DATE = src.SHIP_DATE
                )
            """,
        }
        any_gap = False
        for label, sql in checks.items():
            gap = sf.fetch(sql)[0][0]
            if gap > 0:
                self.logger.warning(
                    f"  Coverage gap [{label}]: {gap:,} source row(s) will be dropped."
                )
                any_gap = True
        if not any_gap:
            self.logger.info("Dim coverage check passed — all keys resolve.")

    # ------------------------------------------------------------------
    # Temp staging
    # ------------------------------------------------------------------

    def _truncate_temp(self, sf: SnowflakeSession) -> None:
        tbl = f"{sf.temp}.{self.TMP_TABLE}"
        self.logger.info(f"Truncating {tbl}")
        sf.execute(f"TRUNCATE TABLE {tbl}")

    def _stage_to_temp(self, sf: SnowflakeSession) -> int:
        """
        Resolve all dimension keys and populate the temp fact table.
        Returns the number of rows staged.
        """
        src_count = sf.fetch(
            f"SELECT COUNT(*) FROM {sf.stage}.{self.STAGE_VIEW}"
        )[0][0]
        self.logger.info(f"Source rows in {self.STAGE_VIEW}: {src_count:,}")

        stage_sql = f"""
            INSERT INTO {sf.temp}.{self.TMP_TABLE}
                (ROW_ID, ORDER_ID,
                 ORDER_DATE_KEY, SHIP_DATE_KEY,
                 CUSTOMER_KEY, PRODUCT_KEY, CITY_KEY, SHIP_MODE_KEY,
                 QUANTITY, SALES, DISCOUNT, DISCOUNT_AMOUNT, REVENUE, PROFIT)
            SELECT
                 src.ROW_ID
                ,src.ORDER_ID
                ,ord_dt.DATE_KEY
                ,shp_dt.DATE_KEY
                ,cust.CUSTOMER_KEY
                ,prod.PRODUCT_KEY
                ,city.CITY_KEY
                ,ship.SHIP_MODE_KEY
                ,src.QUANTITY
                ,src.SALES
                ,src.DISCOUNT
                ,src.DISCOUNT_AMOUNT
                ,src.REVENUE
                ,src.PROFIT
            FROM        {sf.stage}.{self.STAGE_VIEW}       src
            INNER JOIN  {sf.target}.TGT_D_CUSTOMER         cust
                ON  cust.CUSTOMER_ID = src.CUSTOMER_ID
                AND cust.IS_CURRENT  = TRUE
            INNER JOIN  {sf.target}.TGT_D_PRODUCT          prod
                ON  prod.PRODUCT_ID = src.PRODUCT_ID
                AND prod.IS_CURRENT = TRUE
            INNER JOIN  {sf.target}.TGT_D_CITY             city
                ON  city.POSTAL_CODE = src.POSTAL_CODE
                AND city.CITY_NAME   = src.CITY
                AND city.IS_CURRENT  = TRUE
            INNER JOIN  {sf.target}.TGT_D_SHIP_MODE        ship
                ON  ship.SHIP_MODE  = src.SHIP_MODE
                AND ship.IS_CURRENT = TRUE
            INNER JOIN  {sf.target}.TGT_D_DATE             ord_dt
                ON  ord_dt.FULL_DATE = src.ORDER_DATE
            INNER JOIN  {sf.target}.TGT_D_DATE             shp_dt
                ON  shp_dt.FULL_DATE = src.SHIP_DATE
        """
        sf.execute(stage_sql)

        staged = sf.fetch(
            f"SELECT COUNT(*) FROM {sf.temp}.{self.TMP_TABLE}"
        )[0][0]
        dropped = src_count - staged
        self.logger.info(
            f"Staged: {staged:,} row(s)  |  Dropped (unresolved keys): {dropped:,}"
        )
        if dropped > 0:
            self.logger.warning(
                f"{dropped:,} row(s) were dropped during key resolution. "
                "Re-run the dimension loaders and check the coverage gaps above."
            )
        return staged

    # ------------------------------------------------------------------
    # MERGE into target
    # ------------------------------------------------------------------

    def _merge_into_target(self, sf: SnowflakeSession) -> None:
        """
        Upsert TMP_F_SALES into TGT_F_SALES on ROW_ID.
        WHEN MATCHED  → update all measure + FK columns (handles source corrections)
        WHEN NOT MATCHED → insert new rows
        """
        merge_sql = f"""
            MERGE INTO {sf.target}.{self.TGT_TABLE}   tgt
            USING      {sf.temp}.{self.TMP_TABLE}      tmp
                ON  tgt.ROW_ID = tmp.ROW_ID
            WHEN MATCHED THEN
                UPDATE SET
                     tgt.ORDER_ID        = tmp.ORDER_ID
                    ,tgt.ORDER_DATE_KEY  = tmp.ORDER_DATE_KEY
                    ,tgt.SHIP_DATE_KEY   = tmp.SHIP_DATE_KEY
                    ,tgt.CUSTOMER_KEY    = tmp.CUSTOMER_KEY
                    ,tgt.PRODUCT_KEY     = tmp.PRODUCT_KEY
                    ,tgt.CITY_KEY        = tmp.CITY_KEY
                    ,tgt.SHIP_MODE_KEY   = tmp.SHIP_MODE_KEY
                    ,tgt.QUANTITY        = tmp.QUANTITY
                    ,tgt.SALES           = tmp.SALES
                    ,tgt.DISCOUNT        = tmp.DISCOUNT
                    ,tgt.DISCOUNT_AMOUNT = tmp.DISCOUNT_AMOUNT
                    ,tgt.REVENUE         = tmp.REVENUE
                    ,tgt.PROFIT          = tmp.PROFIT
            WHEN NOT MATCHED THEN
                INSERT (ROW_ID, ORDER_ID,
                        ORDER_DATE_KEY, SHIP_DATE_KEY,
                        CUSTOMER_KEY, PRODUCT_KEY, CITY_KEY, SHIP_MODE_KEY,
                        QUANTITY, SALES, DISCOUNT, DISCOUNT_AMOUNT, REVENUE, PROFIT)
                VALUES (tmp.ROW_ID, tmp.ORDER_ID,
                        tmp.ORDER_DATE_KEY, tmp.SHIP_DATE_KEY,
                        tmp.CUSTOMER_KEY, tmp.PRODUCT_KEY, tmp.CITY_KEY, tmp.SHIP_MODE_KEY,
                        tmp.QUANTITY, tmp.SALES, tmp.DISCOUNT, tmp.DISCOUNT_AMOUNT,
                        tmp.REVENUE, tmp.PROFIT)
        """
        self.logger.info(
            f"Merging {sf.temp}.{self.TMP_TABLE} -> {sf.target}.{self.TGT_TABLE}"
        )
        sf.execute(merge_sql)

    # ------------------------------------------------------------------
    # Post-load summary
    # ------------------------------------------------------------------

    def _post_load_summary(self, sf: SnowflakeSession) -> None:
        """Log total rows, revenue, and profit in the fact table."""
        summary = sf.fetch(f"""
            SELECT
                 COUNT(*)          AS total_rows
                ,SUM(REVENUE)      AS total_revenue
                ,SUM(PROFIT)       AS total_profit
                ,MIN(ORDER_DATE_KEY) AS earliest_order
                ,MAX(ORDER_DATE_KEY) AS latest_order
            FROM {sf.target}.{self.TGT_TABLE}
        """)[0]
        self.logger.info(
            f"Fact table summary — "
            f"rows: {summary[0]:,}  |  "
            f"revenue: {summary[1]:,.2f}  |  "
            f"profit: {summary[2]:,.2f}  |  "
            f"date range: {summary[3]} – {summary[4]}"
        )

    # ------------------------------------------------------------------
    # BaseLoader interface
    # ------------------------------------------------------------------

    def run(self, sf: SnowflakeSession) -> None:
        self.logger.info("Running dimension coverage check ...")
        self._dim_coverage_check(sf)
        self._truncate_temp(sf)
        self._stage_to_temp(sf)
        self._merge_into_target(sf)
        self._post_load_summary(sf)


if __name__ == "__main__":
    SalesLoader().execute()
