"""
loaders/dim_loaders/category_loader.py

SCD2 loader for TGT_D_CATEGORY.

CATEGORY_NAME is both the natural key and the only attribute — no FK
dependency.  Expire step is skipped; only new names are inserted.

Run standalone:
    python -m loaders.dim_loaders.category_loader
"""

from loaders.dim_loaders.scd2_loader import Scd2DimLoader
from utils.db_connector import SnowflakeSession


class CategoryLoader(Scd2DimLoader):
    STAGE_VIEW = "VW_D_CATEGORY"
    TMP_TABLE  = "TMP_D_CATEGORY"
    TGT_TABLE  = "TGT_D_CATEGORY"

    def __init__(self):
        super().__init__("category_loader")

    def _stage_sql(self, sf: SnowflakeSession) -> str:
        return f"""
            INSERT INTO {sf.temp}.{self.TMP_TABLE} (CATEGORY_NAME)
            SELECT CATEGORY_NAME
            FROM   {sf.stage}.{self.STAGE_VIEW}
        """

    def _insert_sql(self, sf: SnowflakeSession) -> str:
        return f"""
            INSERT INTO {sf.target}.{self.TGT_TABLE}
                (CATEGORY_NAME, EFF_START_DATE, EFF_END_DATE, IS_CURRENT)
            SELECT
                 tmp.CATEGORY_NAME
                ,CURRENT_DATE()
                ,'9999-12-31'::DATE
                ,TRUE
            FROM {sf.temp}.{self.TMP_TABLE} tmp
            WHERE NOT EXISTS (
                SELECT 1
                FROM   {sf.target}.{self.TGT_TABLE} tgt
                WHERE  tgt.CATEGORY_NAME = tmp.CATEGORY_NAME
                  AND  tgt.IS_CURRENT    = TRUE
            )
        """


if __name__ == "__main__":
    CategoryLoader().execute()
