"""
loaders/dim_loaders/subcategory_loader.py

SCD2 loader for TGT_D_SUBCATEGORY.

Tracked change: a sub-category can be re-classified under a different
parent CATEGORY_KEY. 

Expire condition: SUBCATEGORY_NAME matches but resolved CATEGORY_KEY differs.

Run standalone:
    python -m loaders.dim_loaders.subcategory_loader
"""

from loaders.dim_loaders.scd2_loader import Scd2DimLoader
from utils.db_connector import SnowflakeSession


class SubcategoryLoader(Scd2DimLoader):
    STAGE_VIEW = "VW_D_SUBCATEGORY"
    TMP_TABLE  = "TMP_D_SUBCATEGORY"
    TGT_TABLE  = "TGT_D_SUBCATEGORY"

    def __init__(self):
        super().__init__("subcategory_loader")

    def _stage_sql(self, sf: SnowflakeSession) -> str:
        return f"""
            INSERT INTO {sf.temp}.{self.TMP_TABLE}
                (SUBCATEGORY_NAME, CATEGORY_NAME)
            SELECT SUBCATEGORY_NAME, CATEGORY_NAME
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
                FROM   {sf.temp}.{self.TMP_TABLE}    tmp
                JOIN   {sf.target}.TGT_D_CATEGORY    cat
                    ON cat.CATEGORY_NAME = tmp.CATEGORY_NAME
                   AND cat.IS_CURRENT    = TRUE
                WHERE  tmp.SUBCATEGORY_NAME  = tgt.SUBCATEGORY_NAME
                  AND  cat.CATEGORY_KEY     != tgt.CATEGORY_KEY
              )
        """

    def _insert_sql(self, sf: SnowflakeSession) -> str:
        return f"""
            INSERT INTO {sf.target}.{self.TGT_TABLE}
                (SUBCATEGORY_NAME, CATEGORY_KEY,
                 EFF_START_DATE, EFF_END_DATE, IS_CURRENT)
            SELECT
                 tmp.SUBCATEGORY_NAME
                ,cat.CATEGORY_KEY
                ,CURRENT_DATE()
                ,'9999-12-31'::DATE
                ,TRUE
            FROM  {sf.temp}.{self.TMP_TABLE}   tmp
            JOIN  {sf.target}.TGT_D_CATEGORY   cat
               ON cat.CATEGORY_NAME = tmp.CATEGORY_NAME
              AND cat.IS_CURRENT    = TRUE
            WHERE NOT EXISTS (
                SELECT 1
                FROM   {sf.target}.{self.TGT_TABLE} tgt
                WHERE  tgt.SUBCATEGORY_NAME = tmp.SUBCATEGORY_NAME
                  AND  tgt.CATEGORY_KEY     = cat.CATEGORY_KEY
                  AND  tgt.IS_CURRENT       = TRUE
            )
        """


if __name__ == "__main__":
    SubcategoryLoader().execute()
