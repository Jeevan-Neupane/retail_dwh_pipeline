"""
loaders/dim_loaders/region_loader.py

SCD2 loader for TGT_D_REGION.

Tracked change: a region's COUNTRY_KEY can change if the same REGION_NAME
is reassigned to a different country (unusual but handled for completeness).

Expire condition: REGION_NAME matches but the resolved COUNTRY_KEY differs.

Run standalone:
    python -m loaders.dim_loaders.region_loader
"""

from loaders.dim_loaders.scd2_loader import Scd2DimLoader
from utils.db_connector import SnowflakeSession


class RegionLoader(Scd2DimLoader):
    STAGE_VIEW = "VW_D_REGION"
    TMP_TABLE  = "TMP_D_REGION"
    TGT_TABLE  = "TGT_D_REGION"

    def __init__(self):
        super().__init__("region_loader")

    def _stage_sql(self, sf: SnowflakeSession) -> str:
        return f"""
            INSERT INTO {sf.temp}.{self.TMP_TABLE}
                (REGION_NAME, COUNTRY_NAME)
            SELECT REGION_NAME, COUNTRY_NAME
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
                FROM   {sf.temp}.{self.TMP_TABLE}   tmp
                JOIN   {sf.target}.TGT_D_COUNTRY    c
                    ON c.COUNTRY_NAME = tmp.COUNTRY_NAME
                   AND c.IS_CURRENT   = TRUE
                WHERE  tmp.REGION_NAME  = tgt.REGION_NAME
                  AND  c.COUNTRY_KEY   != tgt.COUNTRY_KEY
              )
        """

    def _insert_sql(self, sf: SnowflakeSession) -> str:
        return f"""
            INSERT INTO {sf.target}.{self.TGT_TABLE}
                (REGION_NAME, COUNTRY_KEY, EFF_START_DATE, EFF_END_DATE, IS_CURRENT)
            SELECT
                 tmp.REGION_NAME
                ,c.COUNTRY_KEY
                ,CURRENT_DATE()
                ,'9999-12-31'::DATE
                ,TRUE
            FROM  {sf.temp}.{self.TMP_TABLE}    tmp
            JOIN  {sf.target}.TGT_D_COUNTRY     c
               ON c.COUNTRY_NAME = tmp.COUNTRY_NAME
              AND c.IS_CURRENT   = TRUE
            WHERE NOT EXISTS (
                SELECT 1
                FROM   {sf.target}.{self.TGT_TABLE} tgt
                WHERE  tgt.REGION_NAME = tmp.REGION_NAME
                  AND  tgt.COUNTRY_KEY = c.COUNTRY_KEY
                  AND  tgt.IS_CURRENT  = TRUE
            )
        """


if __name__ == "__main__":
    RegionLoader().execute()
