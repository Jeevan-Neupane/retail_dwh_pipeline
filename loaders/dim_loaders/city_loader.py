"""
loaders/dim_loaders/city_loader.py

SCD2 loader for TGT_D_CITY.

Natural key: POSTAL_CODE (unique per physical location).

Tracked changes:
  - CITY_NAME    — postal code renamed or reassigned
  - STATE_KEY    — postal code moved to a different state (e.g. reclassified)

Expire condition: POSTAL_CODE matches but CITY_NAME or STATE_KEY differs.

Run standalone:
    python -m loaders.dim_loaders.city_loader
"""

from loaders.dim_loaders.scd2_loader import Scd2DimLoader
from utils.db_connector import SnowflakeSession


class CityLoader(Scd2DimLoader):
    STAGE_VIEW = "VW_D_CITY"
    TMP_TABLE  = "TMP_D_CITY"
    TGT_TABLE  = "TGT_D_CITY"

    def __init__(self):
        super().__init__("city_loader")

    def _stage_sql(self, sf: SnowflakeSession) -> str:
        return f"""
            INSERT INTO {sf.temp}.{self.TMP_TABLE}
                (CITY_NAME, POSTAL_CODE, STATE_NAME)
            SELECT CITY_NAME, POSTAL_CODE, STATE_NAME
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
                JOIN   {sf.target}.TGT_D_STATE     s
                    ON s.STATE_NAME = tmp.STATE_NAME
                   AND s.IS_CURRENT = TRUE
                WHERE  tmp.POSTAL_CODE  = tgt.POSTAL_CODE
                  AND (
                        tmp.CITY_NAME  != tgt.CITY_NAME
                     OR s.STATE_KEY    != tgt.STATE_KEY
                  )
              )
        """

    def _insert_sql(self, sf: SnowflakeSession) -> str:
        return f"""
            INSERT INTO {sf.target}.{self.TGT_TABLE}
                (CITY_NAME, POSTAL_CODE, STATE_KEY,
                 EFF_START_DATE, EFF_END_DATE, IS_CURRENT)
            SELECT
                 tmp.CITY_NAME
                ,tmp.POSTAL_CODE
                ,s.STATE_KEY
                ,CURRENT_DATE()
                ,'9999-12-31'::DATE
                ,TRUE
            FROM  {sf.temp}.{self.TMP_TABLE}   tmp
            JOIN  {sf.target}.TGT_D_STATE      s
               ON s.STATE_NAME = tmp.STATE_NAME
              AND s.IS_CURRENT = TRUE
            WHERE NOT EXISTS (
                SELECT 1
                FROM   {sf.target}.{self.TGT_TABLE} tgt
                WHERE  tgt.POSTAL_CODE = tmp.POSTAL_CODE
                  AND  tgt.CITY_NAME   = tmp.CITY_NAME
                  AND  tgt.STATE_KEY   = s.STATE_KEY
                  AND  tgt.IS_CURRENT  = TRUE
            )
        """


if __name__ == "__main__":
    CityLoader().execute()
