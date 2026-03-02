"""
loaders/dim_loaders/state_loader.py

SCD2 loader for TGT_D_STATE.

Tracked change: a state's REGION_KEY can change if the same STATE_NAME
is later associated with a different region.

Expire condition: STATE_NAME matches but the resolved REGION_KEY differs.

Run standalone:
    python -m loaders.dim_loaders.state_loader
"""

from loaders.dim_loaders.scd2_loader import Scd2DimLoader
from utils.db_connector import SnowflakeSession


class StateLoader(Scd2DimLoader):
    STAGE_VIEW = "VW_D_STATE"
    TMP_TABLE  = "TMP_D_STATE"
    TGT_TABLE  = "TGT_D_STATE"

    def __init__(self):
        super().__init__("state_loader")

    def _stage_sql(self, sf: SnowflakeSession) -> str:
        return f"""
            INSERT INTO {sf.temp}.{self.TMP_TABLE}
                (STATE_NAME, REGION_NAME)
            SELECT STATE_NAME, REGION_NAME
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
                JOIN   {sf.target}.TGT_D_REGION    r
                    ON r.REGION_NAME = tmp.REGION_NAME
                   AND r.IS_CURRENT  = TRUE
                WHERE  tmp.STATE_NAME  = tgt.STATE_NAME
                  AND  r.REGION_KEY   != tgt.REGION_KEY
              )
        """

    def _insert_sql(self, sf: SnowflakeSession) -> str:
        return f"""
            INSERT INTO {sf.target}.{self.TGT_TABLE}
                (STATE_NAME, REGION_KEY, EFF_START_DATE, EFF_END_DATE, IS_CURRENT)
            SELECT
                 tmp.STATE_NAME
                ,r.REGION_KEY
                ,CURRENT_DATE()
                ,'9999-12-31'::DATE
                ,TRUE
            FROM  {sf.temp}.{self.TMP_TABLE}   tmp
            JOIN  {sf.target}.TGT_D_REGION     r
               ON r.REGION_NAME = tmp.REGION_NAME
              AND r.IS_CURRENT  = TRUE
            WHERE NOT EXISTS (
                SELECT 1
                FROM   {sf.target}.{self.TGT_TABLE} tgt
                WHERE  tgt.STATE_NAME = tmp.STATE_NAME
                  AND  tgt.REGION_KEY = r.REGION_KEY
                  AND  tgt.IS_CURRENT = TRUE
            )
        """


if __name__ == "__main__":
    StateLoader().execute()
