"""
loaders/dim_loaders/ship_mode_loader.py

SCD2 loader for TGT_D_SHIP_MODE.

SHIP_MODE is the natural key and only attribute — no FK dependency.
Expire step is skipped; only new shipping modes are inserted.

Run standalone:
    python -m loaders.dim_loaders.ship_mode_loader
"""

from loaders.dim_loaders.scd2_loader import Scd2DimLoader
from utils.db_connector import SnowflakeSession


class ShipModeLoader(Scd2DimLoader):
    STAGE_VIEW = "VW_D_SHIP_MODE"
    TMP_TABLE  = "TMP_D_SHIP_MODE"
    TGT_TABLE  = "TGT_D_SHIP_MODE"

    def __init__(self):
        super().__init__("ship_mode_loader")

    def _stage_sql(self, sf: SnowflakeSession) -> str:
        return f"""
            INSERT INTO {sf.temp}.{self.TMP_TABLE} (SHIP_MODE)
            SELECT SHIP_MODE
            FROM   {sf.stage}.{self.STAGE_VIEW}
        """

    def _insert_sql(self, sf: SnowflakeSession) -> str:
        return f"""
            INSERT INTO {sf.target}.{self.TGT_TABLE}
                (SHIP_MODE, EFF_START_DATE, EFF_END_DATE, IS_CURRENT)
            SELECT
                 tmp.SHIP_MODE
                ,CURRENT_DATE()
                ,'9999-12-31'::DATE
                ,TRUE
            FROM {sf.temp}.{self.TMP_TABLE} tmp
            WHERE NOT EXISTS (
                SELECT 1
                FROM   {sf.target}.{self.TGT_TABLE} tgt
                WHERE  tgt.SHIP_MODE  = tmp.SHIP_MODE
                  AND  tgt.IS_CURRENT = TRUE
            )
        """


if __name__ == "__main__":
    ShipModeLoader().execute()
