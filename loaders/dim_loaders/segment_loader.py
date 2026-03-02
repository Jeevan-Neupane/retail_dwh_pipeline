"""
loaders/dim_loaders/segment_loader.py

SCD2 loader for TGT_D_SEGMENT.

SEGMENT_NAME is both the natural key and the only attribute — no FK
dependency.  Expire step is skipped; only new segment names are inserted.

Run standalone:
    python -m loaders.dim_loaders.segment_loader
"""

from loaders.dim_loaders.scd2_loader import Scd2DimLoader
from utils.db_connector import SnowflakeSession


class SegmentLoader(Scd2DimLoader):
    STAGE_VIEW = "VW_D_SEGMENT"
    TMP_TABLE  = "TMP_D_SEGMENT"
    TGT_TABLE  = "TGT_D_SEGMENT"

    def __init__(self):
        super().__init__("segment_loader")

    def _stage_sql(self, sf: SnowflakeSession) -> str:
        return f"""
            INSERT INTO {sf.temp}.{self.TMP_TABLE} (SEGMENT_NAME)
            SELECT SEGMENT_NAME
            FROM   {sf.stage}.{self.STAGE_VIEW}
        """

    def _insert_sql(self, sf: SnowflakeSession) -> str:
        return f"""
            INSERT INTO {sf.target}.{self.TGT_TABLE}
                (SEGMENT_NAME, EFF_START_DATE, EFF_END_DATE, IS_CURRENT)
            SELECT
                 tmp.SEGMENT_NAME
                ,CURRENT_DATE()
                ,'9999-12-31'::DATE
                ,TRUE
            FROM {sf.temp}.{self.TMP_TABLE} tmp
            WHERE NOT EXISTS (
                SELECT 1
                FROM   {sf.target}.{self.TGT_TABLE} tgt
                WHERE  tgt.SEGMENT_NAME = tmp.SEGMENT_NAME
                  AND  tgt.IS_CURRENT   = TRUE
            )
        """


if __name__ == "__main__":
    SegmentLoader().execute()
