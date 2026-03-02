"""
loaders/dim_loaders/country_loader.py

SCD2 loader for TGT_D_COUNTRY.

COUNTRY_NAME is the only attribute.  Because a country's name is its own
natural key, there is nothing that can "change" independently — a new name
would simply be a new country.  For this reason the expire step is skipped
and we only insert rows that do not already exist as IS_CURRENT = TRUE.

Run standalone:
    python -m loaders.dim_loaders.country_loader
"""

from loaders.dim_loaders.scd2_loader import Scd2DimLoader
from utils.db_connector import SnowflakeSession


class CountryLoader(Scd2DimLoader):
    STAGE_VIEW = "VW_D_COUNTRY"
    TMP_TABLE  = "TMP_D_COUNTRY"
    TGT_TABLE  = "TGT_D_COUNTRY"

    def __init__(self):
        super().__init__("country_loader")

    def _stage_sql(self, sf: SnowflakeSession) -> str:
        return f"""
            INSERT INTO {sf.temp}.{self.TMP_TABLE} (COUNTRY_NAME)
            SELECT COUNTRY_NAME
            FROM   {sf.stage}.{self.STAGE_VIEW}
        """

    # No _expire_sql override — default None is correct for this dimension.

    def _insert_sql(self, sf: SnowflakeSession) -> str:
        return f"""
            INSERT INTO {sf.target}.{self.TGT_TABLE}
                (COUNTRY_NAME, EFF_START_DATE, EFF_END_DATE, IS_CURRENT)
            SELECT
                 tmp.COUNTRY_NAME
                ,CURRENT_DATE()
                ,'9999-12-31'::DATE
                ,TRUE
            FROM {sf.temp}.{self.TMP_TABLE} tmp
            WHERE NOT EXISTS (
                SELECT 1
                FROM   {sf.target}.{self.TGT_TABLE} tgt
                WHERE  tgt.COUNTRY_NAME = tmp.COUNTRY_NAME
                  AND  tgt.IS_CURRENT   = TRUE
            )
        """


if __name__ == "__main__":
    CountryLoader().execute()
