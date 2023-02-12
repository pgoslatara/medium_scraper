import logging
import pyarrow as pa
from utils.utils import *

class EmailSender:
    def __init__(self, lookback_days: int):
        self.con = initialise_duckdb()
        self.lookback_days = lookback_days
        
    def get_relevant_blogs(self, lookback_days:int) -> dict:
        arrow_table = pa.Table.from_pylist(get_json_content())
        df = self.con.execute(f"""
            SELECT
                *,
                ROW_NUMBER() OVER(PARTITION BY story_url ORDER BY extracted_at_epoch DESC) AS row_num
            FROM arrow_table
            WHERE
                CAST(published_at AS DATE) >= (CURRENT_DATE() - INTERVAL {lookback_days} DAY)
                AND CAST(published_at AS DATE) < CURRENT_DATE()
            QUALIFY row_num = 1
            ORDER BY published_at
        """).arrow()

        data = df.to_pydict()
        logging.info(f"SELECTed {len(data)} blogs from the last {lookback_days} days.")
        return data

        
    def run(self):
        blogs = self.get_relevant_blogs(self.lookback_days)