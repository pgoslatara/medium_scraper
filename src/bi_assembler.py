import duckdb
import os
from pathlib import Path
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from utils.utils import *


class BiAssembler:
    def __init__(self):
        pass

    def get_duckdb_con(self) -> duckdb.connect:
        if get_environment() == "prod":
            database_file = (
                f"{os.getenv('DATA_DIR').replace('/output', '')}/dbt/dbt_prod.duckdb"
            )
        else:
            database_file = f"{os.getenv('DATA_DIR').replace('/local_output', '')}/dbt/dbt_dev.duckdb"

        logging.info(f"Opening duckdb connection to {database_file}...")

        return duckdb.connect(database=database_file, read_only=True)

    def run(self):
        # Use duckdb to read mart from data lake
        df = (
            self.get_duckdb_con()
            .execute(
                f"""
            SELECT
                *
            FROM read_parquet('{os.getenv('DATA_DIR')}/marts/fct_blogs_per_day.parquet')
        """
            )
            .df()
        )

        # Create subplot per tag
        tags = list(sorted(df["tag"].unique().tolist()))
        fig = make_subplots(rows=len(tags), cols=1, subplot_titles=tags)
        for tag in tags:
            fig.append_trace(
                go.Bar(
                    x=list(df[df.tag == tag].published_date),
                    y=list(df[df.tag == tag].num_blogs),
                    showlegend=False,
                ),
                row=tags.index(tag) + 1,
                col=1,
            )

        fig.update_layout(title_text="Medium Blog posts per tag per day")

        # Save to html file in data lake
        bi_html_filename = f"{os.getenv('DATA_DIR')}/bi/index.html"
        Path(bi_html_filename[: bi_html_filename.rfind("/")]).mkdir(
            parents=True, exist_ok=True
        )
        logging.info(f"Saving plotly html to {bi_html_filename}...")
        fig.write_html(bi_html_filename)
