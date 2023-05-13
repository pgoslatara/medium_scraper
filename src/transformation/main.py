import os
from pathlib import Path

from utils.utils import run_dbt_commands

from .nlp_author_location import main as nlp_main


def main() -> None:
    # Ensure required directories exist
    Path(f"{os.getenv('DATA_DIR')}/marts/").mkdir(parents=True, exist_ok=True)

    run_dbt_commands(["dbt deps"])

    # nlp_main depends on a dbt model
    run_dbt_commands(["dbt build --select dim_medium_authors"])
    nlp_main()

    # Run dbt to update marts
    run_dbt_commands(["dbt build"])


if __name__ == "__main__":
    main()
