import os
from pathlib import Path

from utils.utils import run_dbt_commands

from .author_details import main as transformation_main


def main() -> None:
    # Ensure required directories exist
    Path(f"{os.getenv('DATA_DIR')}/staging/medium_authors/").mkdir(
        parents=True, exist_ok=True
    )
    Path(f"{os.getenv('DATA_DIR')}/marts/").mkdir(parents=True, exist_ok=True)

    run_dbt_commands(["dbt deps"])

    # transformation_main depends on a dbt model
    run_dbt_commands(["dbt build --select stg_medium_authors__authors"])
    transformation_main()

    # Run dbt to update marts
    run_dbt_commands(["dbt build"])


if __name__ == "__main__":
    main()
