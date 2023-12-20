import os
from pathlib import Path

from src.transformation.author_details import main as transformation_main
from utils.utils import dbt_invoke


def main() -> None:
    # Ensure required directories exist
    Path(f"{os.getenv('DATA_DIR')}/staging/medium_authors/").mkdir(parents=True, exist_ok=True)
    Path(f"{os.getenv('DATA_DIR')}/marts/").mkdir(parents=True, exist_ok=True)

    dbt_invoke(["deps"])

    # transformation_main depends on a dbt model
    dbt_invoke(["build", "--select", "stg_medium_authors__authors"])
    transformation_main()

    # Run dbt to update marts
    dbt_invoke(["build"])


if __name__ == "__main__":
    main()
