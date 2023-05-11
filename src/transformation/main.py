import os
from pathlib import Path

from utils.utils import run_dbt_command


def main() -> None:
    # Ensure required directories exist
    Path(f"{os.getenv('DATA_DIR')}/marts/").mkdir(parents=True, exist_ok=True)

    # Run dbt to update marts
    run_dbt_command("dbt build")


if __name__ == "__main__":
    main()
