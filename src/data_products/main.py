import papermill as pm  # type: ignore[import-not-found]

from src.data_products.email_weekly_medium_blogs import SendMediumBlogsEmail
from src.data_products.email_weekly_new_dbt_discussions import SendNewDbtDiscussionEmail
from src.data_products.email_weekly_new_dbt_repos import SendNewDbtRepoEmail
from src.data_products.email_weekly_new_dutch_interactors import (
    SendNewDutchInteractorEmail,
)
from utils.utils import get_ingestion_lookback_days


def main() -> None:
    lookback_days = get_ingestion_lookback_days()

    SendMediumBlogsEmail(lookback_days=lookback_days).run()
    SendNewDbtDiscussionEmail(lookback_days=lookback_days).run()
    SendNewDbtRepoEmail(lookback_days=lookback_days).run()
    SendNewDutchInteractorEmail(lookback_days=lookback_days).run()
    pm.execute_notebook(
        input_path="./src/data_products/jupyter-lite.ipynb",
        output_path="./src/data_products/jupyter-lite.ipynb",
    )


if __name__ == "__main__":
    main()
