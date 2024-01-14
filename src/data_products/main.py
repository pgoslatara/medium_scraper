import os

import papermill as pm  # type: ignore[import-not-found]

from src.data_products.email_weekly_medium_blogs import SendMediumBlogsEmail
from src.data_products.email_weekly_new_dbt_repos import SendNewDbtRepoEmail


def main() -> None:
    # MEDIUM_BLOG_LOOKBACK_WINDOW is "" when action is triggered from main branch
    default_lookback = 7
    try:
        lookback_days = int(os.getenv("MEDIUM_BLOG_LOOKBACK_WINDOW", default_lookback))
    except:
        lookback_days = default_lookback

    SendMediumBlogsEmail(lookback_days=lookback_days).run()
    SendNewDbtRepoEmail(lookback_days=lookback_days).run()
    pm.execute_notebook(
        input_path="./src/data_products/jupyter-lite.ipynb",
        output_path="./src/data_products/jupyter-lite.ipynb",
    )


if __name__ == "__main__":
    main()
