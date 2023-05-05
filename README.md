# medium_scraper

# Development setup

1. Create virtual env and install required packages:

    ```bash
    python -m virtualenv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    pre-commit install
    ```

1. Required environment variables:

    ```bash
    set -gx DATA_DIR $(pwd)/local_output
    ```
