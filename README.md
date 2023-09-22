# medium_scraper

A slightly sarcastic, highly over-engineered GitHub-as-a-Data-Platform Proof-of-Concept.

# Development setup

1. Create virtual env and install required packages:

    ```bash
    python3.11 -m virtualenv .venv
    source .venv/bin/activate
    pip install -e .
    pre-commit install
    ```

1. Required environment variables:

    ```bash
    set -gx DATA_DIR $(pwd)/local_output
    ```
