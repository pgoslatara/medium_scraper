dbt_build:
	dbt build --project-dir ./dbt --profiles-dir ./dbt

init:
	wget https://github.com/duckdb/duckdb/releases/download/v0.7.1/duckdb_cli-linux-amd64.zip
	unzip duckdb_cli-linux-amd64.zip -d duckdb
	rm duckdb_cli-linux-amd64.zip

lite_build:
	jupyter lite build --lite-dir ./src/data_products --contents ./src/data_products/jupyter-lite.ipynb --contents ./output/marts --output-dir docs

lite_serve:
	jupyter lite serve --lite-dir ./src/data_products --contents ./src/data_products/jupyter-lite.ipynb --contents ./output/marts --output-dir docs

mypy:
	mypy . --strict --config-file ./mypy.ini
