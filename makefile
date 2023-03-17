dbt_build:
	dbt build --project-dir ./dbt --profiles-dir ./dbt 

init:
	wget https://github.com/duckdb/duckdb/releases/download/v0.7.1/duckdb_cli-linux-amd64.zip
	unzip duckdb_cli-linux-amd64.zip -d duckdb
	rm duckdb_cli-linux-amd64.zip