set http_proxy=
set https_proxy=

@gover\go run .

.\.venv\scripts\activate & .\dbt\dbt run