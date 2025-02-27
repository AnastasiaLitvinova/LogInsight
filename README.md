# LogInsight: Web Server Logs Analyzer

> Parses Apache logs, loads data into PostgreSQL, and performs basic analysis.

## Project Goals

- parsing Apache logs
- uploading to PostgreSQL
- analysis (Top URLs, response codes)
- script automation `run_analysis.sh` with CSV hashing to avoid duplicates when uploading.

## Structure

- `data/`: Log files (`apache_logs.txt` [link to GitHub repository](https://github.com/elastic/examples/tree/master/Common%20Data%20Formats/apache_logs)), parsed data (`parsed_logs.csv`), result queries.
- `scripts/`: `log_parser.py` (parsing), `load_to_db.py` (DB loading), `run_analysis.sh` (automation).
- `sql/`: `create_table.sql` (table creation), `queries.sql` (analysis queries).

## Setup

1. Install PostgreSQL, Python, and `psycopg2`.
2. Create a database named `log_analyzer`.
3. Execute `sql/create_table.sql` to create the table.

## Usage

- Run `run_analysis.sh`:

    ``` bash
    bash scripts/run_analysis.sh
    ```

## Output

- The script parses the logs, uploads them to the database, executes SQL queries, and saves the results to `data/`.
