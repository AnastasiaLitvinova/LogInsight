#!/bin/bash

set -euo pipefail

readonly LOG_FILE="test.log"
readonly PSQL_DB="log_analyzer"
readonly PSQL_PORT=5432
readonly LOG_PARSER_SCRIPT="scripts/log_parser.py"
readonly LOAD_TO_DB_SCRIPT="scripts/load_to_db.py"
readonly SQL_SCRIPT="sql/queries.sql"
readonly OUTPUT_FILE="data/queries.txt"

# Clean up previous test results
rm -rf "$LOG_FILE" "data/parsed_logs.csv" "data/csv_hash.txt" "data/queries.txt"
rm -rf ".pytest_cache" */__pycache__

# Run test script with detailed logging
echo "Running tests..."
python3 -m pytest tests/ -v | tee -a "$LOG_FILE" | grep -E "passed|failed" --color=always
echo "Tests completed. Check $LOG_FILE for details."

# Run the log parser script
python3 "$LOG_PARSER_SCRIPT" &

# Load data to the database
python3 "$LOAD_TO_DB_SCRIPT" &

# Wait for background processes to finish
wait

# Execute SQL queries and output results
echo "Executing SQL queries..."
psql -p "$PSQL_PORT" -d "$PSQL_DB" -f "$SQL_SCRIPT" -o "$OUTPUT_FILE"

echo "Analysis completed. Results saved to $OUTPUT_FILE"
