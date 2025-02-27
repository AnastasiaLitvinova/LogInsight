#!/bin/bash

# Define variables for readability and maintainability
LOG_PARSER_SCRIPT="scripts/log_parser.py"
LOAD_TO_DB_SCRIPT="scripts/load_to_db.py"
PSQL_DB="log_analyzer"
SQL_SCRIPT="sql/queries.sql"
OUTPUT_FILE="data/queries.txt"
PSQL_PORT=5432

# Run the log parser script
python3 $LOG_PARSER_SCRIPT

# Load data to the database
python3 $LOAD_TO_DB_SCRIPT

# Execute SQL queries and output results
psql -p $PSQL_PORT -d $PSQL_DB -f $SQL_SCRIPT -o $OUTPUT_FILE

echo "Analysis completed. Results saved to $OUTPUT_FILE"
