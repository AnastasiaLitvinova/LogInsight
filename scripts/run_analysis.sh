# run from LogInsight, like: sh scripts/run_analysis.sh
python3 scripts/log_parser.py
python3 scripts/load_to_db.py
psql -p 5432 log_analyzer -f sql/queries.sql -o data/queries.txt
