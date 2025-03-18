import os
import hashlib
import sys
import time
import pandas as pd
import psycopg2
from psycopg2 import extras
from typing import Optional, List, Tuple
from dataclasses import dataclass


@dataclass
class DBConfig:
    """Database configuration."""
    host: str
    port: str
    dbname: str
    user: str
    password: str


class DatabaseLoader:
    """Loads and manages data from CSV to PostgreSQL database with change detection."""

    CSV_PATH = "data/parsed_logs.csv"
    HASH_PATH = "data/csv_hash.txt"
    BATCH_SIZE = 1000

    def __init__(self) -> None:
        """Initialize database loader."""
        self.config = self._load_config()
        self._validate_config()
        self.conn: Optional[psycopg2.extensions.connection] = None

    def _load_config(self) -> DBConfig:
        """Load and validate database configuration from environment variables."""
        required_vars = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
        missing = [var for var in required_vars if var not in os.environ]
        if missing:
            raise EnvironmentError(f"Missing environment variables: {', '.join(missing)}")
        
        return DBConfig(
            host=os.environ["DB_HOST"],
            port=os.environ["DB_PORT"],
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"]
        )

    def _validate_config(self) -> None:
        """Validate database configuration."""
        if not all(vars(self.config).values()):
            raise EnvironmentError("Missing database configuration in environment variables")

    def _calculate_file_hash(self) -> Optional[str]:
        """Calculate SHA-256 hash of the CSV file."""
        hasher = hashlib.sha256()
        try:
            with open(self.CSV_PATH, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except FileNotFoundError:
            print(f"Error: CSV file not found at {self.CSV_PATH}")
            return None

    def _check_file_changes(self) -> bool:
        """Check if CSV file has changed since last load."""
        current_hash = self._calculate_file_hash()
        if not current_hash:
            return False

        try:
            with open(self.HASH_PATH, 'r') as f:
                previous_hash = f.read().strip()
        except FileNotFoundError:
            previous_hash = ''

        if current_hash != previous_hash:
            with open(self.HASH_PATH, 'w') as f:
                f.write(current_hash)
            return True
        return False

    def _connect(self) -> psycopg2.extensions.connection:
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                dbname=self.config.dbname,
                user=self.config.user,
                password=self.config.password
            )
            return self.conn
        except psycopg2.OperationalError as e:
            raise ConnectionError(f"Database connection failed: {e}") from e

    def _prepare_data(self) -> Optional[List[Tuple]]:
        """Load and prepare data from CSV using pandas with type conversion."""
        try:
            df = pd.read_csv(
                self.CSV_PATH,
                parse_dates=['log_timestamp'],
                na_values=['-']
            )
        except FileNotFoundError:
            print(f"Error: CSV file not found at {self.CSV_PATH}")
            return None

        # Replace NaN and convert to standard Python types
        df = df.fillna({'status_code': 0, 'response_size': 0})

        # Explicit type conversion
        df['status_code'] = df['status_code'].astype(int)
        df['response_size'] = df['response_size'].astype(int)

        # Generate hashes
        df['row_hash'] = df.apply(
            lambda row: hashlib.sha256(
                ','.join(map(str, row.values)).encode()
            ).hexdigest(),
            axis=1
        )

        # Convert data frame to list of tuples with standard Python types
        result = df.apply(
            lambda row: (
                str(row.ip_address),
                row.log_timestamp.to_pydatetime(),
                str(row.request_url),
                int(row.status_code),
                int(row.response_size),
                str(row.referer),
                str(row.user_agent),
                str(row.row_hash)
            ),
            axis=1
        ).tolist()

        return result

    def _execute_batch_insert(self, cursor: psycopg2.extensions.cursor, data: List[Tuple]) -> None:
        """Execute batch insert with conflict handling."""
        query = """
            INSERT INTO apache_logs (
                ip_address, log_timestamp, request_url, 
                status_code, response_size, referer, 
                user_agent, row_hash
            ) VALUES %s
            ON CONFLICT (row_hash) DO NOTHING
        """
        try:
            extras.execute_values(
                cursor, query, data, page_size=self.BATCH_SIZE
            )
        except psycopg2.Error as error:
            print(f"Database error during insert: {error}")
            raise

    def load_data(self) -> None:
        """Main method to load data into database."""
        if not self._check_file_changes():
            print("No changes detected in CSV file. Skipping load.")
            return

        start_time = time.time()
        data = self._prepare_data()
        if not data:
            return

        try:
            with self._connect() as conn:
                with conn.cursor() as cursor:
                    self._execute_batch_insert(cursor, data)
                    conn.commit()
                    print(f"Successfully inserted {len(data)} rows")

        except psycopg2.Error as e:
            print(f"Database error: {e}")
            conn.rollback()
        except Exception as e:
            print(f"Unexpected error: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()
            print(f"Execution time: {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    try:
        loader = DatabaseLoader()
        loader.load_data()
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

