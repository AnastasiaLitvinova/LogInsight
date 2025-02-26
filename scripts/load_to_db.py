import os
import csv
import sys
import hashlib
import psycopg2
from psycopg2 import extras
import time

class DatabaseLoader:
    """Load data from CSV file to PostgreSQL database."""
    def __init__(self):
        """Initialize DatabaseLoader with environment variables."""
        self._host = os.environ.get("DB_HOST")
        self._port = os.environ.get("DB_PORT")
        self._db_name = os.environ.get("DB_NAME")
        self._user = os.environ.get("DB_USER")
        self._password = os.environ.get("DB_PASSWORD")
        self._csv_file = "data/parsed_logs.csv"  # Path to CSV file
        self.hash_file = "data/csv_hash.txt"  # File to store the hash

        if not all([
            self._host, self._port, self._db_name, self._user, self._password
        ]):
            raise EnvironmentError("Missing environment variables for database connection.")
        
        self.batch_size = 1000  # Batch size for insert


    def _calculate_file_hash(self, filepath: str) -> str:
        """Calculate the SHA-256 hash of a file."""
        hasher = hashlib.sha256()
        try:
            with open(filepath, 'rb') as file:
                while True:
                    chunk = file.read(4096)
                    if not chunk:
                        break
                    hasher.update(chunk)
        except FileNotFoundError:
            print(f"File not found: {filepath}")
            return None
        return hasher.hexdigest()


    def _check_if_file_changed(self):
        """Check if the CSV file has changed since the last load."""
        current_hash = self._calculate_file_hash(self._csv_file)
        if current_hash is None:
            return False  # File not found, assume it hasn't changed

        try:
            with open(self.hash_file, 'r') as file:
                previous_hash = file.read().strip()
        except FileNotFoundError:
            previous_hash = None  # No previous hash found

        if current_hash != previous_hash:
            # Save new hash
            with open(self.hash_file, 'w') as file:
                file.write(current_hash)
            return True  # File has changed
        return False  # File hasn't changed


    def _hash_row(self, row: list) -> str:
        """Calculates the SHA-256 hash of a CSV row."""
        row_str = ','.join(str(x) for x in row).encode('utf-8')
        return hashlib.sha256(row_str).hexdigest()


    def _connect(self):
        """Connect to PostgreSQL database and return cursor."""
        try:
            print("Attempting to connect to the database...")
            connection = psycopg2.connect(
                host=self._host,
                port=self._port,
                dbname=self._db_name,
                user=self._user,
                password=self._password
            )
            print("Database connection established successfully.")
            return connection.cursor()
        except psycopg2.OperationalError as error:
            print("OperationalError: Failed to connect to the database.")
            raise ConnectionRefusedError(
                "Failed to connect to the database. Ensure PostgreSQL is running and connection parameters are correct."
            ) from error
        except psycopg2.Error as error:
            print(f"DatabaseError: {error}")
            raise error


    def _load_data_to_db(self, cursor: psycopg2.extensions.cursor) -> None:
        if not self._check_if_file_changed():
            print("The file has not changed. Skipping database load.")
            return

        with open(self._csv_file, 'r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)  # Skip header
            batch_data = []
            
            for row in csv_reader:
                row_hash = self._hash_row(row)
                cursor.execute("SELECT EXISTS (SELECT 1 FROM apache_logs WHERE row_hash = %s)", (row_hash,))
                hash_exists = cursor.fetchone()[0]
                
                if not hash_exists:
                    ip, timestamp, url, status, size, referer, agent = row
                    status = int(status) if status else None
                    size = int(size) if size else None
                    batch_data.append((ip, timestamp, url, status, size, referer, agent, row_hash))
                    
                if len(batch_data) >= self.batch_size:
                    self._execute_batch(cursor, batch_data)
                    batch_data = []

            if batch_data:
                self._execute_batch(cursor, batch_data)


    def _execute_batch(self, cursor: psycopg2.extensions.cursor, batch_data: list) -> None:
        """Executes the batch insert, handling potential duplicates."""
        if not batch_data:
            print("No data to insert in batch.")
            return

        unique_data = []
        seen_hashes = set()
        for row in batch_data:
            row_hash = row[-1]
            if row_hash not in seen_hashes:
                unique_data.append(row)
                seen_hashes.add(row_hash)

        if not unique_data:
            print("No unique data to insert in batch.")
            return

        query = """
            INSERT INTO apache_logs
            (ip_address, log_timestamp, request_url, status_code, response_size, referer_url, user_agent, row_hash)
            VALUES %s
        """
        try:
            extras.execute_values(cursor, query, unique_data)
        except psycopg2.errors.UniqueViolation as e:
            print(f"Unique violation error: {e}")
            raise


    def load_data_to_db(self):
        """Load data from CSV file to PostgreSQL database."""
        start_time = time.time()
        try:
            cursor = self._connect()
            self._load_data_to_db(cursor)
            cursor.connection.commit()
        except (ConnectionRefusedError, FileNotFoundError) as error:
            print(f"{type(error).__name__}: {error}")
            sys.exit(1)
        except psycopg2.Error as error:
            cursor.connection.rollback()
            raise error
        except Exception as error:
            cursor.connection.rollback()
            raise error
        finally:
            cursor.close()
            cursor.connection.close()
        end_time = time.time()
        print(f"Execution time: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    loader = DatabaseLoader()
    loader.load_data_to_db()
