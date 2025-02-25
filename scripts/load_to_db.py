import os
import csv
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

        if not all([
            self._host, self._port, self._db_name, self._user, self._password
        ]):
            raise EnvironmentError("Missing environment variables for database connection.")
        
        self.batch_size = 1000  # Batch size for insert

    def _connect(self):
        """Connect to PostgreSQL database and return cursor."""
        try:
            connection = psycopg2.connect(
                host=self._host,
                port=self._port,
                database=self._db_name,
                user=self._user,
                password=self._password
            )
            return connection.cursor()
        except psycopg2.Error as error:
            raise error

    def _load_data_to_db(self, cursor):
        """Load data from CSV file to PostgreSQL database."""
        with open(self._csv_file, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header
            data = []
            for row in reader:
                ip_address, log_timestamp, request_url, status_code, response_size, referer_url, user_agent = row
                status_code = int(status_code) if status_code else None
                response_size = int(response_size) if response_size else None
                data.append((ip_address, log_timestamp, request_url, status_code, response_size, referer_url, user_agent))
                if len(data) >= self.batch_size:
                    self._execute_batch(cursor, data)
                    data = []  # Reset batch

            # Insert any remaining data
            if data:
                self._execute_batch(cursor, data)


    def _execute_batch(self, cursor, data):
        query = """
            INSERT INTO apache_logs 
            (ip_address, log_timestamp, request_url, status_code, response_size, referer_url, user_agent)
            VALUES %s
        """
        extras.execute_values(cursor, query, data)


    def load_data_to_db(self):
        """Load data from CSV file to PostgreSQL database."""
        start_time = time.time()
        cursor = None
        try:
            cursor = self._connect()
            self._load_data_to_db(cursor)
            cursor.connection.commit()
        except psycopg2.Error as error:
            if cursor:
                cursor.connection.rollback()
            raise error
        except FileNotFoundError:
            raise FileNotFoundError(f"Error: File {self._csv_file} not found.")
        except Exception as e: # Catch any other exceptions during execution
            if cursor:
                cursor.connection.rollback()
            raise e
        finally:
            if cursor:
                cursor.close()
                cursor.connection.close()
        end_time = time.time()
        print(f"Execution time: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    loader = DatabaseLoader()
    loader.load_data_to_db()
