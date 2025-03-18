import pytest
import os
import hashlib
from datetime import datetime
from unittest import mock
import pandas as pd
from scripts.load_to_db import DatabaseLoader
import psycopg2

@pytest.fixture
def loader(mocker):
    mocker.patch.dict(os.environ, {
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_NAME": "test_db",
        "DB_USER": "test_user",
        "DB_PASSWORD": "test_pass"
    })
    return DatabaseLoader()

@pytest.fixture
def sample_csv(tmp_path):
    csv_path = tmp_path / "test.csv"
    df = pd.DataFrame({
        'ip_address': ['192.168.1.1'],
        'log_timestamp': ['2023-01-01 12:00:00'],
        'request_url': ['GET /test HTTP/1.1'],
        'status_code': [200],
        'response_size': [1234],
        'referer': ['-'],
        'user_agent': ['Mozilla']
    })
    df.to_csv(csv_path, index=False)
    return csv_path

class TestDatabaseLoader:
    def test_load_config(self, loader):
        assert loader.config.host == "localhost"
        assert loader.config.dbname == "test_db"

    def test_file_hash_calculation(self, loader, tmp_path):
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")
        
        loader.CSV_PATH = str(test_file)
        file_hash = loader._calculate_file_hash()
        
        expected_hash = hashlib.sha256(b"test content").hexdigest()
        assert file_hash == expected_hash

    def test_file_change_detection(self, loader, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("version1")
        loader.CSV_PATH = str(csv_file)
        loader.HASH_PATH = str(tmp_path / "hash.txt")
        
        assert loader._check_file_changes() is True
        
        assert loader._check_file_changes() is False
        
        csv_file.write_text("version2")
        assert loader._check_file_changes() is True

    @mock.patch('psycopg2.connect')
    def test_database_connection(self, mock_connect, loader):
        loader._connect()
        mock_connect.assert_called_once_with(
            host="localhost",
            port="5432",
            dbname="test_db",
            user="test_user",
            password="test_pass"
        )

    def test_data_preparation(self, loader, sample_csv):
        loader.CSV_PATH = str(sample_csv)
        data = loader._prepare_data()
        
        assert len(data) == 1
        assert isinstance(data[0][1], datetime)
        assert len(data[0][7]) == 64

    @mock.patch('psycopg2.extras.execute_values')
    def test_batch_insert(self, mock_execute, loader):
        test_data = [('192.168.1.1', datetime.now(), '/test', 200, 1234, '-', 'Mozilla', 'hash')]
        
        with mock.patch('psycopg2.connect') as mock_connect:
            mock_cursor = mock_connect.return_value.cursor.return_value
            loader._execute_batch_insert(mock_cursor, test_data)
            
            mock_execute.assert_called_once()
            _, kwargs = mock_execute.call_args
            assert kwargs['page_size'] == 1000

    @mock.patch.object(DatabaseLoader, '_check_file_changes')
    @mock.patch.object(DatabaseLoader, '_prepare_data')
    @mock.patch('psycopg2.extras.execute_values')
    def test_full_load_process(self, mock_execute, mock_prepare, mock_check, loader):
        mock_check.return_value = True
        mock_prepare.return_value = [(
            '192.168.1.1',
            datetime(2023, 1, 1, 12, 0),
            '/test',
            200,
            1234,
            '-',
            'Mozilla',
            'hash123'
        )]
        
        with mock.patch('psycopg2.connect') as mock_connect:
            mock_conn = mock.MagicMock()
            mock_cursor = mock.MagicMock()
            
            mock_connect.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            
            loader.load_data()
            
            mock_execute.assert_called_once_with(
                mock_cursor,
                mock.ANY,
                mock_prepare.return_value,
                page_size=loader.BATCH_SIZE
            )
            
            mock_conn.commit.assert_called_once()
            mock_conn.close.assert_called_once()

    def test_missing_config(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(EnvironmentError) as exc_info:
                DatabaseLoader()
            assert "Missing environment variables" in str(exc_info.value)

    @mock.patch.object(DatabaseLoader, '_check_file_changes', return_value=False)
    def test_skip_load_when_no_changes(self, _, loader):
        with mock.patch('psycopg2.connect') as mock_connect:
            loader.load_data()
            mock_connect.assert_not_called()

