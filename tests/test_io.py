from datetime import datetime
import pytest
import csv
import tempfile
from pathlib import Path
from scripts.log_parser import LogEntry, ApacheLogParser, ApacheLogParserConfig, read_log_file, save_to_csv

class TestIOOperations:
    @pytest.fixture
    def sample_entries(self):
        return [
            LogEntry(
                ip_address='192.168.1.1',
                log_timestamp=datetime(2023, 1, 1, 12, 0),
                request_url='GET /',
                status_code=200,
                response_size=1234,
                referer='-',
                user_agent='Mozilla'
            )
        ]

    def test_save_and_load_csv(self, sample_entries):
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.csv"
            
            # Test saving
            save_to_csv(sample_entries, test_file)
            assert test_file.exists()
            
            # Test reading
            with open(test_file, 'r') as f:
                reader = csv.reader(f)
                headers = next(reader)
                assert headers == list(LogEntry._fields)
                
                row = next(reader)
                assert row[0] == '192.168.1.1'
                assert row[3] == '200'

    def test_read_nonexistent_file(self):
        config = ApacheLogParserConfig(
            log_patterns=[r'...'],
            datetime_format='%d/%b/%Y:%H:%M:%S %z',
            default_status=0,
            default_size=0
        )
        parser = ApacheLogParser(config=config)
        
        with pytest.raises(FileNotFoundError):
            list(read_log_file("nonexistent.log", parser))

    def test_save_empty_data(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "empty.csv"
            save_to_csv([], test_file)
            assert not test_file.exists()