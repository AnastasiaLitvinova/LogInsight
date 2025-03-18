import pytest
from datetime import datetime, timezone
from scripts.log_parser import ApacheLogParser, LogEntry

class TestApacheLogParser:
    def test_parse_valid_line(self, parser, valid_log_line):
        entry = parser.parse_line(valid_log_line)
        assert isinstance(entry, LogEntry)
        assert entry.ip_address == "192.168.1.1"
        expected_time = datetime(2023, 10, 10, 12, 34, 56, tzinfo=timezone.utc)
        assert entry.log_timestamp == expected_time
        assert entry.status_code == 200
        assert entry.response_size == 1234
        assert entry.request_url == "GET /test HTTP/1.1"

    def test_parse_invalid_line(self, parser, invalid_log_line):
        assert parser.parse_line(invalid_log_line) is None

    def test_invalid_ip_handling(self, parser):
        line = '256.0.0.1 - - [10/Oct/2023:12:34:56 +0000] "GET / HTTP/1.1" 200 1234 "-" "-"'
        entry = parser.parse_line(line)
        assert entry is None

    @pytest.mark.parametrize("size_input,expected", [
        ('1234', 1234),
        ('-', 0),
        ('invalid', 0)
    ])
    def test_size_parsing(self, parser, size_input, expected):
        line = f'127.0.0.1 - - [10/Oct/2023:12:34:56 +0000] "GET / HTTP/1.1" 200 {size_input} "-" "-"'
        entry = parser.parse_line(line)
        if entry is None:
            return
        assert entry.response_size == expected

    def test_datetime_parsing(self, parser):
        line = '127.0.0.1 - - [10/Oct/2023:12:34:56 +0000] "GET / HTTP/1.1" 200 1234 "-" "-"'
        entry = parser.parse_line(line)
        assert entry.log_timestamp.year == 2023
        assert entry.log_timestamp.hour == 12

    @pytest.mark.parametrize("status_input,expected", [
        ('200', 200),
        ('999', 0),
        ('invalid', 0)
    ])
    def test_status_parsing(self, parser, status_input, expected):
        line = f'127.0.0.1 - - [10/Oct/2023:12:34:56 +0000] "GET / HTTP/1.1" {status_input} 1234 "-" "-"'
        entry = parser.parse_line(line)
        if entry is None:
            return
        assert entry.status_code == expected
