import pytest
from scripts.log_parser import ApacheLogParser, ApacheLogParserConfig

@pytest.fixture
def parser():
    return ApacheLogParser(
        ApacheLogParserConfig(
            datetime_format='%d/%b/%Y:%H:%M:%S %z',
            default_status=0,
            default_size=0
        )
    )

@pytest.fixture
def valid_log_line():
    return '192.168.1.1 - - [10/Oct/2023:12:34:56 +0000] "GET /test HTTP/1.1" 200 1234 "http://referer" "Mozilla/5.0"'

@pytest.fixture
def invalid_log_line():
    return 'INVALID_LOG_LINE'
