from dataclasses import dataclass, field
import re
import csv
import logging
from datetime import datetime
from typing import List, Dict, Optional, NamedTuple, Iterable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LogEntry(NamedTuple):
    """Data structure representing parsed Apache log entry"""
    ip_address: str
    log_timestamp: datetime
    request_url: str
    status_code: int
    response_size: int
    referer: str
    user_agent: str

@dataclass
class ApacheLogParserConfig:
    """Configuration for Apache log parser"""
    log_patterns: List[str] = field(default_factory=list)
    datetime_format: str = '%d/%b/%Y:%H:%M:%S'
    default_status: int = 0
    default_size: int = 0

class ApacheLogParser:
    """Parse Apache access logs with validation and error handling"""
    
    def __init__(self, config: ApacheLogParserConfig) -> None:
        self.config = config or ApacheLogParserConfig()
        self._compiled_patterns = self._compile_patterns()
        
    def _compile_patterns(self) -> List[re.Pattern]:
        """Compile regex patterns during initialization"""
        default_patterns = [
            r'(?P<ip_address>\S+) - - \[(?P<log_timestamp>[^\]]+)\] '
            r'"(?P<request_url>\S+ \S+ \S+)" '
            r'(?P<status_code>\d{3}) (?P<response_size>\d+|-) '
            r'"(?P<referer>[^"]*)" "(?P<user_agent>[^"]*)"'
        ]
        patterns = self.config.log_patterns or default_patterns
        return [re.compile(p, re.IGNORECASE) for p in patterns]

    def parse_line(self, line: str) -> Optional[LogEntry]:
        """Parse single log line with validation"""
        try:
            raw_data = self._match_pattern(line)
            if not raw_data:
                return None
                
            return LogEntry(
                ip_address=self._validate_ip(raw_data['ip_address']),
                log_timestamp=self._parse_datetime(raw_data['log_timestamp']),
                request_url=raw_data['request_url'],
                status_code=self._parse_status(raw_data['status_code']),
                response_size=self._parse_size(raw_data['response_size']),
                referer=raw_data['referer'],
                user_agent=raw_data['user_agent']
            )
        except (ValueError, KeyError) as e:
            logger.debug(f"Skipping invalid log line: {e}")
            return None

    def _match_pattern(self, line: str) -> Optional[Dict[str, str]]:
        """Match log line against known patterns"""
        for pattern in self._compiled_patterns:
            match = pattern.match(line)
            if match:
                return match.groupdict()
        return None

    def _validate_ip(self, ip_address: str) -> str:
        """Validate IP address format with complete check"""
        if not re.match(
            r'^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}'
            r'(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$', 
            ip_address
        ):
            raise ValueError(f"Invalid IP address: {ip_address}")
        return ip_address

    def _parse_datetime(self, log_timestamp: str) -> datetime:
        """Parse log_timestamp with fallback handling"""
        try:
            return datetime.strptime(log_timestamp, self.config.datetime_format)
        except ValueError:
            logger.warning(f"Invalid log_timestamp format: {log_timestamp}")
            exit(1)

    def _parse_status(self, status_code: str) -> int:
        """Parse and validate HTTP status code"""
        try:
            code = int(status_code)
            return code if 100 <= code <= 599 else self.config.default_status
        except (ValueError, TypeError):
            return self.config.default_status

    def _parse_size(self, response_size: str) -> int:
        """Parse and validate response size"""
        try:
            return int(response_size) if response_size != '-' else self.config.default_size
        except (ValueError, TypeError):
            return self.config.default_size

def read_log_file(file_path: str, parser: ApacheLogParser) -> Iterable[LogEntry]:
    """Read and parse log file with error handling"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                if entry := parser.parse_line(line):
                    yield entry
    except FileNotFoundError:
        logger.error(f"Log file not found: {file_path}")
        raise
    except IOError as e:
        logger.error(f"Error reading file {file_path}: {e}")
        raise

def save_to_csv(data: Iterable[LogEntry], csv_file: str) -> None:
    """Save parsed log entries to CSV with proper formatting"""
    if not data:
        logger.warning("No data to save to CSV")
        return

    field_names = LogEntry._fields
    try:
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(field_names)
            
            for entry in data:
                writer.writerow([
                    entry.ip_address,
                    entry.log_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    entry.request_url,
                    entry.status_code,
                    entry.response_size,
                    entry.referer,
                    entry.user_agent
                ])
        logger.info(f"Successfully saved {csv_file}")
    except IOError as e:
        logger.error(f"Failed to write CSV file: {e}")
        raise

def configure_parser() -> ApacheLogParser:
    """Create configured log parser instance"""
    config = ApacheLogParserConfig(
        default_status=0,
        default_size=0,
        datetime_format='%d/%b/%Y:%H:%M:%S %z'
    )
    return ApacheLogParser(config)

def main(log_file: str, csv_file: str) -> None:
    """Main processing pipeline"""
    parser = configure_parser()
    
    try:
        parsed_data = list(read_log_file(log_file, parser))
        if not parsed_data:
            logger.warning("No valid log entries found")
            return
            
        save_to_csv(parsed_data, csv_file)
        logger.info(f"Processed {len(parsed_data)} log entries")
        
    except Exception as e:
        logger.exception(f"Fatal error during processing: {e}")
        raise

if __name__ == '__main__':
    import sys
    
    try:
        log_path = 'data/apache_logs.txt'
        output_path = 'data/parsed_logs.csv'
        main(log_path, output_path)
    except Exception:
        sys.exit(1)

