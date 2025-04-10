import logging
from typing import AsyncGenerator, List, Optional, Dict, Tuple
import re
from datetime import datetime
import hashlib
import time
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
import os
import asyncio

from db_manager import DatabaseManager, DBConfig


class LogReader:
    """Async log file reader with batch processing"""
    
    def __init__(self, batch_size: int = 10000):
        self.batch_size = batch_size
        self.logger = logging.getLogger("file_logger")

    async def read_batches(self, file_path: str) -> AsyncGenerator[List[str], None]:
        """Read log file in batches"""
        try:
            buffer = []
            with open(file_path, 'r') as f:
                for line in f:
                    buffer.append(line.strip())
                    if len(buffer) >= self.batch_size:
                        yield buffer
                        buffer = []
                if buffer:
                    yield buffer
            self.logger.info(f"Finished reading {file_path}")
        
        except IOError as e:
            self.logger.error(f"File read error: {e}")
            raise


class LogParser:
    """Apache log parser with hash generation"""

    logger = logging.getLogger("file_logger")
    
    LOG_PATTERN = re.compile(
        r'(?P<ip>\S+) - - \[(?P<time>\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})\] '
        r'"(?P<method>\S+) (?P<url>\S+) (?P<protocol>\S+)" '
        r'(?P<status_code>\d{3}) (?P<response_size>\d+|-)'
        r'(?: "(?P<referrer>[^"]*)" "(?P<user_agent>[^"]*)")?'
    )

    @classmethod
    def parse_line(cls, line: str) -> Optional[Dict]:
        """Parse single log line"""
        match = cls.LOG_PATTERN.match(line)
        if not match:
            cls.logger.warning(f"Failed to parse line: {line[:50]}...")
            return None

        entry = match.groupdict()
        
        try:
            entry['time'] = datetime.strptime(
                entry['time'], 
                '%d/%b/%Y:%H:%M:%S %z'
            )
        except ValueError as e:
            cls.logger.error(f"Invalid time format: {entry['time']} - {e}")
            return None
            
        return entry

    @staticmethod
    def generate_hash(log_entry: Dict) -> str:
        """Generate SHA-256 hash for log entry"""
        log_str = str(sorted(log_entry.items())).encode('utf-8')
        return hashlib.sha256(log_str).hexdigest()


class LogLoader:
    """Async log loader with deduplication and batch processing"""
    
    def __init__(self, db_manager: DatabaseManager, batch_size: int = 10000):
        self.logger = logging.getLogger("file_logger")
        self.db_manager = db_manager
        self.batch_size = batch_size
        self.buffer: List[Dict] = []
        self.seen_hashes = set()
        self.metrics = {
            'total': 0,
            'duplicates': 0,
            'errors': 0,
            'parsing_time': 0.0,
            'db_time': 0.0
        }

    async def add_log_entry(self, log_entry: Dict) -> bool:
        """Add log entry to buffer with deduplication"""
        self.metrics['total'] += 1
        
        if not log_entry:
            self.metrics['errors'] += 1
            return False

        log_hash = LogParser.generate_hash(log_entry)
        if log_hash in self.seen_hashes:
            self.metrics['duplicates'] += 1
            return False
            
        self.seen_hashes.add(log_hash)
        self.buffer.append(log_entry)
        
        if len(self.buffer) >= self.batch_size:
            await self.flush()
            
        return True

    async def flush(self) -> int:
        """Flush buffer to database"""
        if not self.buffer:
            return 0

        start_time = time.time()    
        try:
            inserted = await self.db_manager.bulk_insert(
                "apache_logs",
                [
                    'ip', 'time', 'method', 'url', 'protocol',
                    'status_code', 'response_size', 'referrer', 
                    'user_agent', 'log_hash'
                ],
                [
                    (
                        e['ip'], 
                        e['time'],
                        e['method'], 
                        e['url'],
                        e['protocol'], 
                        int(e['status_code']),
                        int(e['response_size']) if e['response_size'] != '-' else 0,
                        e.get('referrer', ''), 
                        e.get('user_agent', ''),
                        LogParser.generate_hash(e)
                    )
                    for e in self.buffer
                ]
            )
            self.buffer.clear()
            self.metrics['db_time'] += time.time() - start_time
            return inserted
        except Exception as e:
            self.metrics['errors'] += 1
            self.logger.error(f"Flush failed: {str(e)}")
            return 0


class LogAnalyzer:
    """Log data analyzer with async database access"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def get_status_stats(self, table_name: str) -> List[Tuple]:
        """Get statistics on HTTP status codes"""
        async with self.db_manager.get_connection() as conn:
            return await conn.fetch(
                f"""
                SELECT status_code, COUNT(*) AS count
                FROM {table_name}
                GROUP BY status_code
                ORDER BY count DESC
                """
            )

    async def get_top_urls(self, table_name: str, limit: int = 10) -> List[Tuple]:
        """Get top URLs by request count"""
        async with self.db_manager.get_connection() as conn:
            return await conn.fetch(
                f"""
                SELECT url, COUNT(*) AS count
                FROM {table_name}
                GROUP BY url
                ORDER BY count DESC
                LIMIT {limit}
                """
            )


class LogAnalysisApp:
    """Main application controller"""
    
    def __init__(self, db_config: DBConfig):
        self._setup_logging()
        self.logger = logging.getLogger("file_logger")
        self.db_config = db_config
        self.db_manager = DatabaseManager(db_config)
        self.reader = LogReader()
        self.loader = LogLoader(self.db_manager)
        self.analyzer = LogAnalyzer(self.db_manager)

    def _setup_logging(self):
        """Setup separate logging"""
        file_logger = logging.getLogger("file_logger")
        file_logger.setLevel(logging.INFO)

        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        file_handler = RotatingFileHandler(
            'app.log',
            maxBytes=10*1024*1024,
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setFormatter(file_formatter)
        file_logger.addHandler(file_handler)
        file_logger.propagate = False

    def _show_progress(self, current: int, total: int, bar_length: int = 40) -> None:
        """Show progress bar in console"""
        percent = current / total
        arrow = '*' * int(round(percent * bar_length))
        spaces = ' ' * (bar_length - len(arrow))
        print(f'\r[ {arrow + spaces} ] {int(round(percent * 100))}%', end='', flush=True)

    async def run(self, file_path: str, table_name: str = "apache_logs") -> None:
        start_time = time.time()
        self.logger.info("Starting log processing")
        total_lines = sum(1 for _ in open(file_path, 'r'))
        processed_lines = 0
        self._show_progress(0, total_lines)
        
        # Initialize database
        await self.db_manager.create_table(
            table_name,
            {
                "ip": "VARCHAR(15) NOT NULL",
                "time": "TIMESTAMPTZ NOT NULL",
                "method": "VARCHAR(10) NOT NULL",
                "url": "TEXT NOT NULL",
                "protocol": "VARCHAR(10) NOT NULL",
                "status_code": "SMALLINT NOT NULL",
                "response_size": "BIGINT NOT NULL",
                "referrer": "TEXT",
                "user_agent": "TEXT",
                "log_hash": "VARCHAR(64) UNIQUE NOT NULL"
            }
        )

        # Process logs
        parse_start = time.time()
        async for batch in self.reader.read_batches(file_path):
            parsed = (LogParser.parse_line(line) for line in batch)
            for entry in parsed:
                if entry:
                    try:
                        await self.loader.add_log_entry(entry)
                    except Exception as e:
                        self.logger.error(f"Error processing entry: {str(e)}")

            processed_lines += len(batch)
            self._show_progress(processed_lines, total_lines)

        self.loader.metrics['parsing_time'] = time.time() - parse_start
        await self.loader.flush()

        # Show metrics
        self.logger.info(f"Processing complete. Metrics: {self.loader.metrics}")

        # Run analysis
        top_urls = await self.analyzer.get_top_urls(table_name)
        self.logger.info("Top URLs:")
        for url, count in top_urls:
            self.logger.info(f"{url}: {count} requests")
        
        metrics = await self._collect_metrics()
        self._print_report(metrics, time.time() - start_time)
        await self._perform_analysis(table_name)

    def _print_report(self, metrics: Dict, total_time: float) -> None:
        """Prints the report to the console"""
        print("\n\n\033[1m" + "="*40 + "\033[0m")
        print("\033[1m Log Processing Report \033[0m".center(50))
        print("\033[1m" + "="*40 + "\033[0m\n")
        print(f"  Total lines processed: \033[36m{metrics['total_lines']}\033[0m")
        print(f"  Unique lines loaded:   \033[32m{metrics['unique_lines']}\033[0m")
        print(f"  Duplicate lines found: \033[33m{metrics['duplicate_lines']}\033[0m")
        print(f"  Parsing time:          \033[35m{metrics['parsing_time']:.2f}s\033[0m")
        print(f"  Database load time:    \033[35m{metrics['db_time']:.2f}s\033[0m")
        print(f"  Total execution time:  \033[35m{total_time:.2f}s\033[0m\n")

    async def _perform_analysis(self, table_name: str) -> None:
        """Prints the analysis to the console"""
        print(f"\n{' Log Analysis ':=^40}")
        
        print("\nTop 10 URLs:")
        top_urls = await self.analyzer.get_top_urls(table_name)
        for url, count in top_urls:
            print(f"{url:<50} | {count:>6}")
        
        print("\nStatus Code Statistics:")
        stats = await self.analyzer.get_status_stats(table_name)
        for code, count in stats:
            print(f"HTTP {code}: {count} requests")

    async def _collect_metrics(self) -> Dict:
        """Collects metrics for the report"""
        return {
            'total_lines': self.loader.metrics['total'],
            'unique_lines': len(self.loader.seen_hashes),
            'duplicate_lines': self.loader.metrics['duplicates'],
            'parsing_time': self.loader.metrics.get('parsing_time', 0.0),
            'db_time': self.loader.metrics.get('db_time', 0.0)
        }


if __name__ == "__main__":
    # Initialize configuration
    load_dotenv()
    config = DBConfig(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "apache_logs"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres")
    )

    # Run the application
    app = LogAnalysisApp(config)
    asyncio.run(app.run("./data/sample.log", table_name="apache_logs"))
