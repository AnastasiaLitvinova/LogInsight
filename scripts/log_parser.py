import re
import datetime
import csv

class ApacheLogParser:
    """Parse Apache log data."""

    _DATE_TIME_FORMAT = '%d/%b/%Y:%H:%M:%S %z'
    _ISO_8601_FORMAT = '%Y-%m-%d %H:%M:%S'

    def __init__(self) -> None:
        """Initialize the ApacheLogParser with given log formats."""
        log_formats = [
            r'(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(?P<date_time>[^\]]+)\] "(?P<url>[^"]+)" (?P<status>\d{1,3}) (?P<size>-|\d+) "(?P<referer>[^"]*)" "(?P<user_agent>[^"]*)"'
        ]
        self._log_formats = [re.compile(pattern, re.DOTALL) for pattern in log_formats]

    def parse(self, line: str) -> dict:
        """Parse a single line of log data."""
        for regex in self._log_formats:
            match = regex.match(line)
            if match:
                data = match.groupdict()
                self._parse_fields(data)
                return data
        return None

    def _parse_fields(self, data: dict) -> None:
        """Parse and format fields in the data dictionary."""
        self._parse_date_time(data)
        self._parse_status(data)
        self._parse_size(data)

    def _parse_date_time(self, data: dict) -> None:
        """Parse and format the date_time field in the data dictionary."""
        try:
            date_time_obj = datetime.datetime.strptime(data['date_time'], self._DATE_TIME_FORMAT)
            data['date_time'] = date_time_obj.strftime(self._ISO_8601_FORMAT)
        except ValueError:
            data['date_time'] = None

    def _parse_status(self, data: dict) -> None:
        """Parse and convert the status field to an integer in the data dictionary."""
        data['status'] = int(data['status']) if data['status'].isdigit() else None

    def _parse_size(self, data: dict) -> None:
        """Parse and convert the size field to an integer in the data dictionary."""
        data['size'] = int(data['size']) if data['size'].isdigit() else None


def read_file(file_path: str, parser: ApacheLogParser) -> list:
    """Read a file and parse its contents using the given parser."""
    try:
        with open(file_path, 'r') as file:
            return [parser.parse(line) for line in file if parser.parse(line)]
    except FileNotFoundError:
        raise FileNotFoundError(f"Log file not found: {file_path}")
    except Exception as e:
        raise Exception(f"An error occurred while reading the log file: {e}")


def save_to_csv(data: list, csv_file: str) -> None:
    """Save parsed data to a CSV file."""
    if not data:
        print("No data to save to CSV.")
        return
    fieldnames = data[0].keys() if data else []  # Handle the case where data is empty
    try:
        with open(csv_file, 'w', newline='') as csvfile:
            if fieldnames:  # Only write header if there are fieldnames
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
            else:
                print("No fields to write to CSV.")
        print(f"Data saved to {csv_file}")
    except Exception as e:
        print(f"ERROR: Could not save data to CSV: {e}")


def main(log_file: str, csv_file: str) -> None:
    """Parse Apache log data and save to CSV file."""
    parser = ApacheLogParser()
    parsed_data = read_file(log_file, parser)
    save_to_csv(parsed_data, csv_file)


if __name__ == '__main__':
    log_file = 'data/apache_logs.txt'
    csv_file = 'data/parsed_logs.csv'
    main(log_file, csv_file)
