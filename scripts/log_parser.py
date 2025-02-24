import re
import datetime
import csv

class ApacheLogParser:
    def __init__(self, log_formats: list) -> None:
        """Initialize the ApacheLogParser with given log formats."""
        # Compile log format patterns with DOTALL flag
        self._log_formats = [(re.compile(pattern, re.DOTALL), pattern) for pattern in log_formats]

    def parse(self, line: str) -> dict:
        """Parse a single line of log data."""
        for regex, _ in self._log_formats:
            if self._is_log_format(line, regex):
                try:
                    match = regex.match(line)
                    if match:
                        data = match.groupdict()
                        self._parse_date_time(data)
                        self._parse_status(data)
                        self._parse_size(data)
                        return data
                except Exception:
                    return None
        return None

    def _is_log_format(self, line: str, regex: re.Pattern) -> bool:
        """Check if a line matches the log format."""
        return regex.match(line) is not None

    def _parse_date_time(self, data: dict) -> None:
        """Parse and format the date_time field in the data dictionary."""
        try:
            date_time_obj = datetime.datetime.strptime(
                data['date_time'], '%d/%b/%Y:%H:%M:%S %z')
            data['date_time'] = date_time_obj.strftime('%d/%m/%Y %H:%M:%S')
        except ValueError:
            data['date_time'] = None

    def _parse_status(self, data: dict) -> None:
        """Parse and convert the status field to an integer in the data dictionary."""
        try:
            data['status'] = int(data['status'])
        except ValueError:
            data['status'] = None

    def _parse_size(self, data: dict) -> None:
        """Parse and convert the size field to an integer in the data dictionary."""
        try:
            if data['size'] != '-':
                data['size'] = int(data['size'])
            else:
                data['size'] = None
        except ValueError:
            data['size'] = None


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


if __name__ == '__main__':
    log_file = 'data/apache_logs.txt'
    csv_file = 'data/parsed_logs.csv'
    log_formats = [
        r'(?P<ip>\d+\.\d+\.\d+\.\d+) - - \[(?P<date_time>[^\]]+)\] "(?P<url>[^"]+)" (?P<status>\d+) (?P<size>-|\d+) "(?P<referer>[^"]*)" "(?P<user_agent>[^"]*)"',
        r'(?P<ip>\d+\.\d+\.\d+\.\d+) - - \[(?P<date_time>[^\]]+)\] "(?P<url>[^"]+)" (?P<status>\d+) - "(?P<referer>[^"]*)" "(?P<user_agent>[^"]*)"'
    ]
    parser = ApacheLogParser(log_formats=log_formats)
    parsed_data = []

    try:
        with open(log_file, 'r') as f:
            for line in f:
                result = parser.parse(line)
                if result: # Only append parsed result
                    parsed_data.append(result)
    except FileNotFoundError:
        print(f"ERROR: Log file not found: {log_file}")
    except Exception as e:
        print(f"ERROR: An error occurred while reading the log file: {e}")

    save_to_csv(parsed_data, csv_file)
