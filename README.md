# LogInsight: Web Server Logs Analyzer

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

> LogInsight is a high-performance log analysis tool that processes Apache web server logs, stores them in PostgreSQL, and provides actionable insights. Designed for efficiency with large datasets (1M+ log entries).

**Key Features**:

- 🚀 Parallel log processing with async I/O
- 🗃️ PostgreSQL bulk loading (COPY command)
- 🔍 Duplicate detection using SHA-256 hashing
- 📊 Built-in analytics (top URLs, status stats)
- ⚡ 3x faster than naive implementations

## Installation

```bash
git clone https://github.com/AnastasiaLitvinova/LogInsight.git
cd LogInsight
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration

1. Create .env file:

    ``` ini
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=apache_logs
    DB_USER=postgres
    DB_PASSWORD=postgres
    ```

2. Unzip archive

   ``` bash
   unzip data/apache_logs.zip && mv sample.log ./data
   ```

3. Ensure PostgreSQL is running

## Usage

 ``` bash
 python3 src/app.py
 ```
