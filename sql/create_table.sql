CREATE TABLE IF NOT EXISTS apache_logs (
    id SERIAL PRIMARY KEY,
    ip_address VARCHAR(50) NOT NULL,
    log_timestamp TIMESTAMP NOT NULL,
    request_url TEXT NOT NULL,
    status_code INTEGER NULL,
    response_size INTEGER NULL,
    referer_url TEXT,
    user_agent TEXT,
    row_hash VARCHAR(64) UNIQUE
);

CREATE INDEX idx_ip_address ON apache_logs (ip_address);
CREATE INDEX idx_log_timestamp ON apache_logs (log_timestamp);
CREATE INDEX idx_status_code ON apache_logs (status_code);
CREATE INDEX idx_row_hash ON apache_logs (row_hash);
