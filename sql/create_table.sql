-- Creates a table to store Apache log entries.
CREATE TABLE IF NOT EXISTS apache_logs (
    id SERIAL PRIMARY KEY,
    ip_address INET NOT NULL,
    log_timestamp TIMESTAMPTZ NOT NULL,
    request_url TEXT NOT NULL,
    status_code SMALLINT NULL,
    response_size BIGINT NULL,
    referer_url TEXT,
    user_agent TEXT,
    row_hash VARCHAR(64) UNIQUE
);

-- Indexes the table by the columns on which we query most frequently
CREATE INDEX idx_ip_address ON apache_logs USING HASH (ip_address);
CREATE INDEX idx_log_timestamp ON apache_logs (log_timestamp DESC);
CREATE INDEX idx_status_code ON apache_logs (status_code);
CREATE INDEX idx_row_hash ON apache_logs USING HASH (row_hash);

-- Grant read access to the public user so that anyone can query the table
GRANT SELECT ON apache_logs TO PUBLIC;
