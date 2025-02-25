-- Count of requests for each status code
SELECT status_code, COUNT(*) AS request_count
FROM apache_logs
WHERE status_code IS NOT NULL
GROUP BY status_code
ORDER BY request_count DESC;

-- Top 10 most visited URLs
SELECT request_url, COUNT(*) AS request_count
FROM apache_logs
GROUP BY request_url
ORDER BY request_count DESC
LIMIT 10;
