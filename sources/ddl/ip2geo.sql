CREATE TABLE IF NOT EXISTS ip2geo (
    ip_start BIGINT NOT NULL,
    ip_end BIGINT NOT NULL,
    country_abbr TEXT NOT NULL,
    country TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ip_idx on ip2geo (ip_start, ip_end);
