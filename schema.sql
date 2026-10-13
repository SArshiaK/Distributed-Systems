-- Run once on the server VM:
--   psql -h 127.0.0.1 -U bankuser -d minibank -f schema.sql

CREATE TABLE IF NOT EXISTS accounts (
    account_id VARCHAR(32) PRIMARY KEY,
    balance    NUMERIC(14, 2) NOT NULL CHECK (balance >= 0)
);

INSERT INTO accounts (account_id, balance) VALUES
    ('A1001', 1000.00),
    ('A1002', 500.00),
    ('A1003', 250.00)
ON CONFLICT (account_id) DO NOTHING;
