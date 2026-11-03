CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY,
    balance NUMERIC(14, 2) NOT NULL CHECK (balance >= 0)
);

-- Shard A owns A0001–A1000
INSERT INTO accounts (account_id, balance)
SELECT 'A' || lpad(i::text, 4, '0'), 1000.00
FROM generate_series(1, 1000) AS i
ON CONFLICT (account_id) DO NOTHING;
