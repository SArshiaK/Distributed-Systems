CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY,
    balance NUMERIC(14, 2) NOT NULL CHECK (balance >= 0)
);

-- Shard C starts empty; Bonus I migrator copies A1501–A2000 from B at runtime.
