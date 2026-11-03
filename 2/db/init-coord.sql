CREATE TABLE IF NOT EXISTS distributed_transactions (
    transaction_id TEXT PRIMARY KEY,
    from_account TEXT NOT NULL,
    to_account TEXT NOT NULL,
    amount NUMERIC(14, 2) NOT NULL,
    from_shard TEXT NOT NULL,
    to_shard TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transaction_participants (
    transaction_id TEXT NOT NULL REFERENCES distributed_transactions(transaction_id),
    shard_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    account TEXT NOT NULL,
    vote TEXT,
    ack TEXT,
    PRIMARY KEY (transaction_id, shard_id)
);
