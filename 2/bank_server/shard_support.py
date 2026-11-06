"""Bonus I helpers: changelog CDC, ownership fencing, client request idempotency."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any, Optional

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS account_changelog (
    seq BIGSERIAL PRIMARY KEY,
    account_id TEXT NOT NULL,
    balance NUMERIC(14, 2) NOT NULL,
    op TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS account_changelog_account_seq
    ON account_changelog (account_id, seq);

CREATE TABLE IF NOT EXISTS applied_changes (
    source_seq BIGINT PRIMARY KEY,
    account_id TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ownership_state (
    id INT PRIMARY KEY CHECK (id = 1),
    epoch INT NOT NULL DEFAULT 1,
    owned_start INT NOT NULL,
    owned_end INT NOT NULL,
    write_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    fenced_start INT,
    fenced_end INT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS client_requests (
    client_request_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    account TEXT NOT NULL,
    to_account TEXT,
    amount NUMERIC(14, 2),
    response_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

TRIGGER_SQL = """
CREATE OR REPLACE FUNCTION log_account_change() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO account_changelog(account_id, balance, op)
        VALUES (OLD.account_id, OLD.balance, 'DELETE');
        RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO account_changelog(account_id, balance, op)
        VALUES (NEW.account_id, NEW.balance, 'UPSERT');
        RETURN NEW;
    ELSE
        INSERT INTO account_changelog(account_id, balance, op)
        VALUES (NEW.account_id, NEW.balance, 'UPSERT');
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_account_changelog ON accounts;
CREATE TRIGGER trg_account_changelog
AFTER INSERT OR UPDATE OR DELETE ON accounts
FOR EACH ROW EXECUTE FUNCTION log_account_change();
"""


def account_num(account_id: str) -> Optional[int]:
    if not account_id:
        return None
    a = account_id.strip().upper()
    if len(a) == 5 and a[0] == "A" and a[1:].isdigit():
        return int(a[1:])
    return None


def ensure_schema(conn, *, shard_id: str, default_start: int, default_end: int) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        cur.execute(TRIGGER_SQL)
        cur.execute("SELECT 1 FROM ownership_state WHERE id = 1")
        if cur.fetchone() is None:
            cur.execute(
                "INSERT INTO ownership_state "
                "(id, epoch, owned_start, owned_end, write_enabled) "
                "VALUES (1, 1, %s, %s, TRUE)",
                (default_start, default_end),
            )
    conn.commit()


def get_ownership(cur) -> dict:
    cur.execute("SELECT * FROM ownership_state WHERE id = 1")
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("ownership_state missing")
    return dict(row)


def set_ownership(
    cur,
    *,
    epoch: Optional[int] = None,
    owned_start: Optional[int] = None,
    owned_end: Optional[int] = None,
    write_enabled: Optional[bool] = None,
    fenced_start: Optional[int] = None,
    fenced_end: Optional[int] = None,
    clear_fence: bool = False,
) -> dict:
    current = get_ownership(cur)
    new_epoch = current["epoch"] if epoch is None else int(epoch)
    new_start = current["owned_start"] if owned_start is None else int(owned_start)
    new_end = current["owned_end"] if owned_end is None else int(owned_end)
    new_we = current["write_enabled"] if write_enabled is None else bool(write_enabled)
    if clear_fence:
        fs = fe = None
    else:
        fs = current["fenced_start"] if fenced_start is None else int(fenced_start)
        fe = current["fenced_end"] if fenced_end is None else int(fenced_end)
        if fenced_start is None and fenced_end is None:
            fs, fe = current["fenced_start"], current["fenced_end"]
    cur.execute(
        "UPDATE ownership_state SET epoch=%s, owned_start=%s, owned_end=%s, "
        "write_enabled=%s, fenced_start=%s, fenced_end=%s, updated_at=NOW() WHERE id=1",
        (new_epoch, new_start, new_end, new_we, fs, fe),
    )
    return get_ownership(cur)


def check_write_allowed(
    cur,
    account: str,
    *,
    request_epoch: Optional[int],
    allow_during_fence: bool = False,
) -> Optional[str]:
    """Return error code if write must be rejected, else None."""
    own = get_ownership(cur)
    num = account_num(account)
    if num is None:
        return "invalid_account"
    if not (own["owned_start"] <= num <= own["owned_end"]):
        return "stale_owner"
    if request_epoch is not None and int(request_epoch) < int(own["epoch"]):
        return "stale_epoch"
    if not own["write_enabled"] and not allow_during_fence:
        return "writes_disabled"
    fs, fe = own["fenced_start"], own["fenced_end"]
    if fs is not None and fe is not None and fs <= num <= fe and not allow_during_fence:
        return "account_fenced"
    return None


def get_cached_client_request(cur, client_request_id: str) -> Optional[dict]:
    cur.execute(
        "SELECT response_json FROM client_requests WHERE client_request_id = %s",
        (client_request_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    raw = row["response_json"]
    if isinstance(raw, str):
        return json.loads(raw)
    return dict(raw)


def store_client_request(
    cur,
    client_request_id: str,
    action: str,
    account: str,
    to_account: Optional[str],
    amount: Optional[Decimal],
    response: dict,
) -> None:
    cur.execute(
        "INSERT INTO client_requests "
        "(client_request_id, action, account, to_account, amount, response_json) "
        "VALUES (%s, %s, %s, %s, %s, %s::jsonb) "
        "ON CONFLICT (client_request_id) DO NOTHING",
        (
            client_request_id,
            action,
            account,
            to_account,
            amount,
            json.dumps(response),
        ),
    )


def changelog_max_seq(cur) -> int:
    cur.execute("SELECT COALESCE(MAX(seq), 0) AS m FROM account_changelog")
    return int(cur.fetchone()["m"])


def snapshot_accounts(cur, start: int, end: int) -> tuple[list[dict], int]:
    # Consistent read of range + durable change position.
    cur.execute(
        "SELECT account_id, balance FROM accounts "
        "WHERE SUBSTRING(account_id FROM 2)::INT BETWEEN %s AND %s "
        "ORDER BY account_id FOR SHARE",
        (start, end),
    )
    rows = [
        {"account_id": r["account_id"], "balance": float(Decimal(r["balance"]))}
        for r in cur.fetchall()
    ]
    pos = changelog_max_seq(cur)
    return rows, pos


def fetch_changes(cur, after_seq: int, start: int, end: int, limit: int = 500) -> list[dict]:
    cur.execute(
        "SELECT seq, account_id, balance, op FROM account_changelog "
        "WHERE seq > %s "
        "AND SUBSTRING(account_id FROM 2)::INT BETWEEN %s AND %s "
        "ORDER BY seq ASC LIMIT %s",
        (after_seq, start, end, limit),
    )
    out = []
    for r in cur.fetchall():
        out.append(
            {
                "seq": int(r["seq"]),
                "account_id": r["account_id"],
                "balance": float(Decimal(r["balance"])),
                "op": r["op"],
            }
        )
    return out


def apply_snapshot_batch(cur, rows: list[dict]) -> int:
    n = 0
    for row in rows:
        cur.execute(
            "INSERT INTO accounts(account_id, balance) VALUES (%s, %s) "
            "ON CONFLICT (account_id) DO UPDATE SET balance = EXCLUDED.balance",
            (row["account_id"], row["balance"]),
        )
        n += 1
    return n


def apply_changes_idempotent(cur, changes: list[dict]) -> int:
    applied = 0
    for ch in changes:
        cur.execute(
            "SELECT 1 FROM applied_changes WHERE source_seq = %s",
            (ch["seq"],),
        )
        if cur.fetchone():
            continue
        if ch["op"] == "DELETE":
            cur.execute("DELETE FROM accounts WHERE account_id = %s", (ch["account_id"],))
        else:
            cur.execute(
                "INSERT INTO accounts(account_id, balance) VALUES (%s, %s) "
                "ON CONFLICT (account_id) DO UPDATE SET balance = EXCLUDED.balance",
                (ch["account_id"], ch["balance"]),
            )
        cur.execute(
            "INSERT INTO applied_changes(source_seq, account_id) VALUES (%s, %s)",
            (ch["seq"], ch["account_id"]),
        )
        applied += 1
    return applied


def delete_account_range(cur, start: int, end: int) -> int:
    cur.execute(
        "DELETE FROM accounts "
        "WHERE SUBSTRING(account_id FROM 2)::INT BETWEEN %s AND %s",
        (start, end),
    )
    return cur.rowcount or 0


def count_range(cur, start: int, end: int) -> int:
    cur.execute(
        "SELECT COUNT(*) AS n FROM accounts "
        "WHERE SUBSTRING(account_id FROM 2)::INT BETWEEN %s AND %s",
        (start, end),
    )
    return int(cur.fetchone()["n"])


def sum_range(cur, start: int, end: int) -> Decimal:
    cur.execute(
        "SELECT COALESCE(SUM(balance),0) AS s FROM accounts "
        "WHERE SUBSTRING(account_id FROM 2)::INT BETWEEN %s AND %s",
        (start, end),
    )
    return Decimal(str(cur.fetchone()["s"]))
