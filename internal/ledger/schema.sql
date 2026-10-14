-- Double-entry ledger for card authorisations.
--
-- Every money movement is a ledger entry with postings that sum to zero. The
-- database enforces it: a deferred constraint trigger checks each entry at
-- COMMIT, so no code path can commit an unbalanced entry.
--
-- Accounts per card:
--   available  open-to-buy credit
--   held       authorised, not yet captured
--   posted     captured spend the cardholder owes
-- plus one issuer "credit_line" account that funds every card's limit.
--
--   open card      credit_line -L   available +L
--   hold A         available   -A   held      +A
--   capture C      held        -C   posted    +C
--   reversal R     held        -R   available +R
--   refund F       posted      -F   available +F

CREATE TABLE IF NOT EXISTS cards (
    id               text PRIMARY KEY,
    credit_limit     bigint NOT NULL CHECK (credit_limit >= 0),
    per_txn_limit    bigint NOT NULL CHECK (per_txn_limit > 0),
    daily_limit      bigint NOT NULL CHECK (daily_limit > 0),
    velocity_per_min int    NOT NULL CHECK (velocity_per_min > 0),
    status           text   NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'blocked'))
);

CREATE TABLE IF NOT EXISTS accounts (
    id      bigserial PRIMARY KEY,
    card_id text REFERENCES cards (id),     -- NULL for the issuer account
    kind    text   NOT NULL CHECK (kind IN ('credit_line', 'available', 'held', 'posted')),
    balance bigint NOT NULL DEFAULT 0,
    UNIQUE (card_id, kind)
);
CREATE UNIQUE INDEX IF NOT EXISTS one_credit_line ON accounts (kind) WHERE card_id IS NULL;

CREATE TABLE IF NOT EXISTS authorizations (
    id         uuid PRIMARY KEY,
    card_id    text   NOT NULL REFERENCES cards (id),
    merchant   text   NOT NULL,
    amount     bigint NOT NULL CHECK (amount > 0),
    captured   bigint NOT NULL DEFAULT 0,
    reversed   bigint NOT NULL DEFAULT 0,
    refunded   bigint NOT NULL DEFAULT 0,
    status     text   NOT NULL CHECK (status IN ('approved', 'declined', 'closed')),
    decline    text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (captured >= 0 AND reversed >= 0 AND refunded >= 0),
    CHECK (captured + reversed <= amount),
    CHECK (refunded <= captured)
);
CREATE INDEX IF NOT EXISTS auth_card_time ON authorizations (card_id, created_at);

CREATE TABLE IF NOT EXISTS entries (
    id              bigserial PRIMARY KEY,
    kind            text   NOT NULL CHECK (kind IN ('open', 'hold', 'capture', 'reversal', 'refund')),
    authorization_id uuid  REFERENCES authorizations (id),
    amount          bigint NOT NULL CHECK (amount > 0),
    idempotency_key text   UNIQUE,  -- at most one entry per client request, ever
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS postings (
    id         bigserial PRIMARY KEY,
    entry_id   bigint NOT NULL REFERENCES entries (id),
    account_id bigint NOT NULL REFERENCES accounts (id),
    amount     bigint NOT NULL CHECK (amount <> 0)
);
CREATE INDEX IF NOT EXISTS postings_entry ON postings (entry_id);
CREATE INDEX IF NOT EXISTS postings_account ON postings (account_id);

-- Idempotency: the stored response for every write, keyed by the client's
-- Idempotency-Key. Written in the same transaction as the ledger entry.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key          text PRIMARY KEY,
    request_hash bytea NOT NULL,
    status       int   NOT NULL,
    response     json  NOT NULL, -- json, not jsonb: replays are byte-identical
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION check_entry_balanced() RETURNS trigger AS $$
DECLARE total bigint;
BEGIN
    SELECT coalesce(sum(amount), 0) INTO total FROM postings WHERE entry_id = NEW.entry_id;
    IF total <> 0 THEN
        RAISE EXCEPTION 'ledger entry % is unbalanced by %', NEW.entry_id, total USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS entry_balanced ON postings;
CREATE CONSTRAINT TRIGGER entry_balanced AFTER INSERT ON postings
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_entry_balanced();
