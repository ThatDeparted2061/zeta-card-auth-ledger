// Package ledger implements card authorisation on a double-entry ledger in
// Postgres: holds, full and partial captures, reversals and refunds, with
// per-card spending limits and velocity checks, and idempotency keys that
// make every write safe to retry.
package ledger

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

// Ledger is safe for concurrent use.
type Ledger struct {
	db       *pgxpool.Pool
	accounts sync.Map // card id -> cardAccounts (never change once created)
	line     int64    // issuer credit-line account
}

type cardAccounts struct{ available, held, posted int64 }

func Open(ctx context.Context, url string, maxConns int32) (*Ledger, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(ctx); err != nil {
		db.Close()
		return nil, err
	}
	l := &Ledger{db: db}
	if _, err := db.Exec(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	err = db.QueryRow(ctx, `
		WITH ins AS (INSERT INTO accounts (kind) VALUES ('credit_line') ON CONFLICT DO NOTHING RETURNING id)
		SELECT id FROM ins UNION ALL SELECT id FROM accounts WHERE card_id IS NULL AND kind = 'credit_line' LIMIT 1`).Scan(&l.line)
	return l, err
}

func (l *Ledger) Close() { l.db.Close() }

// Response is what a write returns, and what a replay of it returns again.
type Response struct {
	Status int
	Body   any
}

// Problem is a client error that is part of the deterministic outcome of a
// request (and so is stored and replayed like a success).
type Problem struct {
	Status  int    `json:"-"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (p *Problem) Error() string { return p.Code + ": " + p.Message }

func problem(status int, code, format string, a ...any) *Problem {
	return &Problem{Status: status, Code: code, Message: fmt.Sprintf(format, a...)}
}

// ErrKeyReused means an idempotency key came back with a different request.
var ErrKeyReused = errors.New("idempotency key was already used for a different request")

// Idempotent runs fn exactly once per key. The key, the ledger writes and the
// stored response commit in one transaction, so:
//   - a retry after a crash before COMMIT runs fn again (nothing was kept);
//   - a retry after COMMIT gets the stored response (nothing runs twice);
//   - two concurrent copies serialise on the key: the second waits for the
//     first and then replays its response.
func (l *Ledger) Idempotent(ctx context.Context, key string, hash []byte, fn func(tx pgx.Tx) (Response, error)) (status int, body []byte, replayed bool, err error) {
	err = pgx.BeginFunc(ctx, l.db, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO idempotency_keys (key, request_hash, status, response) VALUES ($1, $2, 0, '{}') ON CONFLICT (key) DO NOTHING`, key, hash)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			var stored []byte
			if err := tx.QueryRow(ctx, `SELECT request_hash, status, response FROM idempotency_keys WHERE key = $1`, key).Scan(&stored, &status, &body); err != nil {
				return err
			}
			if string(stored) != string(hash) {
				return ErrKeyReused
			}
			replayed = true
			return nil
		}
		resp, err := fn(tx)
		var p *Problem
		if errors.As(err, &p) {
			resp, err = Response{Status: p.Status, Body: map[string]any{"error": p}}, nil
		}
		if err != nil {
			return err // not stored: the client may retry
		}
		status = resp.Status
		if body, err = json.Marshal(resp.Body); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE idempotency_keys SET status = $2, response = $3 WHERE key = $1`, key, status, body)
		return err
	})
	return status, body, replayed, err
}

type leg struct {
	account int64
	amount  int64
}

// post writes one balanced entry and moves the account balances.
func post(ctx context.Context, tx pgx.Tx, kind string, auth *uuid.UUID, amount int64, key string, legs ...leg) error {
	var entry int64
	if err := tx.QueryRow(ctx, `INSERT INTO entries (kind, authorization_id, amount, idempotency_key) VALUES ($1, $2, $3, $4) RETURNING id`,
		kind, auth, amount, key).Scan(&entry); err != nil {
		return err
	}
	b := &pgx.Batch{}
	for _, lg := range legs {
		b.Queue(`INSERT INTO postings (entry_id, account_id, amount) VALUES ($1, $2, $3)`, entry, lg.account, lg.amount)
		b.Queue(`UPDATE accounts SET balance = balance + $2 WHERE id = $1`, lg.account, lg.amount)
	}
	return tx.SendBatch(ctx, b).Close()
}

func (l *Ledger) cardAccounts(ctx context.Context, tx pgx.Tx, card string) (cardAccounts, error) {
	if v, ok := l.accounts.Load(card); ok {
		return v.(cardAccounts), nil
	}
	var a cardAccounts
	rows, err := tx.Query(ctx, `SELECT kind, id FROM accounts WHERE card_id = $1`, card)
	if err != nil {
		return a, err
	}
	for rows.Next() {
		var kind string
		var id int64
		if err := rows.Scan(&kind, &id); err != nil {
			return a, err
		}
		switch kind {
		case "available":
			a.available = id
		case "held":
			a.held = id
		case "posted":
			a.posted = id
		}
	}
	if err := rows.Err(); err != nil {
		return a, err
	}
	if a.available == 0 {
		return a, problem(http.StatusNotFound, "card_not_found", "card %s does not exist", card)
	}
	l.accounts.Store(card, a)
	return a, nil
}

// Card is a card and its limits.
type Card struct {
	ID             string `json:"id"`
	CreditLimit    int64  `json:"credit_limit"`
	PerTxnLimit    int64  `json:"per_txn_limit"`
	DailyLimit     int64  `json:"daily_limit"`
	VelocityPerMin int    `json:"velocity_per_min"`
}

// CreateCard opens a card: its three accounts, funded from the credit line.
func (l *Ledger) CreateCard(ctx context.Context, tx pgx.Tx, key string, c Card) (Response, error) {
	if c.ID == "" || c.CreditLimit < 0 || c.PerTxnLimit <= 0 || c.DailyLimit <= 0 || c.VelocityPerMin <= 0 {
		return Response{}, problem(http.StatusBadRequest, "invalid_card", "id, credit_limit >= 0 and positive per_txn_limit, daily_limit, velocity_per_min are required")
	}
	tag, err := tx.Exec(ctx, `INSERT INTO cards (id, credit_limit, per_txn_limit, daily_limit, velocity_per_min) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`,
		c.ID, c.CreditLimit, c.PerTxnLimit, c.DailyLimit, c.VelocityPerMin)
	if err != nil {
		return Response{}, err
	}
	if tag.RowsAffected() == 0 {
		return Response{}, problem(http.StatusConflict, "card_exists", "card %s already exists", c.ID)
	}
	var a cardAccounts
	if _, err := tx.Exec(ctx, `INSERT INTO accounts (card_id, kind) VALUES ($1, 'available'), ($1, 'held'), ($1, 'posted')`, c.ID); err != nil {
		return Response{}, err
	}
	if err := tx.QueryRow(ctx, `SELECT
		(SELECT id FROM accounts WHERE card_id = $1 AND kind = 'available'),
		(SELECT id FROM accounts WHERE card_id = $1 AND kind = 'held'),
		(SELECT id FROM accounts WHERE card_id = $1 AND kind = 'posted')`, c.ID).Scan(&a.available, &a.held, &a.posted); err != nil {
		return Response{}, err
	}
	if c.CreditLimit > 0 {
		if err := post(ctx, tx, "open", nil, c.CreditLimit, key, leg{l.line, -c.CreditLimit}, leg{a.available, c.CreditLimit}); err != nil {
			return Response{}, err
		}
	}
	return Response{http.StatusCreated, c}, nil
}

// Authorization is the state of one authorisation.
type Authorization struct {
	ID        uuid.UUID `json:"id"`
	CardID    string    `json:"card_id"`
	Merchant  string    `json:"merchant"`
	Amount    int64     `json:"amount"`
	Captured  int64     `json:"captured"`
	Reversed  int64     `json:"reversed"`
	Refunded  int64     `json:"refunded"`
	Status    string    `json:"status"` // approved | declined | closed
	Decline   string    `json:"decline_reason,omitempty"`
	Available *int64    `json:"available_after,omitempty"` // on holds: open-to-buy after this authorisation
}

// Hold authorises amount on a card: limits and velocity are checked under a
// row lock on the card, so concurrent authorisations for one card queue
// instead of racing past a limit.
func (l *Ledger) Hold(ctx context.Context, tx pgx.Tx, key, card, merchant string, amount int64) (Response, error) {
	if amount <= 0 || merchant == "" {
		return Response{}, problem(http.StatusBadRequest, "invalid_request", "amount must be positive and merchant set")
	}
	var c Card
	var status string
	err := tx.QueryRow(ctx, `SELECT credit_limit, per_txn_limit, daily_limit, velocity_per_min, status FROM cards WHERE id = $1 FOR UPDATE`, card).
		Scan(&c.CreditLimit, &c.PerTxnLimit, &c.DailyLimit, &c.VelocityPerMin, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return Response{}, problem(http.StatusNotFound, "card_not_found", "card %s does not exist", card)
	}
	if err != nil {
		return Response{}, err
	}
	acc, err := l.cardAccounts(ctx, tx, card)
	if err != nil {
		return Response{}, err
	}
	var recent int
	var today, available int64
	if err := tx.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM authorizations WHERE card_id = $1 AND created_at > now() - interval '1 minute'),
		(SELECT coalesce(sum(amount - reversed), 0) FROM authorizations WHERE card_id = $1 AND status <> 'declined' AND created_at >= date_trunc('day', now())),
		(SELECT balance FROM accounts WHERE id = $2)`, card, acc.available).Scan(&recent, &today, &available); err != nil {
		return Response{}, err
	}
	a := Authorization{ID: uuid.New(), CardID: card, Merchant: merchant, Amount: amount, Status: "approved"}
	switch {
	case status != "active":
		a.Decline = "card_blocked"
	case amount > c.PerTxnLimit:
		a.Decline = "per_transaction_limit"
	case recent >= c.VelocityPerMin:
		a.Decline = "velocity"
	case today+amount > c.DailyLimit:
		a.Decline = "daily_limit"
	case amount > available:
		a.Decline = "insufficient_funds"
	}
	if a.Decline != "" {
		a.Status = "declined"
	}
	if _, err := tx.Exec(ctx, `INSERT INTO authorizations (id, card_id, merchant, amount, status, decline) VALUES ($1, $2, $3, $4, $5, nullif($6, ''))`,
		a.ID, card, merchant, amount, a.Status, a.Decline); err != nil {
		return Response{}, err
	}
	if a.Status == "approved" {
		if err := post(ctx, tx, "hold", &a.ID, amount, key, leg{acc.available, -amount}, leg{acc.held, amount}); err != nil {
			return Response{}, err
		}
		available -= amount
	}
	a.Available = &available
	return Response{http.StatusCreated, a}, nil
}

// lockAuth loads an authorisation for update.
func (l *Ledger) lockAuth(ctx context.Context, tx pgx.Tx, id uuid.UUID) (Authorization, cardAccounts, error) {
	var a Authorization
	var decline *string
	err := tx.QueryRow(ctx, `SELECT id, card_id, merchant, amount, captured, reversed, refunded, status, decline FROM authorizations WHERE id = $1 FOR UPDATE`, id).
		Scan(&a.ID, &a.CardID, &a.Merchant, &a.Amount, &a.Captured, &a.Reversed, &a.Refunded, &a.Status, &decline)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, cardAccounts{}, problem(http.StatusNotFound, "authorization_not_found", "authorization %s does not exist", id)
	}
	if err != nil {
		return a, cardAccounts{}, err
	}
	if decline != nil {
		a.Decline = *decline
	}
	acc, err := l.cardAccounts(ctx, tx, a.CardID)
	return a, acc, err
}

func (l *Ledger) save(ctx context.Context, tx pgx.Tx, a *Authorization) error {
	if a.Status == "approved" && a.Captured+a.Reversed == a.Amount {
		a.Status = "closed"
	}
	_, err := tx.Exec(ctx, `UPDATE authorizations SET captured = $2, reversed = $3, refunded = $4, status = $5 WHERE id = $1`,
		a.ID, a.Captured, a.Reversed, a.Refunded, a.Status)
	return err
}

// Capture moves part or all of the remaining hold to posted spend.
func (l *Ledger) Capture(ctx context.Context, tx pgx.Tx, key string, id uuid.UUID, amount int64) (Response, error) {
	a, acc, err := l.lockAuth(ctx, tx, id)
	if err != nil {
		return Response{}, err
	}
	remaining := a.Amount - a.Captured - a.Reversed
	switch {
	case a.Status == "declined":
		return Response{}, problem(http.StatusConflict, "declined", "a declined authorization cannot be captured")
	case amount <= 0 || amount > remaining:
		return Response{}, problem(http.StatusConflict, "exceeds_hold", "capture of %d exceeds the remaining hold of %d", amount, remaining)
	}
	if err := post(ctx, tx, "capture", &a.ID, amount, key, leg{acc.held, -amount}, leg{acc.posted, amount}); err != nil {
		return Response{}, err
	}
	a.Captured += amount
	return Response{http.StatusOK, a}, l.save(ctx, tx, &a)
}

// Reverse releases part or all (amount 0) of the remaining hold.
func (l *Ledger) Reverse(ctx context.Context, tx pgx.Tx, key string, id uuid.UUID, amount int64) (Response, error) {
	a, acc, err := l.lockAuth(ctx, tx, id)
	if err != nil {
		return Response{}, err
	}
	remaining := a.Amount - a.Captured - a.Reversed
	if amount == 0 {
		amount = remaining
	}
	switch {
	case a.Status == "declined":
		return Response{}, problem(http.StatusConflict, "declined", "a declined authorization holds nothing")
	case amount <= 0 || amount > remaining:
		return Response{}, problem(http.StatusConflict, "exceeds_hold", "reversal of %d exceeds the remaining hold of %d", amount, remaining)
	}
	if err := post(ctx, tx, "reversal", &a.ID, amount, key, leg{acc.held, -amount}, leg{acc.available, amount}); err != nil {
		return Response{}, err
	}
	a.Reversed += amount
	return Response{http.StatusOK, a}, l.save(ctx, tx, &a)
}

// Refund returns captured money to the card's available credit.
func (l *Ledger) Refund(ctx context.Context, tx pgx.Tx, key string, id uuid.UUID, amount int64) (Response, error) {
	a, acc, err := l.lockAuth(ctx, tx, id)
	if err != nil {
		return Response{}, err
	}
	if amount <= 0 || amount > a.Captured-a.Refunded {
		return Response{}, problem(http.StatusConflict, "exceeds_captured", "refund of %d exceeds the refundable %d", amount, a.Captured-a.Refunded)
	}
	if err := post(ctx, tx, "refund", &a.ID, amount, key, leg{acc.posted, -amount}, leg{acc.available, amount}); err != nil {
		return Response{}, err
	}
	a.Refunded += amount
	return Response{http.StatusOK, a}, l.save(ctx, tx, &a)
}

// Get returns an authorisation.
func (l *Ledger) Get(ctx context.Context, id uuid.UUID) (Authorization, error) {
	var a Authorization
	err := pgx.BeginFunc(ctx, l.db, func(tx pgx.Tx) error {
		var err error
		a, _, err = l.lockAuth(ctx, tx, id)
		return err
	})
	return a, err
}

// Balance returns a card's three account balances.
func (l *Ledger) Balance(ctx context.Context, card string) (map[string]int64, error) {
	rows, err := l.db.Query(ctx, `SELECT kind, balance FROM accounts WHERE card_id = $1`, card)
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	_, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (struct{}, error) {
		var k string
		var b int64
		err := r.Scan(&k, &b)
		out[k] = b
		return struct{}{}, err
	})
	if len(out) == 0 && err == nil {
		return nil, problem(http.StatusNotFound, "card_not_found", "card %s does not exist", card)
	}
	return out, err
}

// IsUnbalanced reports whether err is the database refusing an unbalanced entry.
func IsUnbalanced(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23514"
}
