package ledger

import (
	"context"
	"time"
)

// Report is the end-of-day proof.
type Report struct {
	At                  time.Time `json:"at"`
	Entries             int64     `json:"entries"`
	Postings            int64     `json:"postings"`
	Sum                 int64     `json:"sum_of_all_postings"`   // must be 0
	UnbalancedEntries   int64     `json:"unbalanced_entries"`    // entries whose postings do not net to 0
	EntriesWithoutLegs  int64     `json:"entries_without_postings"`
	AccountMismatches   int64     `json:"account_balance_mismatches"` // stored balance != sum of its postings
	CardIdentity        int64     `json:"cards_breaking_identity"`    // available + held + posted != credit limit
	HoldMismatches      int64     `json:"cards_held_mismatch"`        // held != open authorisation remainders
	PostedMismatches    int64     `json:"cards_posted_mismatch"`      // posted != captured - refunded
	DuplicateOperations int64     `json:"duplicate_operations"`       // ledger entries sharing a client request
	Cards               int64     `json:"cards"`
	Authorizations      int64     `json:"authorizations"`
	Balanced            bool      `json:"balanced"`
}

// EndOfDay proves the ledger: every entry nets to zero, so all postings sum
// to zero; every stored balance equals the sum of its postings; every card's
// accounts add up to its credit limit; and the held and posted balances agree
// with the authorisation records they summarise.
func (l *Ledger) EndOfDay(ctx context.Context) (Report, error) {
	r := Report{At: time.Now().UTC()}
	err := l.db.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM entries),
		(SELECT count(*) FROM postings),
		(SELECT coalesce(sum(amount), 0) FROM postings),
		(SELECT count(*) FROM (SELECT entry_id FROM postings GROUP BY entry_id HAVING sum(amount) <> 0) x),
		(SELECT count(*) FROM entries e WHERE NOT EXISTS (SELECT 1 FROM postings p WHERE p.entry_id = e.id)),
		(SELECT count(*) FROM accounts a LEFT JOIN (SELECT account_id, sum(amount) s FROM postings GROUP BY account_id) p ON p.account_id = a.id
		  WHERE a.balance <> coalesce(p.s, 0)),
		(SELECT count(*) FROM cards c JOIN (SELECT card_id, sum(balance) s FROM accounts WHERE card_id IS NOT NULL GROUP BY card_id) a ON a.card_id = c.id
		  WHERE a.s <> c.credit_limit),
		(SELECT count(*) FROM accounts a LEFT JOIN (SELECT card_id, sum(amount - captured - reversed) s FROM authorizations WHERE status <> 'declined' GROUP BY card_id) h
		  ON h.card_id = a.card_id WHERE a.kind = 'held' AND a.balance <> coalesce(h.s, 0)),
		(SELECT count(*) FROM accounts a LEFT JOIN (SELECT card_id, sum(captured - refunded) s FROM authorizations GROUP BY card_id) p
		  ON p.card_id = a.card_id WHERE a.kind = 'posted' AND a.balance <> coalesce(p.s, 0)),
		(SELECT count(*) FROM (SELECT idempotency_key FROM entries WHERE idempotency_key IS NOT NULL GROUP BY idempotency_key HAVING count(*) > 1) d),
		(SELECT count(*) FROM cards),
		(SELECT count(*) FROM authorizations)`).
		Scan(&r.Entries, &r.Postings, &r.Sum, &r.UnbalancedEntries, &r.EntriesWithoutLegs, &r.AccountMismatches,
			&r.CardIdentity, &r.HoldMismatches, &r.PostedMismatches, &r.DuplicateOperations, &r.Cards, &r.Authorizations)
	r.Balanced = err == nil && r.Sum == 0 && r.UnbalancedEntries == 0 && r.EntriesWithoutLegs == 0 && r.AccountMismatches == 0 &&
		r.CardIdentity == 0 && r.HoldMismatches == 0 && r.PostedMismatches == 0 && r.DuplicateOperations == 0
	return r, err
}
