package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func testLedger(t *testing.T) *Ledger {
	admin := os.Getenv("DATABASE_URL")
	if admin == "" {
		admin = "postgres://localhost:5432/postgres?sslmode=disable"
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Skip("no Postgres: " + err.Error())
	}
	db := "ledger_test_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+db); err != nil {
		t.Skip(err.Error())
	}
	t.Cleanup(func() {
		conn.Exec(context.Background(), "DROP DATABASE "+db+" WITH (FORCE)")
		conn.Close(context.Background())
	})
	l, err := Open(ctx, strings.Replace(admin, "/postgres?", "/"+db+"?", 1), 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.Close)
	return l
}

// do runs an operation under an idempotency key, like the API does.
func do(t *testing.T, l *Ledger, key string, fn func(tx pgx.Tx) (Response, error)) (int, map[string]any, bool) {
	t.Helper()
	st, body, replayed, err := l.Idempotent(context.Background(), key, []byte(key), fn)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	json.Unmarshal(body, &m)
	return st, m, replayed
}

func card(t *testing.T, l *Ledger, id string, limit, perTxn, daily int64, velocity int) {
	ctx := context.Background()
	if st, m, _ := do(t, l, "card-"+id, func(tx pgx.Tx) (Response, error) {
		return l.CreateCard(ctx, tx, "card-"+id, Card{ID: id, CreditLimit: limit, PerTxnLimit: perTxn, DailyLimit: daily, VelocityPerMin: velocity})
	}); st != 201 {
		t.Fatalf("create card: %d %v", st, m)
	}
}

func hold(t *testing.T, l *Ledger, key, c string, amount int64) map[string]any {
	_, m, _ := do(t, l, key, func(tx pgx.Tx) (Response, error) {
		return l.Hold(context.Background(), tx, key, c, "shop", amount)
	})
	return m
}

func TestFlowAndEndOfDay(t *testing.T) {
	l := testLedger(t)
	ctx := context.Background()
	card(t, l, "c1", 10_000, 5_000, 9_000, 100)
	a := hold(t, l, "h1", "c1", 4_000)
	id := uuid.MustParse(a["id"].(string))
	for _, step := range []struct {
		key string
		fn  func(context.Context, pgx.Tx, string, uuid.UUID, int64) (Response, error)
		amt int64
	}{{"cap1", l.Capture, 1_500}, {"cap2", l.Capture, 1_000}, {"rev", l.Reverse, 0}, {"ref", l.Refund, 700}} {
		if st, m, _ := do(t, l, step.key, func(tx pgx.Tx) (Response, error) { return step.fn(ctx, tx, step.key, id, step.amt) }); st != 200 {
			t.Fatalf("%s: %d %v", step.key, st, m)
		}
	}
	got, _ := l.Get(ctx, id)
	if got.Captured != 2_500 || got.Reversed != 1_500 || got.Refunded != 700 || got.Status != "closed" {
		t.Fatalf("unexpected state %+v", got)
	}
	bal, _ := l.Balance(ctx, "c1")
	if bal["available"]+bal["held"]+bal["posted"] != 10_000 || bal["posted"] != 1_800 || bal["held"] != 0 {
		t.Fatalf("balances %v", bal)
	}
	r, err := l.EndOfDay(ctx)
	if err != nil || !r.Balanced || r.Sum != 0 {
		t.Fatalf("end of day: %+v %v", r, err)
	}
}

func TestDeclines(t *testing.T) {
	l := testLedger(t)
	card(t, l, "c1", 10_000, 3_000, 5_000, 10)
	for i, c := range []struct {
		amount int64
		want   string
	}{{3_500, "per_transaction_limit"}, {2_500, ""}, {2_500, ""}, {100, "daily_limit"}} {
		a := hold(t, l, fmt.Sprintf("h-%d-%d", i, c.amount), "c1", c.amount)
		if got, _ := a["decline_reason"].(string); got != c.want {
			t.Fatalf("hold %d: decline %q, want %q (%v)", c.amount, got, c.want, a)
		}
	}
	card(t, l, "c2", 1_000_000, 1_000, 1_000_000, 2)
	hold(t, l, "v1", "c2", 10)
	hold(t, l, "v2", "c2", 10)
	if a := hold(t, l, "v3", "c2", 10); a["decline_reason"] != "velocity" {
		t.Fatalf("third authorisation in a minute should hit velocity: %v", a)
	}
	card(t, l, "c3", 500, 1_000, 1_000_000, 100)
	if a := hold(t, l, "f1", "c3", 600); a["decline_reason"] != "insufficient_funds" {
		t.Fatalf("want insufficient_funds: %v", a)
	}
}

func TestConcurrentDuplicatesPostOnce(t *testing.T) {
	l := testLedger(t)
	card(t, l, "c1", 100_000, 10_000, 100_000, 1000)
	var wg sync.WaitGroup
	ids := make([]string, 20)
	for i := range ids {
		wg.Go(func() { ids[i] = hold(t, l, "same-key", "c1", 1_234)["id"].(string) })
	}
	wg.Wait()
	for _, id := range ids {
		if id != ids[0] {
			t.Fatalf("duplicates got different authorisations: %v", ids)
		}
	}
	if bal, _ := l.Balance(context.Background(), "c1"); bal["held"] != 1_234 {
		t.Fatalf("20 copies of one request must hold once, held = %d", bal["held"])
	}
	if _, _, err := func() (int, []byte, error) {
		st, b, _, err := l.Idempotent(context.Background(), "same-key", []byte("a different request"), func(pgx.Tx) (Response, error) { return Response{}, nil })
		return st, b, err
	}(); err != ErrKeyReused {
		t.Fatalf("reusing a key for another request must fail, got %v", err)
	}
}

func TestDatabaseRejectsUnbalancedEntry(t *testing.T) {
	l := testLedger(t)
	card(t, l, "c1", 1_000, 1_000, 1_000, 10)
	err := pgx.BeginFunc(context.Background(), l.db, func(tx pgx.Tx) error {
		a, err := l.cardAccounts(context.Background(), tx, "c1")
		if err != nil {
			return err
		}
		return post(context.Background(), tx, "hold", nil, 100, "bad", leg{a.available, -100}, leg{a.held, 99})
	})
	if !IsUnbalanced(err) {
		t.Fatalf("an unbalanced entry must be refused at commit, got %v", err)
	}
}
