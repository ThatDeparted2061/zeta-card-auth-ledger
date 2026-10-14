// Command chaos runs card flows against the ledger while killing it.
//
// Clients authorise, capture (fully or partly), reverse and refund, retrying
// every request with the same Idempotency-Key until they get a definitive
// answer, as a payment network does. On top of that:
//
//   - the service is killed with SIGKILL every few seconds, mid-transaction,
//     and restarted;
//   - a share of requests are sent twice at once (duplicate network retries);
//   - at the end, a sample of old requests is replayed.
//
// Then it checks that every authorisation in the database matches exactly
// what clients were told, that duplicates and replays changed nothing, and
// that the end-of-day proof balances to zero.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type config struct {
	bin, db, addr, out  string
	cards, clients      int
	duration, killEvery time.Duration
	dup                 float64
	replay              int
}

type auth struct {
	ID       string `json:"id"`
	CardID   string `json:"card_id"`
	Amount   int64  `json:"amount"`
	Captured int64  `json:"captured"`
	Reversed int64  `json:"reversed"`
	Refunded int64  `json:"refunded"`
	Status   string `json:"status"`
}

type sent struct {
	method, path, key string
	body              []byte
	status            int
	resp              []byte
}

// Result is the run's outcome.
type Result struct {
	DurationSeconds    float64         `json:"duration_seconds"`
	Kills              int64           `json:"service_kills"`
	Requests           int64           `json:"requests"`             // distinct client requests (idempotency keys)
	Attempts           int64           `json:"http_attempts"`        // including retries and duplicates
	Retries            int64           `json:"retries_after_errors"` // connection errors / 503 while the service was down
	Duplicates         int64           `json:"duplicate_sends"`      // the same request sent twice at once
	DuplicateMismatch  int64           `json:"duplicate_mismatches"` // the two copies got different answers
	Replays            int64           `json:"replayed_requests"`
	ReplayMismatch     int64           `json:"replay_mismatches"`
	ReplayHeaders      int64           `json:"responses_marked_replay"`
	Authorizations     int64           `json:"authorizations"`
	Approved           int64           `json:"approved"`
	Captures           int64           `json:"captures"`
	Reversals          int64           `json:"reversals"`
	Refunds            int64           `json:"refunds"`
	StateMismatches    int64           `json:"authorization_state_mismatches"`
	EOD                json.RawMessage `json:"end_of_day"`
	EODAfterReplay     json.RawMessage `json:"end_of_day_after_replay"`
	LedgerUnchanged    bool            `json:"ledger_unchanged_by_replay"`
	Balanced           bool            `json:"balanced"`
	OK                 bool            `json:"ok"`
	FirstMismatchNotes []string        `json:"notes,omitempty"`
}

func main() {
	var c config
	flag.StringVar(&c.bin, "bin", "bin/ledger", "ledger binary")
	flag.StringVar(&c.db, "db", "postgres://localhost:5432/card_ledger_chaos?sslmode=disable", "Postgres URL (a scratch database)")
	flag.StringVar(&c.addr, "addr", "127.0.0.1:8061", "address for the ledger under test")
	flag.StringVar(&c.out, "out", "", "write the JSON result here")
	flag.IntVar(&c.cards, "cards", 300, "cards")
	flag.IntVar(&c.clients, "clients", 48, "concurrent clients")
	flag.DurationVar(&c.duration, "duration", 2*time.Minute, "how long to run flows")
	flag.DurationVar(&c.killEvery, "kill-every", 6*time.Second, "mean time between SIGKILLs")
	flag.Float64Var(&c.dup, "dup", 0.2, "share of requests also sent as a concurrent duplicate")
	flag.IntVar(&c.replay, "replay", 2000, "old requests to replay at the end")
	flag.Parse()

	r := run(c)
	b, _ := json.MarshalIndent(r, "", "  ")
	if c.out != "" {
		os.MkdirAll(filepath.Dir(c.out), 0o755)
		os.WriteFile(c.out, b, 0o644)
	}
	fmt.Println(string(b))
	if !r.OK {
		os.Exit(1)
	}
}

// service manages the ledger process.
type service struct {
	c   config
	mu  sync.Mutex
	cmd *exec.Cmd
}

func (s *service) start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd := exec.Command(s.c.bin, "-addr", s.c.addr, "-db", s.c.db)
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	s.cmd = cmd
	go cmd.Wait()
	for range 100 {
		if resp, err := http.Get("http://" + s.c.addr + "/healthz"); err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	log.Fatal("ledger did not start")
}

func (s *service) kill() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cmd.Process.Signal(syscall.SIGKILL)
}

type client struct {
	base                                 string
	http                                 *http.Client
	dup                                  float64
	attempts, retries, duplicates, dupMM atomic.Int64
	replayHeaders                        atomic.Int64
	logMu                                sync.Mutex
	log                                  []sent
}

// call sends one request until it gets a definitive answer (anything but a
// network error or 503), sometimes racing a duplicate copy alongside.
func (cl *client) call(method, path, key string, body any, rng *rand.Rand) (int, []byte) {
	raw, _ := json.Marshal(body)
	var dupCh chan [2]any
	if rng.Float64() < cl.dup {
		cl.duplicates.Add(1)
		dupCh = make(chan [2]any, 1)
		go func() {
			st, b := cl.send(method, path, key, raw)
			dupCh <- [2]any{st, b}
		}()
	}
	st, b := cl.send(method, path, key, raw)
	if dupCh != nil {
		d := <-dupCh
		if d[0].(int) != st || !bytes.Equal(d[1].([]byte), b) {
			cl.dupMM.Add(1)
		}
	}
	cl.logMu.Lock()
	cl.log = append(cl.log, sent{method, path, key, raw, st, b})
	cl.logMu.Unlock()
	return st, b
}

func (cl *client) send(method, path, key string, raw []byte) (int, []byte) {
	for attempt := 0; ; attempt++ {
		cl.attempts.Add(1)
		req, _ := http.NewRequest(method, cl.base+path, bytes.NewReader(raw))
		req.Header.Set("Idempotency-Key", key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := cl.http.Do(req)
		if err == nil {
			b, rerr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if rerr == nil && resp.StatusCode != http.StatusServiceUnavailable {
				if resp.Header.Get("Idempotent-Replay") == "true" {
					cl.replayHeaders.Add(1)
				}
				return resp.StatusCode, b
			}
		}
		cl.retries.Add(1)
		time.Sleep(time.Duration(min(50*(attempt+1), 500)) * time.Millisecond)
	}
}

func run(c config) Result {
	svc := &service{c: c}
	svc.start()
	cl := &client{base: "http://" + c.addr, dup: c.dup, http: &http.Client{Timeout: 5 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: c.clients * 2}}}
	runID := time.Now().UnixNano()
	rng := rand.New(rand.NewPCG(uint64(runID), 1))
	for i := range c.cards {
		card := map[string]any{"id": fmt.Sprintf("card-%d-%d", runID, i), "credit_limit": 50_000_000, "per_txn_limit": 40_000, "daily_limit": 50_000_000, "velocity_per_min": 5000}
		if i%10 == 0 { // a tight card in every ten, so declines are exercised too
			card["credit_limit"], card["daily_limit"], card["velocity_per_min"] = 200_000, 100_000, 20
		}
		cl.call("POST", "/v1/cards", fmt.Sprintf("%d-card-%d", runID, i), card, rng)
	}

	var res Result
	var kills atomic.Int64
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), c.duration)
	defer cancel()
	go func() { // the killer
		krng := rand.New(rand.NewPCG(uint64(runID), 2))
		for {
			d := c.killEvery/2 + time.Duration(krng.Int64N(int64(c.killEvery)))
			select {
			case <-ctx.Done():
				return
			case <-time.After(d):
			}
			svc.kill()
			kills.Add(1)
			time.Sleep(200 * time.Millisecond)
			svc.start()
		}
	}()

	var mu sync.Mutex
	expected := map[string]*auth{} // what clients were told, per authorisation
	var wg sync.WaitGroup
	var nAuth, nApproved, nCap, nRev, nRef atomic.Int64
	for w := range c.clients {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(runID), uint64(w)+10))
			for i := 0; ctx.Err() == nil; i++ {
				key := func(op string) string { return fmt.Sprintf("%d-%d-%d-%s", runID, w, i, op) }
				card := fmt.Sprintf("card-%d-%d", runID, rng.IntN(c.cards))
				st, b := cl.call("POST", "/v1/authorizations", key("auth"), map[string]any{"card_id": card, "merchant": "m" + fmt.Sprint(rng.IntN(50)), "amount": 100 + rng.Int64N(20_000)}, rng)
				nAuth.Add(1)
				var a auth
				if st != 201 || json.Unmarshal(b, &a) != nil || a.Status != "approved" {
					continue
				}
				nApproved.Add(1)
				mu.Lock()
				expected[a.ID] = &a
				mu.Unlock()
				apply := func(path, op string, amount int64) {
					st, b := cl.call("POST", "/v1/authorizations/"+a.ID+"/"+path, key(op), map[string]any{"amount": amount}, rng)
					var got auth
					if st == 200 && json.Unmarshal(b, &got) == nil {
						mu.Lock()
						*expected[a.ID] = got
						mu.Unlock()
					}
				}
				switch x := rng.Float64(); {
				case x < 0.55: // full capture
					apply("captures", "cap", a.Amount)
					nCap.Add(1)
				case x < 0.80: // partial capture, then release the rest
					apply("captures", "cap", 1+rng.Int64N(a.Amount))
					apply("reversals", "rev", 0)
					nCap.Add(1)
					nRev.Add(1)
				default: // abandoned: reverse
					apply("reversals", "rev", 0)
					nRev.Add(1)
				}
				mu.Lock()
				captured := expected[a.ID].Captured
				mu.Unlock()
				if captured > 0 && rng.Float64() < 0.25 {
					apply("refunds", "ref", 1+rng.Int64N(captured))
					nRef.Add(1)
				}
			}
		})
	}
	wg.Wait()
	res.DurationSeconds = time.Since(start).Seconds()
	res.Kills = kills.Load()

	eod := func() (json.RawMessage, bool) {
		resp, err := http.Get(cl.base + "/v1/eod")
		if err != nil {
			return nil, false
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		var r struct{ Balanced bool }
		json.Unmarshal(b, &r)
		return b, r.Balanced
	}
	res.EOD, res.Balanced = eod()

	// Every authorisation must be exactly what its client was last told.
	for id, want := range expected {
		resp, err := http.Get(cl.base + "/v1/authorizations/" + id)
		if err != nil {
			res.StateMismatches++
			continue
		}
		var got auth
		json.NewDecoder(resp.Body).Decode(&got)
		resp.Body.Close()
		if got.Captured != want.Captured || got.Reversed != want.Reversed || got.Refunded != want.Refunded || got.Amount != want.Amount {
			res.StateMismatches++
			if len(res.FirstMismatchNotes) < 5 {
				res.FirstMismatchNotes = append(res.FirstMismatchNotes, fmt.Sprintf("%s: client saw %+v, db has %+v", id, *want, got))
			}
		}
	}

	// Replay old requests: same answers, and the ledger must not move.
	cl.logMu.Lock()
	log := append([]sent(nil), cl.log...)
	cl.logMu.Unlock()
	rng.Shuffle(len(log), func(i, j int) { log[i], log[j] = log[j], log[i] })
	for _, s := range log[:min(c.replay, len(log))] {
		st, b := cl.send(s.method, s.path, s.key, s.body)
		res.Replays++
		if st != s.status || !bytes.Equal(b, s.resp) {
			res.ReplayMismatch++
		}
	}
	after, balancedAfter := eod()
	res.EODAfterReplay = after
	res.LedgerUnchanged = sameLedger(res.EOD, after)
	res.Balanced = res.Balanced && balancedAfter

	res.Requests = int64(len(log))
	res.Attempts, res.Retries, res.Duplicates, res.DuplicateMismatch = cl.attempts.Load(), cl.retries.Load(), cl.duplicates.Load(), cl.dupMM.Load()
	res.ReplayHeaders = cl.replayHeaders.Load()
	res.Authorizations, res.Approved, res.Captures, res.Reversals, res.Refunds = nAuth.Load(), nApproved.Load(), nCap.Load(), nRev.Load(), nRef.Load()
	res.OK = res.Balanced && res.LedgerUnchanged && res.StateMismatches == 0 && res.DuplicateMismatch == 0 && res.ReplayMismatch == 0 && res.Kills > 0
	svc.kill()
	return res
}

// sameLedger compares two end-of-day reports, ignoring their timestamps.
func sameLedger(a, b json.RawMessage) bool {
	var x, y map[string]any
	json.Unmarshal(a, &x)
	json.Unmarshal(b, &y)
	delete(x, "at")
	delete(y, "at")
	xb, _ := json.Marshal(x)
	yb, _ := json.Marshal(y)
	return x != nil && bytes.Equal(xb, yb)
}
