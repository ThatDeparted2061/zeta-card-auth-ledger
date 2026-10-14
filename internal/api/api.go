// Package api serves the authorisation API over HTTP/JSON.
//
//	POST /v1/cards                              open a card
//	POST /v1/authorizations                     hold
//	POST /v1/authorizations/{id}/captures       full or partial capture
//	POST /v1/authorizations/{id}/reversals      release the remaining hold (or part of it)
//	POST /v1/authorizations/{id}/refunds        refund captured money
//	GET  /v1/authorizations/{id}
//	GET  /v1/cards/{id}/balance
//	GET  /v1/eod                                end-of-day ledger proof
//
// Every POST needs an Idempotency-Key header. Repeating a request with the
// same key returns the first response (Idempotent-Replay: true) and changes
// nothing; reusing a key for a different request is a 422.
package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ThatDeparted2061/zeta-card-auth-ledger/internal/ledger"
)

var (
	latency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "api_request_duration_seconds",
		Help:    "Request latency by route and status.",
		Buckets: []float64{.001, .002, .003, .005, .0075, .01, .015, .02, .03, .05, .075, .1, .25, .5, 1},
	}, []string{"route", "status"})
	decisions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "authorizations_total", Help: "Authorisation decisions by result and decline reason.",
	}, []string{"result", "reason"})
	replays = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "idempotent_replays_total", Help: "Requests answered from a stored response, by route.",
	}, []string{"route"})
)

// Server is the HTTP API.
type Server struct{ L *ledger.Ledger }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/cards", s.write("create_card", func(ctx context.Context, tx pgx.Tx, key string, r *http.Request, body []byte) (ledger.Response, error) {
		var c ledger.Card
		if err := json.Unmarshal(body, &c); err != nil {
			return ledger.Response{}, badJSON(err)
		}
		return s.L.CreateCard(ctx, tx, key, c)
	}))
	mux.HandleFunc("POST /v1/authorizations", s.write("authorize", func(ctx context.Context, tx pgx.Tx, key string, r *http.Request, body []byte) (ledger.Response, error) {
		var req struct {
			CardID   string `json:"card_id"`
			Merchant string `json:"merchant"`
			Amount   int64  `json:"amount"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			return ledger.Response{}, badJSON(err)
		}
		resp, err := s.L.Hold(ctx, tx, key, req.CardID, req.Merchant, req.Amount)
		if a, ok := resp.Body.(ledger.Authorization); ok && err == nil {
			decisions.WithLabelValues(a.Status, a.Decline).Inc()
		}
		return resp, err
	}))
	for _, op := range []struct {
		path, route string
		fn          func(context.Context, pgx.Tx, string, uuid.UUID, int64) (ledger.Response, error)
	}{
		{"captures", "capture", s.L.Capture},
		{"reversals", "reverse", s.L.Reverse},
		{"refunds", "refund", s.L.Refund},
	} {
		mux.HandleFunc("POST /v1/authorizations/{id}/"+op.path, s.write(op.route, func(ctx context.Context, tx pgx.Tx, key string, r *http.Request, body []byte) (ledger.Response, error) {
			id, err := uuid.Parse(r.PathValue("id"))
			if err != nil {
				return ledger.Response{}, &ledger.Problem{Status: 400, Code: "invalid_id", Message: "authorization id must be a UUID"}
			}
			var req struct {
				Amount int64 `json:"amount"`
			}
			if len(body) > 0 {
				if err := json.Unmarshal(body, &req); err != nil {
					return ledger.Response{}, badJSON(err)
				}
			}
			return op.fn(ctx, tx, key, id, req.Amount)
		}))
	}
	mux.HandleFunc("GET /v1/authorizations/{id}", s.read("get_authorization", func(r *http.Request) (any, error) {
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			return nil, &ledger.Problem{Status: 400, Code: "invalid_id", Message: "authorization id must be a UUID"}
		}
		return s.L.Get(r.Context(), id)
	}))
	mux.HandleFunc("GET /v1/cards/{id}/balance", s.read("balance", func(r *http.Request) (any, error) { return s.L.Balance(r.Context(), r.PathValue("id")) }))
	mux.HandleFunc("GET /v1/eod", s.read("eod", func(r *http.Request) (any, error) { return s.L.EndOfDay(r.Context()) }))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.Handle("GET /metrics", promhttp.Handler())
	return mux
}

func badJSON(err error) error {
	return &ledger.Problem{Status: 400, Code: "invalid_json", Message: err.Error()}
}

type writeFn func(ctx context.Context, tx pgx.Tx, key string, r *http.Request, body []byte) (ledger.Response, error)

// write wraps a ledger operation in the idempotency protocol.
func (s *Server) write(route string, fn writeFn) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		status := 500
		defer func() { latency.WithLabelValues(route, strconv.Itoa(status)).Observe(time.Since(start).Seconds()) }()

		key := r.Header.Get("Idempotency-Key")
		if key == "" || len(key) > 255 {
			status = 400
			writeError(w, status, "idempotency_key_required", "set an Idempotency-Key header (1-255 chars) on every write")
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<16))
		if err != nil {
			status = 400
			writeError(w, status, "bad_body", err.Error())
			return
		}
		h := sha256.New()
		h.Write([]byte(r.Method + " " + r.URL.Path + "\n"))
		h.Write(body)
		code, out, replayed, err := s.L.Idempotent(r.Context(), key, h.Sum(nil), func(tx pgx.Tx) (ledger.Response, error) {
			return fn(r.Context(), tx, key, r, body)
		})
		switch {
		case errors.Is(err, ledger.ErrKeyReused):
			status = 422
			writeError(w, status, "idempotency_key_reused", err.Error())
			return
		case err != nil:
			status = 503
			if r.Context().Err() == nil {
				slog.Error("write failed", "route", route, "err", err)
			}
			writeError(w, status, "unavailable", "the request was not applied; retry it with the same Idempotency-Key")
			return
		}
		if replayed {
			replays.WithLabelValues(route).Inc()
			w.Header().Set("Idempotent-Replay", "true")
		}
		status = code
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		w.Write(out)
	}
}

func (s *Server) read(route string, fn func(*http.Request) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		status := 200
		defer func() { latency.WithLabelValues(route, strconv.Itoa(status)).Observe(time.Since(start).Seconds()) }()
		v, err := fn(r)
		var p *ledger.Problem
		switch {
		case errors.As(err, &p):
			status = p.Status
			writeJSON(w, status, map[string]any{"error": p})
		case err != nil:
			status = 500
			writeError(w, status, "internal", err.Error())
		default:
			writeJSON(w, status, v)
		}
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, kind, msg string) {
	writeJSON(w, code, map[string]any{"error": map[string]string{"code": kind, "message": msg}})
}
