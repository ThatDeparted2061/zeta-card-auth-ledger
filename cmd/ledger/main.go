// Command ledger runs the card authorisation API.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ThatDeparted2061/zeta-card-auth-ledger/internal/api"
	"github.com/ThatDeparted2061/zeta-card-auth-ledger/internal/ledger"
)

func main() {
	addr := flag.String("addr", ":8060", "listen address")
	dsn := flag.String("db", envOr("DATABASE_URL", "postgres://localhost:5432/card_ledger?sslmode=disable"), "Postgres URL")
	conns := flag.Int("max-conns", 32, "Postgres pool size")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	l, err := ledger.Open(ctx, *dsn, int32(*conns))
	if err != nil {
		slog.Error("open ledger", "err", err)
		os.Exit(1)
	}
	defer l.Close()
	srv := &http.Server{Addr: *addr, Handler: (&api.Server{L: l}).Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			slog.Error("listen", "err", err)
			os.Exit(1)
		}
	}()
	slog.Info("card ledger listening", "addr", *addr)
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(shutdown)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
