.PHONY: demo demo-quick test build up down

# k6 latency at 1000 TPS, then a chaos run; writes bench/RESULTS.md.
demo:
	bench/run.sh

demo-quick:
	TPS=300 DURATION=30s CHAOS_FOR=30s bench/run.sh

test:
	go vet ./... && go test -race ./...

build:
	go build -o bin/ ./cmd/...

up:
	docker compose up --build --abort-on-container-exit --exit-code-from k6

down:
	docker compose down -v
