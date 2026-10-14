FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/ledger ./cmd/chaos

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /
EXPOSE 8060
ENTRYPOINT ["/ledger"]
