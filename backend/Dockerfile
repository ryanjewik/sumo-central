# Multi-stage Dockerfile for Go Gin backend
# Builder stage
# Use Go 1.25.3 to match the `go.mod` requirement (go >= 1.25.3)
FROM golang:1.25.3-alpine AS builder

# Install git for go module downloads and ca-certificates
RUN apk add --no-cache git ca-certificates

WORKDIR /src

# Copy go mod files first to leverage Docker cache
COPY go.mod go.sum ./
RUN go mod download

# Copy rest of the source
COPY . .

# Build static binary
ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64

# Build output to /app/sumo-backend
RUN go build -ldflags="-s -w" -o /app/sumo-backend ./main.go

# Final stage - small runtime image
FROM alpine:3.18
RUN apk add --no-cache ca-certificates tzdata netcat-openbsd
WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/sumo-backend /app/sumo-backend
# Copy tiny entrypoint that waits for Postgres to accept TCP connections
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

# Create non-root user and set ownership
RUN addgroup -S app && adduser -S -G app app

# Create runtime directories and make sure the non-root user owns them so
# the app can write decoded webhook payloads into /app/tmp/webhooks.
RUN mkdir -p /app/tmp/webhooks \
	&& chown -R app:app /app /app/tmp

# Switch to non-root user
USER app

# Default port - update if your app uses another
EXPOSE 8080

# Recommended environment defaults
ENV GIN_MODE=release

# Entrypoint waits for DB then execs the backend
ENTRYPOINT ["/app/docker-entrypoint.sh"]
# Default command (kept for compatibility; entrypoint will exec the binary)
CMD ["/app/sumo-backend"]
