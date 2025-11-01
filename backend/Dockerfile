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
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/sumo-backend /app/sumo-backend

# Create non-root user and set ownership
RUN addgroup -S app && adduser -S -G app app && chown app:app /app/sumo-backend
USER app

# Default port - update if your app uses another
EXPOSE 8080

# Recommended environment defaults
ENV GIN_MODE=release

# Run the binary
CMD ["/app/sumo-backend"]
