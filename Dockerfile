# Build stage
FROM golang:1.25.0-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application - fix build path to cmd/nebula
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s -X main.version=$(git describe --tags --always || echo 'dev')" \
    -o bin/nebula \
    ./cmd/nebula

# Runtime stage
FROM alpine:3.19

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1000 nebula && \
    adduser -D -u 1000 -G nebula nebula

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/bin/nebula /app/nebula

# Copy config directory (if it exists)
COPY --from=builder /app/config/ /app/config/

# Change ownership
RUN chown -R nebula:nebula /app

# Switch to non-root user
USER nebula

# Expose metrics port
EXPOSE 9090

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/app/nebula", "version"] || exit 1

# Set entrypoint
ENTRYPOINT ["/app/nebula"]

# Default command
CMD ["help"]