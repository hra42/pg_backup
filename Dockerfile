# Build stage
FROM golang:1.25-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o pg_backup .

# Final stage
FROM alpine:latest

# Install runtime dependencies
RUN apk --no-cache add \
    ca-certificates \
    openssh-client \
    rsync \
    postgresql-client \
    tzdata

# Create non-root user
RUN addgroup -g 1000 -S pgbackup && \
    adduser -u 1000 -S pgbackup -G pgbackup

# Create necessary directories
RUN mkdir -p /home/pgbackup/.ssh /config /backup /logs && \
    chown -R pgbackup:pgbackup /home/pgbackup /config /backup /logs

# Copy binary from builder
COPY --from=builder /app/pg_backup /usr/local/bin/pg_backup
RUN chmod +x /usr/local/bin/pg_backup

# Switch to non-root user
USER pgbackup

# Set working directory
WORKDIR /home/pgbackup

# Volume for configuration and SSH keys
VOLUME ["/config", "/home/pgbackup/.ssh", "/backup", "/logs"]

# Default environment variables
ENV CONFIG_PATH=/config/config.yaml \
    LOG_LEVEL=info \
    JSON_LOGS=false

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD pgrep pg_backup || exit 1

# Default command runs in scheduler mode
ENTRYPOINT ["/usr/local/bin/pg_backup"]
CMD ["-schedule", "-config", "/config/config.yaml"]
