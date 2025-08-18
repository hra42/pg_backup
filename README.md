# PostgreSQL Backup Tool

A robust Go application for PostgreSQL backups with strict error handling. Any failure immediately terminates the process with appropriate error reporting.

## Features

- **SSH-based remote backup execution** - Connects to production server and runs pg_dump
- **Rsync file transfer** - Fast, efficient transfer with resume capability
- **S3-compatible storage** - Upload backups to Garage or any S3-compatible storage
- **Automatic retention management** - Clean up old backups based on retention policy
- **Progress tracking** - Real-time progress for all long-running operations
- **Structured logging** - Clear, parseable logs with context
- **Graceful shutdown** - Handles SIGINT/SIGTERM with cleanup
- **Dry-run mode** - Test configuration without performing actual backup

## Installation

```bash
go build -o pg_backup
```

## Configuration

Copy `config.example.yaml` to `config.yaml` and update with your settings:

```yaml
ssh:
  host: "prod-server.example.com"
  port: 22
  username: "backup-user"
  key_path: "/home/user/.ssh/id_rsa"

postgres:
  host: "localhost"
  port: 5432
  database: "production_db"
  username: "postgres"
  password: "your-password"

s3:
  endpoint: "https://s3.garage.example.com"
  access_key_id: "your-key"
  secret_access_key: "your-secret"
  bucket: "backups"
  prefix: "postgres"

backup:
  retention_days: 30
```

## Usage

### Basic backup
```bash
./pg_backup -config config.yaml
```

### Dry run (test configuration)
```bash
./pg_backup -config config.yaml -dry-run
```

### With debug logging
```bash
./pg_backup -config config.yaml -log-level debug
```

### JSON logs (for log aggregation)
```bash
./pg_backup -config config.yaml -json-logs
```

## Exit Codes

- `0` - Success
- `1` - Configuration error
- `2` - SSH connection failed
- `3` - Backup creation failed
- `4` - Transfer failed
- `5` - S3 upload failed
- `6` - Cleanup failed (critical cleanup only)

## Workflow

1. **SSH Connection** - Establishes secure connection to production server
2. **Remote Backup** - Executes pg_dump with plain SQL format and gzip compression
3. **File Transfer** - Downloads backup via rsync with compression and resume support
4. **S3 Upload** - Uploads to S3-compatible storage with multipart support
5. **Cleanup** - Removes temporary files and old backups per retention policy

## Cron Example

For daily backups at 2 AM:

```bash
0 2 * * * /usr/local/bin/pg_backup -config /etc/pg_backup/config.yaml -json-logs >> /var/log/pg_backup.log 2>&1
```

## Requirements

- Go 1.21+
- SSH access to production server
- pg_dump installed on production server
- rsync installed on local machine
- S3-compatible storage (Garage, MinIO, AWS S3, etc.)
- sshpass (optional, for password authentication with rsync)

## Security Notes

- Store configuration files with restricted permissions (600)
- Use SSH key authentication when possible
- Consider using environment variables for sensitive values
- Never commit configuration files with credentials to version control