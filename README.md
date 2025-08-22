# PostgreSQL Backup & Restore Tool

A robust Go application for PostgreSQL backups and restores with strict error handling. Any failure immediately terminates the process with appropriate error reporting.

## Features

- **SSH-based remote backup execution** - Connects to production server and runs pg_dump
- **Database restore capability** - Restore backups from S3 to any PostgreSQL instance
- **Rsync file transfer** - Fast, efficient transfer with resume capability
- **S3-compatible storage** - Upload/download backups to/from Garage or any S3-compatible storage
- **Automatic retention management** - Keep only the N most recent backups
- **Email notifications** - Success/failure notifications via go-notification for both backup and restore
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
  retention_count: 7  # Keep 7 most recent backups

restore:
  enabled: true
  use_ssh: true                              # Set to false for local restore
  # Optional: SSH connection for different server (defaults to main SSH if not specified)
  ssh:
    host: "staging-server.example.com"
    port: 22
    username: "restore-user"
    key_path: "/home/user/.ssh/id_rsa"
  # Optional: specify different target PostgreSQL server
  target_host: "localhost"                    # PostgreSQL host (from SSH server's perspective)
  target_port: 5432                          # Defaults to postgres.port
  target_database: "restored_db"              # Defaults to postgres.database
  target_username: "restore_user"             # Defaults to postgres.username
  target_password: "restore_password"         # Defaults to postgres.password
  drop_existing: true
  create_db: true
  jobs: 4  # Parallel restore jobs

notification:
  enabled: true
  api_key: "your-api-key"
  from: "notifications@example.com"
  to: "admin@example.com"
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

### List available backups
```bash
./pg_backup -config config.yaml -list-backups
```

### Restore latest backup
```bash
./pg_backup -config config.yaml -restore
```

### Restore specific backup
```bash
./pg_backup -config config.yaml -restore -backup-key "backup-20240101-120000-backup_20240101_120000.dump"
```

### Restore to Different PostgreSQL Server

You can restore backups to a different PostgreSQL instance by specifying target connection settings in the configuration:

```yaml
restore:
  enabled: true
  target_host: "staging.example.com"     # Different server
  target_port: 5432
  target_database: "staging_db"          # Different database name
  target_username: "staging_user"
  target_password: "staging_password"
```

This is useful for:
- Restoring production backups to staging/development environments
- Migrating databases between servers
- Creating test databases from production backups
- Disaster recovery to standby servers

## Exit Codes

- `0` - Success
- `1` - Configuration error
- `2` - SSH connection failed
- `3` - Backup creation failed
- `4` - Transfer failed
- `5` - S3 upload failed
- `6` - Cleanup failed (critical cleanup only)

## Backup Workflow

1. **SSH Connection** - Establishes secure connection to production server
2. **Remote Backup** - Executes pg_dump with custom format and compression
3. **File Transfer** - Downloads backup via rsync with compression and resume support
4. **S3 Upload** - Uploads to S3-compatible storage with multipart support
5. **Cleanup** - Removes temporary files and keeps only N most recent backups

## Restore Workflow

1. **Backup Selection** - Lists or selects backup from S3 storage
2. **S3 Download** - Downloads backup file from S3 to local system
3. **SSH Connection** - Establishes connection to target server
4. **File Transfer** - Uploads backup to target server via rsync
5. **Database Restore** - Executes pg_restore with configurable options
6. **Cleanup** - Removes temporary files from both local and remote systems

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
- [go-notification](https://github.com/hra42/go-notification) binary (optional, for email notifications)

## Security Notes

- Store configuration files with restricted permissions (600)
- Use SSH key authentication when possible
- Consider using environment variables for sensitive values
- Never commit configuration files with credentials to version control