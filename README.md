# PostgreSQL Backup & Restore Tool

A robust Go application for PostgreSQL backups and restores with strict error handling. Any failure immediately terminates the process with appropriate error reporting.

## Features

- **SSH-based remote backup execution** - Connects to production server and runs pg_dump
- **Database restore capability** - Restore backups from S3 to any PostgreSQL instance
- **Built-in scheduler** - Schedule backups using gocron (no cron dependency)
- **Rsync file transfer** - Fast, efficient transfer with resume capability
- **S3-compatible storage** - Upload/download backups to/from Garage or any S3-compatible storage
- **Automatic retention management** - Keep only the N most recent backups
- **Email notifications** - Success/failure notifications via go-notification for both backup and restore
- **Progress tracking** - Real-time progress for all long-running operations
- **Structured logging** - Clear, parseable logs with context
- **Graceful shutdown** - Handles SIGINT/SIGTERM with cleanup
- **Dry-run mode** - Test configuration without performing actual backup

## Installation

### Binary Build
```bash
go build -o pg_backup
```

### Docker
```bash
# Build the image
docker build -t pg-backup:latest .

# Or use docker-compose
docker-compose build
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
  # Optional inline schedule for backups
  schedule:
    enabled: false
    type: "daily"
    expression: "02:00"

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

### Run cleanup only
```bash
./pg_backup -config config.yaml -cleanup
```

This will remove old backups from S3 based on your retention policy without performing a new backup.

### Restore latest backup
```bash
./pg_backup -config config.yaml -restore
```

### Restore specific backup
```bash
./pg_backup -config config.yaml -restore -backup-key "backup-20240101-120000-backup_20240101_120000.dump"
```

### Local Restore (Without SSH)

For restoring to a PostgreSQL instance on the same machine where pg_backup runs, you can disable SSH:

```yaml
restore:
  enabled: true
  use_ssh: false                        # Disable SSH for local restore
  auto_install: true                    # Auto-install pg_restore if missing
  target_host: "localhost"               # Local PostgreSQL instance
  target_port: 5432
  target_database: "restored_db"
  target_username: "postgres"
  target_password: "password"
  force_disconnect: true                # Terminate active connections before dropping
```

This executes pg_restore directly on the local machine without any SSH connection. If `auto_install` is enabled and pg_restore is not found, the tool will attempt to install PostgreSQL client tools automatically using the system's package manager (apt, yum, dnf, apk, or brew).

### Restore to Different PostgreSQL Server

You can restore backups to a completely different server by specifying both SSH and PostgreSQL connection settings:

```yaml
restore:
  enabled: true
  # SSH connection to the restore target server
  ssh:
    host: "staging.example.com"          # Different SSH server
    port: 22
    username: "staging-user"
    key_path: "/home/user/.ssh/staging_key"
  
  # PostgreSQL connection on the target server
  target_host: "localhost"               # PostgreSQL host from target server's perspective
  target_port: 5432
  target_database: "staging_db"          # Different database name
  target_username: "staging_user"
  target_password: "staging_password"
```

**Important Notes:**
- If `ssh` is not specified in restore config, it defaults to the main SSH settings (same server as backup source)
- The `target_host` is the PostgreSQL host as seen from the restore SSH server (often "localhost")
- This setup allows complete separation between backup source and restore target

**Restore Modes:**
1. **Local restore** (`use_ssh: false`) - Restore to local PostgreSQL without SSH
2. **Same server restore** (omit `ssh` config) - Use backup server's SSH settings
3. **Different server restore** (provide `ssh` config) - Connect to a different server

This is useful for:
- Local development and testing environments
- Restoring production backups to staging/development environments on different servers
- Cross-server database migrations
- Creating test databases from production backups on isolated servers
- Disaster recovery to standby servers in different data centers

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

## Scheduling

pg_backup includes a built-in scheduler using gocron, allowing you to schedule backup, restore, and cleanup tasks independently. This eliminates the need for system cron and provides more flexibility in task management.

### Key Features

- **Independent Task Scheduling**: Each operation (backup, restore, cleanup) can have its own schedule
- **Multiple Schedule Types**: Supports cron expressions, intervals, daily, weekly, and monthly schedules  
- **Dynamic Resource Management**: Only initializes necessary components (S3 client, SSH connections) when their schedules are enabled
- **Singleton Execution**: Prevents overlapping runs of the same task
- **Graceful Shutdown**: Properly handles SIGINT/SIGTERM signals

### Schedule Configuration

Each task can have its own schedule configuration in the config file:

```yaml
backup:
  schedule:
    enabled: true
    type: "daily"        # Options: cron, interval, daily, weekly, monthly
    expression: "02:00"  # Expression format depends on type
    run_on_start: false  # Run immediately when scheduler starts

restore:
  schedule:
    enabled: false
    type: "weekly"
    expression: "Sunday 03:00"  # Weekly restore test
    # Can optionally specify a specific backup_key to restore

cleanup:
  schedule:
    enabled: true
    type: "daily"
    expression: "04:00"  # Daily cleanup at 4 AM
```

### Schedule Types

#### Cron Expression
Standard cron format for complex schedules:
```yaml
type: "cron"
expression: "0 2 * * *"  # Daily at 2 AM
```

#### Interval
Run at fixed intervals:
```yaml
type: "interval"
expression: "6h"  # Every 6 hours (supports: s, m, h)
```

#### Daily
Run daily at a specific time:
```yaml
type: "daily"
expression: "03:30"  # Daily at 3:30 AM (HH:MM format)
```

#### Weekly
Run weekly on a specific day and time:
```yaml
type: "weekly"
expression: "Monday 02:00"  # Every Monday at 2 AM
```

#### Monthly
Run monthly on a specific day and time:
```yaml
type: "monthly"
expression: "15 02:00"  # 15th of each month at 2 AM
```

### Running the Scheduler

```bash
# Run in scheduled mode (starts the scheduler daemon)
./pg_backup -schedule -config config.yaml

# The scheduler will automatically start if any task has scheduling enabled
./pg_backup -config config.yaml

# View scheduled jobs and next run times in logs
# The scheduler logs when each job is scheduled and when it runs
```

### Use Cases

1. **Daily backups with weekly cleanup**:
   - Schedule backups daily at 2 AM
   - Schedule cleanup weekly on Sunday at 4 AM
   - Maintains optimal storage usage while ensuring regular backups

2. **Disaster recovery testing**:
   - Schedule weekly restore tests to verify backup integrity
   - Use a test database target for automated validation
   - Optionally restore specific backup versions using `backup_key` in restore config

3. **High-frequency backups with smart retention**:
   - Run backups every 6 hours for critical data
   - Run cleanup once daily to manage retention
   - Keeps storage costs controlled while maintaining recovery points

4. **Multi-environment synchronization**:
   - Schedule production backups at night
   - Schedule staging environment restores in the morning
   - Keeps development environments updated with production data

### As a Service (systemd)

Create `/etc/systemd/system/pg-backup-scheduler.service`:

```ini
[Unit]
Description=PostgreSQL Backup Scheduler
After=network.target

[Service]
Type=simple
User=backup
ExecStart=/usr/local/bin/pg_backup -config /etc/pg_backup/config.yaml
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl enable pg-backup-scheduler
sudo systemctl start pg-backup-scheduler
```

### Scheduler Implementation Details

- **Multiple task scheduling**: Schedule backup, restore, and cleanup independently
- **No cron dependency**: Built-in scheduler runs as a long-running process using gocron
- **Singleton mode**: Prevents overlapping executions of the same task
- **Graceful shutdown**: Handles SIGINT/SIGTERM signals properly  
- **Smart initialization**: Only creates S3 clients and SSH connections when needed
- **Detailed logging**: Logs next scheduled run time after each task execution
- **Run on start**: Optional immediate execution when scheduler starts
- **Flexible scheduling**: Each task can use different schedule types (cron, interval, daily, weekly, monthly)

### Legacy Cron Example

If you prefer using system cron instead of the built-in scheduler:

```bash
0 2 * * * /usr/local/bin/pg_backup -config /etc/pg_backup/config.yaml -json-logs >> /var/log/pg_backup.log 2>&1
```

## Docker Deployment

### Quick Start with Docker Compose

1. Create your `config.yaml` file based on `config.example.yaml`
2. Update `docker-compose.yml` with your paths and settings
3. Run the scheduler:

```bash
# Start the scheduler in the background
docker-compose up -d

# View logs
docker-compose logs -f

# Stop the scheduler
docker-compose down
```

### Docker Run Examples

#### Run in scheduler mode:
```bash
docker run -d \
  --name pg-backup-scheduler \
  --restart unless-stopped \
  -v $(pwd)/config.yaml:/config/config.yaml:ro \
  -v ~/.ssh:/home/pgbackup/.ssh:ro \
  -v $(pwd)/logs:/logs \
  -e TZ=America/New_York \
  pg-backup:latest \
  -schedule -config /config/config.yaml
```

#### Run single backup:
```bash
docker run --rm \
  -v $(pwd)/config.yaml:/config/config.yaml:ro \
  -v ~/.ssh:/home/pgbackup/.ssh:ro \
  pg-backup:latest \
  -config /config/config.yaml
```

#### Run restore:
```bash
docker run --rm -it \
  -v $(pwd)/config.yaml:/config/config.yaml:ro \
  -v ~/.ssh:/home/pgbackup/.ssh:ro \
  pg-backup:latest \
  -restore -config /config/config.yaml
```

### Docker Configuration Notes

1. **SSH Keys**: Mount your SSH keys as read-only volumes
2. **Timezone**: Set the `TZ` environment variable for correct scheduling
3. **Config File**: Mount your config.yaml as a read-only volume
4. **Logs**: Optionally mount a logs directory for persistent logging
5. **Network**: Use `network_mode: host` if connecting to local PostgreSQL

### Building Multi-Architecture Images

```bash
# Build for multiple platforms
docker buildx build --platform linux/amd64,linux/arm64 \
  -t pg-backup:latest --push .
```

### Security Considerations

- The Docker image runs as a non-root user (`pgbackup`)
- Mount configuration and SSH keys as read-only (`:ro`)
- Use secrets management for sensitive environment variables
- Consider using Docker secrets or config for production deployments

## Requirements

- Go 1.25+
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