package restore

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/hra42/pg_backup/internal/config"
	"github.com/hra42/pg_backup/internal/notification"
	"github.com/hra42/pg_backup/internal/rsync"
	"github.com/hra42/pg_backup/internal/ssh"
	"github.com/hra42/pg_backup/internal/storage"
)

type RestoreManager struct {
	config             *config.Config
	sshClient          *ssh.SSHClient
	s3Client           *storage.S3Client
	notificationClient *notification.NotificationClient
	logger             *slog.Logger
}

func NewRestoreManager(cfg *config.Config, logger *slog.Logger) (*RestoreManager, error) {
	var sshClient *ssh.SSHClient
	var err error
	
	// Check if SSH is needed for restore
	useSSH := true
	if cfg.Restore.UseSSH != nil {
		useSSH = *cfg.Restore.UseSSH
	}
	
	if useSSH {
		// Use restore SSH config if provided, otherwise use backup SSH config
		sshConfig := cfg.Restore.SSH
		if sshConfig == nil {
			sshConfig = &cfg.SSH
		}
		
		sshClient, err = ssh.NewSSHClient(sshConfig, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create SSH client for restore: %w", err)
		}
	} else {
		logger.Info("Local restore mode - SSH connection disabled")
		sshClient = nil
	}

	s3Client, err := storage.NewS3Client(&cfg.S3, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	notificationClient := notification.NewNotificationClient(&cfg.Notification, logger)

	return &RestoreManager{
		config:             cfg,
		sshClient:          sshClient,
		s3Client:           s3Client,
		notificationClient: notificationClient,
		logger:             logger,
	}, nil
}

func (rm *RestoreManager) Run(ctx context.Context, backupKey string) error {
	defer rm.cleanup()
	startTime := time.Now()

	if !rm.config.Restore.Enabled {
		return fmt.Errorf("restore feature is not enabled in configuration")
	}

	rm.logger.Info("Starting restore process", 
		slog.String("backup_key", backupKey),
		slog.String("target_database", rm.config.Restore.TargetDatabase))

	// If no specific backup key provided, get the latest
	if backupKey == "" {
		latest, err := rm.s3Client.GetLatestBackup(ctx)
		if err != nil {
			rm.notificationClient.SendRestoreFailure(rm.config.Restore.TargetDatabase, err, "backup_selection")
			return fmt.Errorf("failed to get latest backup: %w", err)
		}
		backupKey = latest
		rm.logger.Info("Using latest backup", slog.String("key", backupKey))
	}

	// Download backup from S3
	localBackupPath := filepath.Join(os.TempDir(), filepath.Base(backupKey))
	if err := rm.downloadFromS3(ctx, backupKey, localBackupPath); err != nil {
		rm.notificationClient.SendRestoreFailure(rm.config.Restore.TargetDatabase, err, "download")
		return err
	}
	defer os.Remove(localBackupPath)

	// Check if we're using SSH or local restore
	useSSH := rm.sshClient != nil
	var restoreFilePath string
	
	if useSSH {
		// Connect to SSH
		if err := rm.connectSSH(); err != nil {
			rm.notificationClient.SendRestoreFailure(rm.config.Restore.TargetDatabase, err, "ssh_connection")
			return err
		}

		// Transfer backup to remote server
		remoteBackupPath := filepath.Join(rm.config.Backup.TempDir, filepath.Base(backupKey))
		if err := rm.transferToRemote(localBackupPath, remoteBackupPath); err != nil {
			rm.notificationClient.SendRestoreFailure(rm.config.Restore.TargetDatabase, err, "transfer")
			return err
		}
		defer rm.sshClient.RemoveRemoteFile(remoteBackupPath)
		restoreFilePath = remoteBackupPath
	} else {
		// Local restore - use the downloaded file directly
		rm.logger.Info("Using local file for restore", slog.String("path", localBackupPath))
		restoreFilePath = localBackupPath
	}

	// Perform restore
	if err := rm.performRestore(restoreFilePath); err != nil {
		rm.notificationClient.SendRestoreFailure(rm.config.Restore.TargetDatabase, err, "restore")
		return err
	}

	duration := time.Since(startTime)
	rm.logger.Info("Restore completed successfully", 
		slog.String("database", rm.config.Restore.TargetDatabase),
		slog.Duration("duration", duration))

	// Send success notification
	if rm.notificationClient != nil {
		if err := rm.notificationClient.SendRestoreSuccess(rm.config.Restore.TargetDatabase, duration, backupKey); err != nil {
			rm.logger.Warn("Failed to send success notification", slog.String("error", err.Error()))
		}
	}

	return nil
}

func (rm *RestoreManager) ListAvailableBackups(ctx context.Context) ([]string, error) {
	rm.logger.Info("Listing available backups")
	
	backups, err := rm.s3Client.ListBackups(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list backups: %w", err)
	}

	rm.logger.Info("Found backups", slog.Int("count", len(backups)))
	return backups, nil
}

func (rm *RestoreManager) connectSSH() error {
	if rm.sshClient == nil {
		return fmt.Errorf("SSH client not initialized for local restore")
	}
	
	// Log which server we're connecting to
	sshConfig := rm.config.Restore.SSH
	if sshConfig == nil {
		sshConfig = &rm.config.SSH
	}
	rm.logger.Info("Establishing SSH connection for restore",
		slog.String("host", sshConfig.Host),
		slog.Int("port", sshConfig.Port),
		slog.String("user", sshConfig.Username))
	if err := rm.sshClient.Connect(rm.config.Timeouts.SSHConnection); err != nil {
		return fmt.Errorf("SSH connection failed: %w", err)
	}
	return nil
}

func (rm *RestoreManager) downloadFromS3(ctx context.Context, key string, localPath string) error {
	rm.logger.Info("Downloading backup from S3", 
		slog.String("key", key),
		slog.String("local_path", localPath))

	lastProgress := time.Now()
	err := rm.s3Client.DownloadFile(ctx, key, localPath, func(downloaded int64) {
		if time.Since(lastProgress) > 5*time.Second {
			rm.logger.Info("Download progress", slog.Int64("downloaded", downloaded))
			lastProgress = time.Now()
		}
	})

	if err != nil {
		return fmt.Errorf("S3 download failed: %w", err)
	}

	// Verify file exists and has content
	info, err := os.Stat(localPath)
	if err != nil {
		return fmt.Errorf("failed to verify downloaded file: %w", err)
	}
	if info.Size() == 0 {
		return fmt.Errorf("downloaded file is empty")
	}

	rm.logger.Info("Backup downloaded successfully", slog.Int64("size", info.Size()))
	return nil
}

func (rm *RestoreManager) transferToRemote(localPath, remotePath string) error {
	rm.logger.Info("Transferring backup to remote server",
		slog.String("local", localPath),
		slog.String("remote", remotePath))

	// Use restore SSH config if provided, otherwise use backup SSH config
	sshConfig := rm.config.Restore.SSH
	if sshConfig == nil {
		sshConfig = &rm.config.SSH
	}
	rsyncClient := rsync.NewRsyncClient(sshConfig, rm.logger)
	
	lastProgress := time.Now()
	err := rsyncClient.UploadFile(localPath, remotePath, rm.config.Timeouts.Transfer, 
		func(transferred, total int64) {
			if time.Since(lastProgress) > 5*time.Second {
				percentage := float64(transferred) / float64(total) * 100
				rm.logger.Info("Transfer progress",
					slog.Float64("percentage", percentage),
					slog.Int64("transferred", transferred),
					slog.Int64("total", total))
				lastProgress = time.Now()
			}
		})

	if err != nil {
		return fmt.Errorf("transfer failed: %w", err)
	}

	// Verify remote file
	statOutput, err := rm.sshClient.ExecuteCommand(
		fmt.Sprintf("stat -c %%s %s 2>/dev/null || stat -f %%z %s 2>/dev/null", remotePath, remotePath), 
		10*time.Second)
	if err != nil {
		return fmt.Errorf("failed to verify remote file: %w", err)
	}

	fileSize := strings.TrimSpace(statOutput)
	if fileSize == "" || fileSize == "0" {
		return fmt.Errorf("remote file is empty")
	}

	rm.logger.Info("Backup transferred successfully", slog.String("size", fileSize))
	return nil
}

func (rm *RestoreManager) executeCommand(command string, timeout time.Duration) (string, error) {
	if rm.sshClient != nil {
		// Execute via SSH
		return rm.sshClient.ExecuteCommand(command, timeout)
	}
	
	// Execute locally
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	
	cmd := exec.CommandContext(ctx, "bash", "-c", command)
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func (rm *RestoreManager) tryInstallPostgreSQLClient() error {
	rm.logger.Info("Attempting to auto-install PostgreSQL client tools...")
	
	// Detect the package manager and OS
	detectCmd := `
if command -v apt-get >/dev/null 2>&1; then
    echo "apt"
elif command -v yum >/dev/null 2>&1; then
    echo "yum"
elif command -v dnf >/dev/null 2>&1; then
    echo "dnf"
elif command -v apk >/dev/null 2>&1; then
    echo "apk"
elif command -v brew >/dev/null 2>&1; then
    echo "brew"
else
    echo "unknown"
fi`
	
	output, err := rm.executeCommand(detectCmd, 10*time.Second)
	if err != nil {
		return fmt.Errorf("failed to detect package manager: %w", err)
	}
	
	packageManager := strings.TrimSpace(output)
	rm.logger.Info("Detected package manager", slog.String("type", packageManager))
	
	var installCmd string
	switch packageManager {
	case "apt":
		// Check if running as root or with sudo
		installCmd = "apt-get update && apt-get install -y postgresql-client"
		if os.Geteuid() != 0 {
			// Not root, try with sudo
			if _, err := rm.executeCommand("command -v sudo", 5*time.Second); err == nil {
				installCmd = "sudo " + installCmd
			} else {
				return fmt.Errorf("not running as root and sudo not available")
			}
		}
	case "yum":
		installCmd = "yum install -y postgresql"
		if os.Geteuid() != 0 {
			if _, err := rm.executeCommand("command -v sudo", 5*time.Second); err == nil {
				installCmd = "sudo " + installCmd
			} else {
				return fmt.Errorf("not running as root and sudo not available")
			}
		}
	case "dnf":
		installCmd = "dnf install -y postgresql"
		if os.Geteuid() != 0 {
			if _, err := rm.executeCommand("command -v sudo", 5*time.Second); err == nil {
				installCmd = "sudo " + installCmd
			} else {
				return fmt.Errorf("not running as root and sudo not available")
			}
		}
	case "apk":
		installCmd = "apk add --no-cache postgresql-client"
		if os.Geteuid() != 0 {
			if _, err := rm.executeCommand("command -v sudo", 5*time.Second); err == nil {
				installCmd = "sudo " + installCmd
			} else {
				return fmt.Errorf("not running as root and sudo not available")
			}
		}
	case "brew":
		installCmd = "brew install postgresql"
	default:
		return fmt.Errorf("unsupported package manager or OS")
	}
	
	rm.logger.Info("Installing PostgreSQL client tools...", slog.String("command", installCmd))
	
	// Execute installation with extended timeout
	output, err = rm.executeCommand(installCmd, 5*time.Minute)
	if err != nil {
		return fmt.Errorf("installation failed: %w (output: %s)", err, output)
	}
	
	rm.logger.Info("PostgreSQL client tools installation completed")
	return nil
}

func (rm *RestoreManager) tryInstallSpecificPostgreSQLVersion(version string) error {
	rm.logger.Info("Attempting to install specific PostgreSQL version", slog.String("version", version))
	
	// Map version numbers to major versions (1.16 = PostgreSQL 16, 1.15 = PostgreSQL 15, etc.)
	majorVersion := ""
	switch version {
	case "1.16":
		majorVersion = "16"
	case "1.15":
		majorVersion = "15"
	case "1.14":
		majorVersion = "14"
	case "1.13":
		majorVersion = "13"
	default:
		// Try to extract major version from the format
		if strings.HasPrefix(version, "1.") {
			majorVersion = strings.TrimPrefix(version, "1.")
		}
	}
	
	if majorVersion == "" {
		return fmt.Errorf("unable to determine PostgreSQL major version from backup version %s", version)
	}
	
	rm.logger.Info("Detected PostgreSQL major version", slog.String("major_version", majorVersion))
	
	// Detect package manager
	detectCmd := `command -v apt-get || command -v yum || command -v dnf || command -v apk || echo "unknown"`
	output, err := rm.executeCommand(detectCmd, 10*time.Second)
	if err != nil {
		return fmt.Errorf("failed to detect package manager: %w", err)
	}
	
	packageManager := filepath.Base(strings.TrimSpace(output))
	rm.logger.Info("Using package manager", slog.String("type", packageManager))
	
	var installCmd string
	switch packageManager {
	case "apt-get":
		// For Debian/Ubuntu
		// Try to detect the codename, with multiple fallbacks
		codename := "bookworm" // Default to Debian 12
		
		// Try method 1: /etc/os-release
		if output, err := rm.executeCommand("grep VERSION_CODENAME /etc/os-release 2>/dev/null | cut -d= -f2", 5*time.Second); err == nil && output != "" {
			codename = strings.TrimSpace(strings.Trim(output, "\""))
		} else if output, err := rm.executeCommand("grep UBUNTU_CODENAME /etc/os-release 2>/dev/null | cut -d= -f2", 5*time.Second); err == nil && output != "" {
			codename = strings.TrimSpace(strings.Trim(output, "\""))
		} else if output, err := rm.executeCommand("head -1 /etc/debian_version 2>/dev/null", 5*time.Second); err == nil && output != "" {
			// Map Debian version numbers to codenames
			version := strings.TrimSpace(output)
			if strings.HasPrefix(version, "12") {
				codename = "bookworm"
			} else if strings.HasPrefix(version, "11") {
				codename = "bullseye"
			} else if strings.HasPrefix(version, "10") {
				codename = "buster"
			}
		}
		
		rm.logger.Info("Detected distribution codename", slog.String("codename", codename))
		
		// Simpler approach: try to install from official repos first, then add PostgreSQL repo if needed
		installCmd = fmt.Sprintf("apt-get update && apt-get install -y postgresql-client-%s", majorVersion)
		
		// Execute with elevated privileges if needed
		if os.Geteuid() != 0 {
			if _, err := rm.executeCommand("command -v sudo", 5*time.Second); err == nil {
				installCmd = "sudo " + installCmd
			} else {
				return fmt.Errorf("not running as root and sudo not available")
			}
		}
		
		// Try simple installation first
		rm.logger.Info("Attempting direct installation from system repositories")
		if output, err := rm.executeCommand(installCmd, 2*time.Minute); err != nil {
			rm.logger.Info("Direct installation failed, adding PostgreSQL APT repository", slog.String("error", err.Error()))
			
			// If that fails, add the PostgreSQL APT repository
			// First ensure lsb-release is installed and get the codename
			lsbInstallCmd := "apt-get update && apt-get install -y lsb-release"
			if os.Geteuid() != 0 {
				if _, err := rm.executeCommand("command -v sudo", 5*time.Second); err == nil {
					lsbInstallCmd = "sudo " + lsbInstallCmd
				}
			}
			rm.executeCommand(lsbInstallCmd, 1*time.Minute)
			
			// Get the actual codename
			codenameOutput, _ := rm.executeCommand("lsb_release -cs", 5*time.Second)
			actualCodename := strings.TrimSpace(codenameOutput)
			if actualCodename == "" {
				actualCodename = codename // fallback to detected codename
			}
			
			rm.logger.Info("Using distribution codename for PostgreSQL repo", slog.String("codename", actualCodename))
			
			repoSetupCmd := fmt.Sprintf(`
				apt-get install -y wget ca-certificates &&
				wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - &&
				echo "deb http://apt.postgresql.org/pub/repos/apt/ %s-pgdg main" > /etc/apt/sources.list.d/pgdg.list &&
				apt-get update &&
				apt-get install -y postgresql-client-%s
			`, actualCodename, majorVersion)
			
			if os.Geteuid() != 0 {
				if _, err := rm.executeCommand("command -v sudo", 5*time.Second); err == nil {
					installCmd = fmt.Sprintf("sudo sh -c '%s'", repoSetupCmd)
				} else {
					return fmt.Errorf("not running as root and sudo not available for repository setup")
				}
			} else {
				installCmd = repoSetupCmd
			}
			
			output, err = rm.executeCommand(installCmd, 5*time.Minute)
			if err != nil {
				return fmt.Errorf("failed to install PostgreSQL %s client: %w (output: %s)", majorVersion, err, output)
			}
		}
	case "yum", "dnf":
		// For RHEL/CentOS/Fedora
		installCmd = fmt.Sprintf("%s install -y postgresql%s", packageManager, majorVersion)
		if os.Geteuid() != 0 {
			if _, err := rm.executeCommand("command -v sudo", 5*time.Second); err == nil {
				installCmd = "sudo " + installCmd
			} else {
				return fmt.Errorf("not running as root and sudo not available")
			}
		}
	case "apk":
		// For Alpine Linux
		installCmd = fmt.Sprintf("apk add --no-cache postgresql%s-client", majorVersion)
		if os.Geteuid() != 0 {
			if _, err := rm.executeCommand("command -v sudo", 5*time.Second); err == nil {
				installCmd = "sudo " + installCmd
			} else {
				return fmt.Errorf("not running as root and sudo not available")
			}
		}
	default:
		return fmt.Errorf("unsupported package manager for automatic PostgreSQL %s installation", majorVersion)
	}
	
	rm.logger.Info("Installing PostgreSQL client version", 
		slog.String("version", majorVersion),
		slog.String("command", installCmd))
	
	output, err = rm.executeCommand(installCmd, 5*time.Minute)
	if err != nil {
		return fmt.Errorf("failed to install PostgreSQL %s client: %w (output: %s)", majorVersion, err, output)
	}
	
	// Verify installation
	versionCheck := fmt.Sprintf("pg_restore --version | grep -q 'pg_restore (PostgreSQL) %s'", majorVersion)
	if _, err := rm.executeCommand(versionCheck, 10*time.Second); err == nil {
		rm.logger.Info("Successfully installed PostgreSQL client", slog.String("version", majorVersion))
	}
	
	return nil
}

func (rm *RestoreManager) performRestore(backupPath string) error {
	rm.logger.Info("Performing database restore",
		slog.String("backup_file", backupPath),
		slog.String("target_database", rm.config.Restore.TargetDatabase),
		slog.Bool("local", rm.sshClient == nil))

	// Check PostgreSQL version first
	pgVersionCmd := "pg_restore --version 2>&1 | grep -o 'PostgreSQL) [0-9]*' | grep -o '[0-9]*'"
	versionOutput, err := rm.executeCommand(pgVersionCmd, 10*time.Second)
	if err == nil && versionOutput != "" {
		currentVersion := strings.TrimSpace(versionOutput)
		rm.logger.Info("PostgreSQL client version detected", slog.String("version", currentVersion))
	}
	
	// Check if pg_restore exists
	output, err := rm.executeCommand("which pg_restore || command -v pg_restore || type pg_restore 2>/dev/null", 10*time.Second)
	if err != nil || strings.TrimSpace(output) == "" {
		// Try common PostgreSQL installation paths
		commonPaths := []string{
			"/usr/bin/pg_restore",
			"/usr/local/bin/pg_restore",
			"/opt/homebrew/bin/pg_restore",
			"/usr/pgsql-*/bin/pg_restore",
			"/usr/lib/postgresql/*/bin/pg_restore",
		}
		
		found := false
		for _, path := range commonPaths {
			checkCmd := fmt.Sprintf("test -x %s && echo %s", path, path)
			if output, err := rm.executeCommand(checkCmd, 5*time.Second); err == nil && strings.TrimSpace(output) != "" {
				found = true
				rm.logger.Info("Found pg_restore at", slog.String("path", strings.TrimSpace(output)))
				break
			}
		}
		
		if !found {
			location := "remote server"
			if rm.sshClient == nil {
				location = "local system"
				rm.logger.Warn("pg_restore not found on local system")
				
				// Try to auto-install PostgreSQL client tools if enabled
				if rm.config.Restore.AutoInstall {
					if err := rm.tryInstallPostgreSQLClient(); err != nil {
						rm.logger.Error("Failed to auto-install PostgreSQL client tools",
							slog.String("error", err.Error()),
							slog.String("hint", "Please install manually with: apt-get install postgresql-client or yum install postgresql"))
						return fmt.Errorf("pg_restore not found on %s and auto-install failed: %w", location, err)
					}
					
					// Check again after installation
					output, err = rm.executeCommand("which pg_restore", 10*time.Second)
					if err != nil || strings.TrimSpace(output) == "" {
						return fmt.Errorf("pg_restore still not found after installation attempt")
					}
					rm.logger.Info("PostgreSQL client tools installed successfully", 
						slog.String("pg_restore", strings.TrimSpace(output)))
				} else {
					rm.logger.Error("pg_restore not found. Please install PostgreSQL client tools.",
						slog.String("hint", "Install with: apt-get install postgresql-client or yum install postgresql"),
						slog.String("note", "Or enable auto_install in restore config"))
					return fmt.Errorf("pg_restore not found on %s (auto-install disabled)", location)
				}
			} else {
				return fmt.Errorf("pg_restore not found on %s", location)
			}
		}
	} else {
		rm.logger.Info("Found pg_restore", slog.String("path", strings.TrimSpace(output)))
	}

	pgPassword := fmt.Sprintf("PGPASSWORD='%s'", rm.config.Restore.TargetPassword)

	// Drop existing database if configured
	if rm.config.Restore.DropExisting {
		rm.logger.Info("Dropping existing database", slog.String("database", rm.config.Restore.TargetDatabase))
		
		// Terminate existing connections if force_disconnect is enabled
		if rm.config.Restore.ForceDisconnect {
			rm.logger.Info("Force disconnect enabled - terminating existing connections to database")
			terminateCmd := fmt.Sprintf(
				"%s psql -h %s -p %d -U %s -d postgres -c \"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s' AND pid <> pg_backend_pid();\"",
				pgPassword,
				rm.config.Restore.TargetHost,
				rm.config.Restore.TargetPort,
				rm.config.Restore.TargetUsername,
				rm.config.Restore.TargetDatabase,
			)
			
			if output, err := rm.executeCommand(terminateCmd, 10*time.Second); err != nil {
				// Log but don't fail if we can't terminate connections (might not have permissions)
				rm.logger.Warn("Failed to terminate existing connections", 
					slog.String("error", err.Error()),
					slog.String("output", output))
			} else {
				rm.logger.Info("Terminated existing connections", slog.String("output", strings.TrimSpace(output)))
			}
			
			// Small delay to ensure connections are closed
			time.Sleep(1 * time.Second)
		}
		
		// Now drop the database
		// Quote database name to handle special characters
		dropCmd := fmt.Sprintf(
			"%s psql -h %s -p %d -U %s -d postgres -c \"DROP DATABASE IF EXISTS \\\"%s\\\";\"",
			pgPassword,
			rm.config.Restore.TargetHost,
			rm.config.Restore.TargetPort,
			rm.config.Restore.TargetUsername,
			rm.config.Restore.TargetDatabase,
		)
		
		if output, err := rm.executeCommand(dropCmd, 30*time.Second); err != nil {
			// Check if error is due to active connections
			if strings.Contains(output, "being accessed by other users") {
				// Try a more aggressive approach - force disconnect
				rm.logger.Warn("Database has active connections, attempting force disconnect")
				
				// For PostgreSQL 9.2+, we can use FORCE option (but it's not available in all versions)
				// Try alternative: revoke connect and terminate
				revokeCmd := fmt.Sprintf(
					"%s psql -h %s -p %d -U %s -d postgres -c \"REVOKE CONNECT ON DATABASE \\\"%s\\\" FROM PUBLIC; SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s';\"",
					pgPassword,
					rm.config.Restore.TargetHost,
					rm.config.Restore.TargetPort,
					rm.config.Restore.TargetUsername,
					rm.config.Restore.TargetDatabase,
					rm.config.Restore.TargetDatabase,
				)
				
				if _, err := rm.executeCommand(revokeCmd, 10*time.Second); err != nil {
					rm.logger.Warn("Failed to revoke connections", slog.String("error", err.Error()))
				}
				
				// Wait a bit and try dropping again
				time.Sleep(2 * time.Second)
				
				if output, err := rm.executeCommand(dropCmd, 30*time.Second); err != nil {
					return fmt.Errorf("failed to drop existing database after terminating connections: %w (output: %s)", err, output)
				}
			} else {
				return fmt.Errorf("failed to drop existing database: %w (output: %s)", err, output)
			}
		}
		
		rm.logger.Info("Database dropped successfully")
	}

	// Create database if configured
	if rm.config.Restore.CreateDB {
		rm.logger.Info("Creating target database", slog.String("database", rm.config.Restore.TargetDatabase))
		
		// Quote database name to handle special characters
		createCmd := fmt.Sprintf(
			"%s psql -h %s -p %d -U %s -d postgres -c \"CREATE DATABASE \\\"%s\\\"",
			pgPassword,
			rm.config.Restore.TargetHost,
			rm.config.Restore.TargetPort,
			rm.config.Restore.TargetUsername,
			rm.config.Restore.TargetDatabase,
		)
		
		if rm.config.Restore.Owner != "" {
			// Also quote owner name in case it has special characters
			createCmd += fmt.Sprintf(" OWNER \\\"%s\\\"", rm.config.Restore.Owner)
		}
		createCmd += ";\""
		
		if output, err := rm.executeCommand(createCmd, 30*time.Second); err != nil {
			// Check if database already exists
			if !strings.Contains(err.Error(), "already exists") && !strings.Contains(output, "already exists") {
				return fmt.Errorf("failed to create database: %w (output: %s)", err, output)
			}
			rm.logger.Info("Database already exists, continuing with restore")
		}
	}

	// Build pg_restore command
	// Quote database name to handle special characters
	restoreCmd := fmt.Sprintf(
		"%s pg_restore -h %s -p %d -U %s -d \"%s\" --verbose --no-owner --no-privileges --no-tablespaces",
		pgPassword,
		rm.config.Restore.TargetHost,
		rm.config.Restore.TargetPort,
		rm.config.Restore.TargetUsername,
		rm.config.Restore.TargetDatabase,
	)

	// Add parallel jobs if configured
	if rm.config.Restore.Jobs > 1 {
		restoreCmd += fmt.Sprintf(" --jobs=%d", rm.config.Restore.Jobs)
	}

	// Add clean option if not creating new database
	if !rm.config.Restore.CreateDB && rm.config.Restore.DropExisting {
		restoreCmd += " --clean --if-exists"
	}

	restoreCmd += fmt.Sprintf(" %s 2>&1", backupPath)

	// Execute restore (with extended timeout)
	rm.logger.Info("Executing pg_restore command", slog.Int("jobs", rm.config.Restore.Jobs))
	output, err = rm.executeCommand(restoreCmd, rm.config.Timeouts.BackupOp)
	
	if err != nil {
		// Check for version mismatch
		if strings.Contains(output, "unsupported version") {
			// Extract version info from error
			versionRegex := regexp.MustCompile(`unsupported version \(([0-9.]+)\)`)
			matches := versionRegex.FindStringSubmatch(output)
			backupVersion := "unknown"
			if len(matches) > 1 {
				backupVersion = matches[1]
			}
			
			// Check current PostgreSQL version
			currentVersionCmd := "pg_restore --version 2>&1 | grep -o 'PostgreSQL) [0-9]*' | grep -o '[0-9]*'"
			currentVersionOutput, _ := rm.executeCommand(currentVersionCmd, 5*time.Second)
			currentVersion := strings.TrimSpace(currentVersionOutput)
			
			rm.logger.Error("PostgreSQL version mismatch",
				slog.String("backup_version", backupVersion),
				slog.String("current_version", currentVersion),
				slog.String("error", "The backup was created with a newer PostgreSQL version"),
				slog.String("solution", "Please upgrade PostgreSQL client tools to match the backup version"))
			
			// Check if backup version is 1.16 (PostgreSQL 16/17) and we have version 16
			if backupVersion == "1.16" {
				rm.logger.Info("Backup has dump format version 1.16")
				rm.logger.Info("This format is used by PostgreSQL 17 or newer development versions")
				
				// Check if it's actually a PostgreSQL custom dump
				magicCmd := fmt.Sprintf("hexdump -C %s | head -n 1", backupPath)
				magicOutput, _ := rm.executeCommand(magicCmd, 5*time.Second)
				
				// PostgreSQL custom format should start with "PGDMP"
				if !strings.Contains(magicOutput, "50 47 44 4d 50") { // PGDMP in hex
					rm.logger.Error("File does not appear to be a valid PostgreSQL custom format dump")
					return fmt.Errorf("invalid backup file format - not a PostgreSQL custom dump")
				}
				
				// Try to install PostgreSQL 17 client tools
				if rm.sshClient == nil && rm.config.Restore.AutoInstall {
					rm.logger.Info("Attempting to install PostgreSQL 17 client tools to handle format version 1.16...")
					
					// Install PostgreSQL 17
					installCmd := "apt-get update && apt-get install -y postgresql-client-17"
					if os.Geteuid() != 0 {
						if _, err := rm.executeCommand("command -v sudo", 5*time.Second); err == nil {
							installCmd = "sudo " + installCmd
						}
					}
					
					if output, err := rm.executeCommand(installCmd, 2*time.Minute); err != nil {
						rm.logger.Info("Direct installation of PostgreSQL 17 failed, adding PostgreSQL APT repository", slog.String("error", err.Error()))
						
						// Add PostgreSQL APT repository for version 17
						lsbInstallCmd := "apt-get update && apt-get install -y lsb-release"
						if os.Geteuid() != 0 {
							if _, err := rm.executeCommand("command -v sudo", 5*time.Second); err == nil {
								lsbInstallCmd = "sudo " + lsbInstallCmd
							}
						}
						rm.executeCommand(lsbInstallCmd, 1*time.Minute)
						
						codenameOutput, _ := rm.executeCommand("lsb_release -cs", 5*time.Second)
						actualCodename := strings.TrimSpace(codenameOutput)
						if actualCodename == "" {
							actualCodename = "bookworm"
						}
						
						repoSetupCmd := fmt.Sprintf(`
							apt-get install -y wget ca-certificates &&
							wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - &&
							echo "deb http://apt.postgresql.org/pub/repos/apt/ %s-pgdg main" > /etc/apt/sources.list.d/pgdg.list &&
							apt-get update &&
							apt-get install -y postgresql-client-17
						`, actualCodename)
						
						if os.Geteuid() != 0 {
							if _, err := rm.executeCommand("command -v sudo", 5*time.Second); err == nil {
								installCmd = fmt.Sprintf("sudo sh -c '%s'", repoSetupCmd)
							}
						} else {
							installCmd = repoSetupCmd
						}
						
						output, err = rm.executeCommand(installCmd, 5*time.Minute)
						if err != nil {
							rm.logger.Error("Failed to install PostgreSQL 17 client tools", 
								slog.String("error", err.Error()),
								slog.String("output", output))
							return fmt.Errorf("restore failed - backup requires PostgreSQL 17 or newer (dump format 1.16): %w", err)
						}
					}
					
					// Check if pg_restore 17 is now available
					versionCheck := "pg_restore --version 2>&1 | grep -o 'PostgreSQL) [0-9]*' | grep -o '[0-9]*'"
					newVersion, _ := rm.executeCommand(versionCheck, 5*time.Second)
					newVersion = strings.TrimSpace(newVersion)
					
					if newVersion == "17" {
						rm.logger.Info("PostgreSQL 17 client tools installed successfully, retrying restore...")
						output, err = rm.executeCommand(restoreCmd, rm.config.Timeouts.BackupOp)
						if err == nil {
							rm.logger.Info("Restore succeeded with PostgreSQL 17 client")
							goto restore_success
						}
					}
				}
				
				rm.logger.Error("The backup was created with PostgreSQL 17 or newer",
					slog.String("dump_format", "1.16"),
					slog.String("solution", "Please install PostgreSQL 17 client tools or enable auto_install in config"))
				
				return fmt.Errorf("restore failed - backup requires PostgreSQL 17 or newer (dump format 1.16): %w (output: %s)", err, output)
			}
			
			// Try to suggest installation of newer version
			if rm.sshClient == nil && rm.config.Restore.AutoInstall {
				rm.logger.Info("Attempting to install newer PostgreSQL client tools...")
				if err := rm.tryInstallSpecificPostgreSQLVersion(backupVersion); err != nil {
					rm.logger.Error("Failed to auto-install newer PostgreSQL version",
						slog.String("error", err.Error()))
				} else {
					// Retry the restore with new version
					rm.logger.Info("Retrying restore with updated PostgreSQL client...")
					output, err = rm.executeCommand(restoreCmd, rm.config.Timeouts.BackupOp)
					if err == nil {
						rm.logger.Info("Restore succeeded with updated PostgreSQL client")
						goto restore_success
					}
				}
			}
			
			return fmt.Errorf("restore failed due to PostgreSQL version mismatch - backup requires PostgreSQL %s or newer: %w (output: %s)", backupVersion, err, output)
		} else if strings.Contains(output, "WARNING") && !strings.Contains(output, "ERROR") {
			rm.logger.Warn("Restore completed with warnings", slog.String("output", output))
		} else {
			return fmt.Errorf("restore failed: %w (output: %s)", err, output)
		}
	}
	
restore_success:

	// Verify restore by checking table count
	// Quote database name to handle special characters
	verifyCmd := fmt.Sprintf(
		"%s psql -h %s -p %d -U %s -d \"%s\" -t -c \"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';\"",
		pgPassword,
		rm.config.Restore.TargetHost,
		rm.config.Restore.TargetPort,
		rm.config.Restore.TargetUsername,
		rm.config.Restore.TargetDatabase,
	)

	tableCount, err := rm.executeCommand(verifyCmd, 30*time.Second)
	if err != nil {
		rm.logger.Warn("Failed to verify restore", slog.String("error", err.Error()))
	} else {
		count := strings.TrimSpace(tableCount)
		rm.logger.Info("Restore verification", slog.String("public_tables", count))
	}

	rm.logger.Info("Database restore completed successfully")
	return nil
}

func (rm *RestoreManager) cleanup() {
	if rm.sshClient != nil {
		rm.sshClient.Close()
	}
}