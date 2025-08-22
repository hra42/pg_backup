package main

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
)

type RestoreManager struct {
	config             *Config
	sshClient          *SSHClient
	s3Client           *S3Client
	notificationClient *NotificationClient
	logger             *slog.Logger
}

func NewRestoreManager(config *Config, logger *slog.Logger) (*RestoreManager, error) {
	var sshClient *SSHClient
	var err error
	
	// Check if SSH is needed for restore
	useSSH := true
	if config.Restore.UseSSH != nil {
		useSSH = *config.Restore.UseSSH
	}
	
	if useSSH {
		// Use restore SSH config if provided, otherwise use backup SSH config
		sshConfig := config.Restore.SSH
		if sshConfig == nil {
			sshConfig = &config.SSH
		}
		
		sshClient, err = NewSSHClient(sshConfig, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create SSH client for restore: %w", err)
		}
	} else {
		logger.Info("Local restore mode - SSH connection disabled")
		sshClient = nil
	}

	s3Client, err := NewS3Client(&config.S3, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	notificationClient := NewNotificationClient(&config.Notification, logger)

	return &RestoreManager{
		config:             config,
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
	rsyncClient := NewRsyncClient(sshConfig, rm.logger)
	
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

func (rm *RestoreManager) performRestore(remoteBackupPath string) error {
	rm.logger.Info("Performing database restore",
		slog.String("backup_file", remoteBackupPath),
		slog.String("target_database", rm.config.Restore.TargetDatabase))

	// Check if pg_restore exists
	output, err := rm.sshClient.ExecuteCommand("which pg_restore", 10*time.Second)
	if err != nil || strings.TrimSpace(output) == "" {
		return fmt.Errorf("pg_restore not found on remote server")
	}

	pgPassword := fmt.Sprintf("PGPASSWORD='%s'", rm.config.Restore.TargetPassword)

	// Drop existing database if configured
	if rm.config.Restore.DropExisting {
		rm.logger.Info("Dropping existing database", slog.String("database", rm.config.Restore.TargetDatabase))
		
		dropCmd := fmt.Sprintf(
			"%s psql -h %s -p %d -U %s -d postgres -c \"DROP DATABASE IF EXISTS %s;\"",
			pgPassword,
			rm.config.Restore.TargetHost,
			rm.config.Restore.TargetPort,
			rm.config.Restore.TargetUsername,
			rm.config.Restore.TargetDatabase,
		)
		
		if output, err := rm.sshClient.ExecuteCommand(dropCmd, 30*time.Second); err != nil {
			return fmt.Errorf("failed to drop existing database: %w (output: %s)", err, output)
		}
	}

	// Create database if configured
	if rm.config.Restore.CreateDB {
		rm.logger.Info("Creating target database", slog.String("database", rm.config.Restore.TargetDatabase))
		
		createCmd := fmt.Sprintf(
			"%s psql -h %s -p %d -U %s -d postgres -c \"CREATE DATABASE %s",
			pgPassword,
			rm.config.Restore.TargetHost,
			rm.config.Restore.TargetPort,
			rm.config.Restore.TargetUsername,
			rm.config.Restore.TargetDatabase,
		)
		
		if rm.config.Restore.Owner != "" {
			createCmd += fmt.Sprintf(" OWNER %s", rm.config.Restore.Owner)
		}
		createCmd += ";\""
		
		if output, err := rm.sshClient.ExecuteCommand(createCmd, 30*time.Second); err != nil {
			// Check if database already exists
			if !strings.Contains(err.Error(), "already exists") && !strings.Contains(output, "already exists") {
				return fmt.Errorf("failed to create database: %w (output: %s)", err, output)
			}
			rm.logger.Info("Database already exists, continuing with restore")
		}
	}

	// Build pg_restore command
	restoreCmd := fmt.Sprintf(
		"%s pg_restore -h %s -p %d -U %s -d %s --verbose --no-owner --no-privileges --no-tablespaces",
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

	restoreCmd += fmt.Sprintf(" %s 2>&1", remoteBackupPath)

	// Execute restore (with extended timeout)
	rm.logger.Info("Executing pg_restore command", slog.Int("jobs", rm.config.Restore.Jobs))
	output, err = rm.sshClient.ExecuteCommand(restoreCmd, rm.config.Timeouts.BackupOp)
	
	if err != nil {
		// pg_restore may return warnings as errors, check output
		if strings.Contains(output, "WARNING") && !strings.Contains(output, "ERROR") {
			rm.logger.Warn("Restore completed with warnings", slog.String("output", output))
		} else {
			return fmt.Errorf("restore failed: %w (output: %s)", err, output)
		}
	}

	// Verify restore by checking table count
	verifyCmd := fmt.Sprintf(
		"%s psql -h %s -p %d -U %s -d %s -t -c \"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';\"",
		pgPassword,
		rm.config.Restore.TargetHost,
		rm.config.Restore.TargetPort,
		rm.config.Restore.TargetUsername,
		rm.config.Restore.TargetDatabase,
	)

	tableCount, err := rm.sshClient.ExecuteCommand(verifyCmd, 30*time.Second)
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