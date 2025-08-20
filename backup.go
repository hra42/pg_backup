package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type BackupManager struct {
	config             *Config
	sshClient          *SSHClient
	s3Client           *S3Client
	notificationClient *NotificationClient
	logger             *slog.Logger
	cancelFunc         context.CancelFunc
	backupSize         int64
}

func NewBackupManager(config *Config, logger *slog.Logger) (*BackupManager, error) {
	sshClient, err := NewSSHClient(&config.SSH, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSH client: %w", err)
	}

	s3Client, err := NewS3Client(&config.S3, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	notificationClient := NewNotificationClient(&config.Notification, logger)

	return &BackupManager{
		config:             config,
		sshClient:          sshClient,
		s3Client:           s3Client,
		notificationClient: notificationClient,
		logger:             logger,
	}, nil
}

func (bm *BackupManager) SetCancelFunc(cancel context.CancelFunc) {
	bm.cancelFunc = cancel
}

func (bm *BackupManager) Run(ctx context.Context, dryRun bool) error {
	defer bm.cleanup()
	startTime := time.Now()

	if dryRun {
		bm.logger.Info("DRY RUN MODE - No actual backup will be performed")
		return bm.validateConfiguration()
	}

	timestamp := time.Now().UTC().Format("20060102_150405")
	backupFileName := fmt.Sprintf("backup_%s.dump", timestamp)
	remoteBackupPath := filepath.Join(bm.config.Backup.TempDir, backupFileName)
	localBackupPath := filepath.Join(os.TempDir(), backupFileName)

	if err := bm.connectSSH(); err != nil {
		bm.notificationClient.SendBackupFailure(bm.config.Postgres.Database, err, getBackupStage(err))
		return err
	}

	if err := bm.createRemoteBackup(remoteBackupPath); err != nil {
		bm.notificationClient.SendBackupFailure(bm.config.Postgres.Database, err, getBackupStage(err))
		return err
	}

	if err := bm.transferBackup(remoteBackupPath, localBackupPath); err != nil {
		bm.notificationClient.SendBackupFailure(bm.config.Postgres.Database, err, getBackupStage(err))
		return err
	}

	// Get backup size for notification
	if stat, err := os.Stat(localBackupPath); err == nil {
		bm.backupSize = stat.Size()
	}

	if err := bm.uploadToS3(ctx, localBackupPath); err != nil {
		bm.notificationClient.SendBackupFailure(bm.config.Postgres.Database, err, getBackupStage(err))
		return err
	}

	if err := bm.performCleanup(ctx, localBackupPath); err != nil {
		bm.logger.Warn("Cleanup encountered errors", slog.String("error", err.Error()))
	}

	bm.logger.Info("Backup completed successfully", slog.String("file", backupFileName))
	
	// Send success notification
	if bm.notificationClient != nil {
		duration := time.Since(startTime)
		if err := bm.notificationClient.SendBackupSuccess(bm.config.Postgres.Database, duration, bm.backupSize); err != nil {
			bm.logger.Warn("Failed to send success notification", slog.String("error", err.Error()))
		}
	}
	
	return nil
}

func (bm *BackupManager) validateConfiguration() error {
	bm.logger.Info("Validating configuration...")

	if err := bm.sshClient.Connect(bm.config.Timeouts.SSHConnection); err != nil {
		return fmt.Errorf("SSH validation failed: %w", err)
	}

	output, err := bm.sshClient.ExecuteCommand("which pg_dump", 10*time.Second)
	if err != nil || strings.TrimSpace(output) == "" {
		return fmt.Errorf("pg_dump not found on remote server")
	}
	bm.logger.Info("Found pg_dump", slog.String("path", strings.TrimSpace(output)))

	output, err = bm.sshClient.ExecuteCommand(fmt.Sprintf("test -w %s && echo writable", bm.config.Backup.TempDir), 10*time.Second)
	if err != nil || !strings.Contains(output, "writable") {
		return fmt.Errorf("temp directory %s is not writable", bm.config.Backup.TempDir)
	}

	// Check for rsync on local machine
	if _, err := exec.LookPath("rsync"); err != nil {
		return fmt.Errorf("rsync not found on local machine")
	}
	bm.logger.Info("Found rsync on local machine")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err = bm.s3Client.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: &bm.config.S3.Bucket,
	})
	if err != nil {
		return fmt.Errorf("S3 bucket validation failed: %w", err)
	}

	bm.logger.Info("Configuration validation successful")
	return nil
}

func (bm *BackupManager) connectSSH() error {
	bm.logger.Info("Stage 1: Establishing SSH connection")
	if err := bm.sshClient.Connect(bm.config.Timeouts.SSHConnection); err != nil {
		return fmt.Errorf("SSH connection failed (exit code 2): %w", err)
	}

	return nil
}

func (bm *BackupManager) createRemoteBackup(remoteBackupPath string) error {
	bm.logger.Info("Stage 2: Creating remote backup", slog.String("path", remoteBackupPath))

	// Use pg_dump for better compatibility (doesn't require replication privileges)
	pgPassword := fmt.Sprintf("PGPASSWORD='%s'", bm.config.Postgres.Password)
	
	// Create pg_dump command with custom format and compression
	// Custom format allows for parallel restore and selective restoration
	pgDumpCmd := fmt.Sprintf(
		"%s pg_dump -h %s -p %d -U %s -d %s --verbose --no-password --no-owner --no-privileges --no-tablespaces --no-security-labels --format=custom --compress=%d --file=%s 2>&1",
		pgPassword,
		bm.config.Postgres.Host,
		bm.config.Postgres.Port,
		bm.config.Postgres.Username,
		bm.config.Postgres.Database,
		bm.config.Backup.CompressionLvl,
		remoteBackupPath,
	)

	// Try to run the command and capture all output
	output, err := bm.sshClient.ExecuteCommand(pgDumpCmd, bm.config.Timeouts.BackupOp)
	
	if err != nil {
		// Try to get the error output from the file
		errorOutput, _ := bm.sshClient.ExecuteCommand(fmt.Sprintf("head -100 %s 2>/dev/null", remoteBackupPath), 5*time.Second)
		bm.sshClient.ExecuteCommand(fmt.Sprintf("rm -f %s", remoteBackupPath), 10*time.Second)
		
		errMsg := fmt.Sprintf("backup creation failed (exit code 3): %v", err)
		if errorOutput != "" {
			errMsg = fmt.Sprintf("%s\npg_dump output: %s", errMsg, errorOutput)
		}
		if output != "" {
			errMsg = fmt.Sprintf("%s\nCommand output: %s", errMsg, output)
		}
		return fmt.Errorf(errMsg)
	}

	statOutput, err := bm.sshClient.ExecuteCommand(fmt.Sprintf("stat -c %%s %s 2>/dev/null || stat -f %%z %s 2>/dev/null", remoteBackupPath, remoteBackupPath), 10*time.Second)
	if err != nil {
		return fmt.Errorf("failed to verify backup file (exit code 3): %w", err)
	}

	fileSize := strings.TrimSpace(statOutput)
	if fileSize == "" || fileSize == "0" {
		bm.sshClient.ExecuteCommand(fmt.Sprintf("rm -f %s", remoteBackupPath), 10*time.Second)
		return fmt.Errorf("backup file is empty (exit code 3)")
	}

	bm.logger.Info("Remote backup created successfully", slog.String("size", fileSize))
	return nil
}

func (bm *BackupManager) transferBackup(remoteBackupPath, localBackupPath string) error {
	bm.logger.Info("Stage 3: Transferring backup to local machine",
		slog.String("remote", remoteBackupPath),
		slog.String("local", localBackupPath))

	// Use rsync for file transfer
	rsyncClient := NewRsyncClient(&bm.config.SSH, bm.logger)
	
	lastProgress := time.Now()
	err := rsyncClient.DownloadFile(remoteBackupPath, localBackupPath, bm.config.Timeouts.Transfer, 
		func(transferred, total int64) {
			if time.Since(lastProgress) > 5*time.Second {
				percentage := float64(transferred) / float64(total) * 100
				bm.logger.Info("Transfer progress",
					slog.Float64("percentage", percentage),
					slog.Int64("transferred", transferred),
					slog.Int64("total", total))
				lastProgress = time.Now()
			}
		})

	if err != nil {
		os.Remove(localBackupPath)
		return fmt.Errorf("transfer failed (exit code 4): %w", err)
	}

	// Remove remote file after successful transfer
	if err := bm.sshClient.RemoveRemoteFile(remoteBackupPath); err != nil {
		bm.logger.Warn("Failed to remove remote backup file", slog.String("error", err.Error()))
	}

	return nil
}

func (bm *BackupManager) uploadToS3(ctx context.Context, localBackupPath string) error {
	bm.logger.Info("Stage 4: Uploading backup to S3", slog.String("file", localBackupPath))

	lastProgress := time.Now()
	err := bm.s3Client.UploadFile(ctx, localBackupPath, func(uploaded int64) {
		if time.Since(lastProgress) > 5*time.Second {
			bm.logger.Info("S3 upload progress", slog.Int64("uploaded", uploaded))
			lastProgress = time.Now()
		}
	})

	if err != nil {
		return fmt.Errorf("S3 upload failed (exit code 5): %w", err)
	}

	return nil
}

func (bm *BackupManager) performCleanup(ctx context.Context, localBackupPath string) error {
	bm.logger.Info("Stage 5: Performing cleanup")

	if err := os.Remove(localBackupPath); err != nil {
		bm.logger.Warn("Failed to remove local backup file", slog.String("error", err.Error()))
	} else {
		bm.logger.Info("Local backup file removed", slog.String("path", localBackupPath))
	}

	if err := bm.s3Client.CleanupOldBackups(ctx, bm.config.Backup.RetentionCount); err != nil {
		return fmt.Errorf("retention cleanup failed: %w", err)
	}

	return nil
}

func (bm *BackupManager) cleanup() {
	if bm.sshClient != nil {
		bm.sshClient.Close()
	}
}