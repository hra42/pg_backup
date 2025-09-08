package notification

import (
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"time"

	"github.com/hra42/pg_backup/internal/config"
)

type NotificationClient struct {
	config *config.NotificationConfig
	logger *slog.Logger
}

func NewNotificationClient(cfg *config.NotificationConfig, logger *slog.Logger) *NotificationClient {
	return &NotificationClient{
		config: cfg,
		logger: logger,
	}
}

func (n *NotificationClient) SendBackupSuccess(database string, duration time.Duration, backupSize int64) error {
	if !n.config.Enabled {
		return nil
	}

	subject := fmt.Sprintf("✓ Backup Successful: %s", database)
	
	// Format backup size
	sizeStr := formatBytes(backupSize)
	
	text := fmt.Sprintf(
		"PostgreSQL backup completed successfully.\n\n"+
			"Database: %s\n"+
			"Duration: %s\n"+
			"Backup Size: %s\n"+
			"Timestamp: %s\n",
		database,
		duration.Round(time.Second),
		sizeStr,
		time.Now().UTC().Format(time.RFC3339),
	)

	return n.sendNotification(subject, text)
}

func (n *NotificationClient) SendBackupFailure(database string, err error, stage string) error {
	if !n.config.Enabled {
		return nil
	}

	subject := fmt.Sprintf("✗ Backup Failed: %s", database)
	
	text := fmt.Sprintf(
		"PostgreSQL backup failed.\n\n"+
			"Database: %s\n"+
			"Failed Stage: %s\n"+
			"Error: %s\n"+
			"Timestamp: %s\n",
		database,
		stage,
		err.Error(),
		time.Now().UTC().Format(time.RFC3339),
	)

	return n.sendNotification(subject, text)
}

func (n *NotificationClient) sendNotification(subject, text string) error {
	args := []string{
		"-api-key", n.config.APIKey,
		"-from", n.config.From,
		"-to", n.config.To,
		"-subject", subject,
		"-text", text,
	}

	if n.config.ReplyTo != "" {
		args = append(args, "-reply-to", n.config.ReplyTo)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, n.config.BinaryPath, args...)
	
	n.logger.Debug("Sending notification",
		slog.String("subject", subject),
		slog.String("to", n.config.To))

	output, err := cmd.CombinedOutput()
	if err != nil {
		n.logger.Error("Failed to send notification",
			slog.String("error", err.Error()),
			slog.String("output", string(output)))
		return fmt.Errorf("notification failed: %w", err)
	}

	n.logger.Info("Notification sent successfully",
		slog.String("subject", subject))
	
	return nil
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func (n *NotificationClient) SendRestoreSuccess(database string, duration time.Duration, backupKey string) error {
	if !n.config.Enabled {
		return nil
	}

	subject := fmt.Sprintf("✓ Restore Successful: %s", database)
	
	text := fmt.Sprintf(
		"PostgreSQL restore completed successfully.\n\n"+
			"Database: %s\n"+
			"Backup Used: %s\n"+
			"Duration: %s\n"+
			"Timestamp: %s\n",
		database,
		backupKey,
		duration.Round(time.Second),
		time.Now().UTC().Format(time.RFC3339),
	)

	return n.sendNotification(subject, text)
}

func (n *NotificationClient) SendRestoreFailure(database string, err error, stage string) error {
	if !n.config.Enabled {
		return nil
	}

	subject := fmt.Sprintf("✗ Restore Failed: %s", database)
	
	text := fmt.Sprintf(
		"PostgreSQL restore failed.\n\n"+
			"Database: %s\n"+
			"Failed Stage: %s\n"+
			"Error: %s\n"+
			"Timestamp: %s\n",
		database,
		stage,
		err.Error(),
		time.Now().UTC().Format(time.RFC3339),
	)

	return n.sendNotification(subject, text)
}

func GetBackupStage(err error) string {
	errStr := err.Error()
	if strings.Contains(errStr, "exit code 2") || strings.Contains(errStr, "SSH") {
		return "SSH Connection"
	}
	if strings.Contains(errStr, "exit code 3") || strings.Contains(errStr, "backup creation") {
		return "Remote Backup Creation"
	}
	if strings.Contains(errStr, "exit code 4") || strings.Contains(errStr, "transfer") {
		return "File Transfer"
	}
	if strings.Contains(errStr, "exit code 5") || strings.Contains(errStr, "S3") {
		return "S3 Upload"
	}
	if strings.Contains(errStr, "cleanup") {
		return "Cleanup"
	}
	return "Unknown"
}