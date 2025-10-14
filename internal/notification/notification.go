package notification

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/hra42/pg_backup/internal/config"
)

// EventType represents the type of notification event
type EventType string

const (
	EventBackupSuccess  EventType = "backup_success"
	EventBackupFailure  EventType = "backup_failure"
	EventRestoreSuccess EventType = "restore_success"
	EventRestoreFailure EventType = "restore_failure"
)

// NotificationPayload represents the JSON payload sent to the webhook
type NotificationPayload struct {
	EventType    EventType `json:"event_type"`
	Database     string    `json:"database"`
	Timestamp    string    `json:"timestamp"`
	Duration     *string   `json:"duration,omitempty"`     // Duration in human-readable format (for success events)
	DurationMs   *int64    `json:"duration_ms,omitempty"`  // Duration in milliseconds (for success events)
	BackupSize   *int64    `json:"backup_size,omitempty"`  // Backup size in bytes (for backup success)
	BackupKey    *string   `json:"backup_key,omitempty"`   // Backup key/identifier (for restore events)
	Error        *string   `json:"error,omitempty"`        // Error message (for failure events)
	Stage        *string   `json:"stage,omitempty"`        // Failed stage (for failure events)
	Hostname     string    `json:"hostname,omitempty"`     // Hostname where the backup/restore ran
	Version      string    `json:"version,omitempty"`      // Application version
}

type NotificationClient struct {
	config     *config.NotificationConfig
	logger     *slog.Logger
	httpClient *http.Client
}

func NewNotificationClient(cfg *config.NotificationConfig, logger *slog.Logger) *NotificationClient {
	return &NotificationClient{
		config: cfg,
		logger: logger,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (n *NotificationClient) SendBackupSuccess(database string, duration time.Duration, backupSize int64) error {
	if !n.config.Enabled {
		return nil
	}

	durationStr := duration.Round(time.Second).String()
	durationMs := duration.Milliseconds()

	payload := NotificationPayload{
		EventType:  EventBackupSuccess,
		Database:   database,
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		Duration:   &durationStr,
		DurationMs: &durationMs,
		BackupSize: &backupSize,
		Hostname:   getHostname(),
		Version:    getVersion(),
	}

	return n.sendWebhook(payload)
}

func (n *NotificationClient) SendBackupFailure(database string, err error, stage string) error {
	if !n.config.Enabled {
		return nil
	}

	errMsg := err.Error()

	payload := NotificationPayload{
		EventType: EventBackupFailure,
		Database:  database,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Error:     &errMsg,
		Stage:     &stage,
		Hostname:  getHostname(),
		Version:   getVersion(),
	}

	return n.sendWebhook(payload)
}

func (n *NotificationClient) sendWebhook(payload NotificationPayload) error {
	if n.config.WebhookURL == "" {
		n.logger.Warn("Webhook URL not configured, skipping notification")
		return nil
	}

	// Marshal payload to JSON
	jsonData, err := json.Marshal(payload)
	if err != nil {
		n.logger.Error("Failed to marshal notification payload",
			slog.String("error", err.Error()))
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Create HTTP request
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", n.config.WebhookURL, bytes.NewBuffer(jsonData))
	if err != nil {
		n.logger.Error("Failed to create webhook request",
			slog.String("error", err.Error()))
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", fmt.Sprintf("pg_backup/%s", getVersion()))

	// Add custom headers from config
	for key, value := range n.config.Headers {
		req.Header.Set(key, value)
	}

	n.logger.Debug("Sending webhook notification",
		slog.String("url", n.config.WebhookURL),
		slog.String("event_type", string(payload.EventType)),
		slog.String("database", payload.Database))

	// Send request
	resp, err := n.httpClient.Do(req)
	if err != nil {
		n.logger.Error("Failed to send webhook notification",
			slog.String("error", err.Error()),
			slog.String("url", n.config.WebhookURL))
		return fmt.Errorf("webhook request failed: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		n.logger.Error("Webhook returned error status",
			slog.Int("status_code", resp.StatusCode),
			slog.String("status", resp.Status),
			slog.String("url", n.config.WebhookURL))
		return fmt.Errorf("webhook returned status %d: %s", resp.StatusCode, resp.Status)
	}

	n.logger.Info("Webhook notification sent successfully",
		slog.String("event_type", string(payload.EventType)),
		slog.Int("status_code", resp.StatusCode))

	return nil
}

func (n *NotificationClient) SendRestoreSuccess(database string, duration time.Duration, backupKey string) error {
	if !n.config.Enabled {
		return nil
	}

	durationStr := duration.Round(time.Second).String()
	durationMs := duration.Milliseconds()

	payload := NotificationPayload{
		EventType:  EventRestoreSuccess,
		Database:   database,
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		Duration:   &durationStr,
		DurationMs: &durationMs,
		BackupKey:  &backupKey,
		Hostname:   getHostname(),
		Version:    getVersion(),
	}

	return n.sendWebhook(payload)
}

func (n *NotificationClient) SendRestoreFailure(database string, err error, stage string) error {
	if !n.config.Enabled {
		return nil
	}

	errMsg := err.Error()

	payload := NotificationPayload{
		EventType: EventRestoreFailure,
		Database:  database,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Error:     &errMsg,
		Stage:     &stage,
		Hostname:  getHostname(),
		Version:   getVersion(),
	}

	return n.sendWebhook(payload)
}

// GetBackupStage determines the stage of backup failure from error message
func GetBackupStage(err error) string {
	errStr := err.Error()
	if len(errStr) == 0 {
		return "Unknown"
	}

	// Check for specific error patterns
	patterns := map[string]string{
		"exit code 2": "SSH Connection",
		"SSH":         "SSH Connection",
		"exit code 3": "Remote Backup Creation",
		"backup creation": "Remote Backup Creation",
		"exit code 4": "File Transfer",
		"transfer":    "File Transfer",
		"exit code 5": "S3 Upload",
		"S3":          "S3 Upload",
		"cleanup":     "Cleanup",
	}

	for pattern, stage := range patterns {
		if containsIgnoreCase(errStr, pattern) {
			return stage
		}
	}

	return "Unknown"
}

// Helper functions
func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}

func getVersion() string {
	// This should match the version in main.go
	// For now, return a placeholder - this can be set via build flags
	return "1.0.0"
}

func containsIgnoreCase(s, substr string) bool {
	return len(s) >= len(substr) &&
		(contains(s, substr) || contains(toLower(s), toLower(substr)))
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func toLower(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			result[i] = c + ('a' - 'A')
		} else {
			result[i] = c
		}
	}
	return string(result)
}