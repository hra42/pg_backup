package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/DeRuina/timberjack"
	"github.com/hra42/pg_backup/internal/backup"
	"github.com/hra42/pg_backup/internal/config"
	"github.com/hra42/pg_backup/internal/restore"
	"github.com/hra42/pg_backup/internal/scheduler"
	"github.com/hra42/pg_backup/internal/storage"
)

var (
	version   = "1.0.0"
	buildTime = "unknown"
	gitCommit = "unknown"
)

func main() {
	var (
		configPath    = flag.String("config", "config.yaml", "Path to configuration file")
		dryRun        = flag.Bool("dry-run", false, "Test configuration without performing backup")
		showVersion   = flag.Bool("version", false, "Show version information")
		logLevel      = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
		jsonLogs      = flag.Bool("json-logs", false, "Output logs in JSON format")
		restoreMode   = flag.Bool("restore", false, "Run in restore mode")
		listBackups   = flag.Bool("list-backups", false, "List available backups")
		backupKey     = flag.String("backup-key", "", "Specific backup key to restore (optional, uses latest if not specified)")
		cleanupOnly   = flag.Bool("cleanup", false, "Run cleanup only (remove old backups based on retention policy)")
		scheduleMode  = flag.Bool("schedule", false, "Run in scheduled mode using gocron")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("pg_backup %s\n", version)
		fmt.Printf("Build time: %s\n", buildTime)
		fmt.Printf("Git commit: %s\n", gitCommit)
		fmt.Printf("Go version: %s\n", runtime.Version())
		os.Exit(0)
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	logger := setupLogger(*logLevel, *jsonLogs, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Warn("Received signal, initiating graceful shutdown",
			slog.String("signal", sig.String()))
		cancel()
		time.Sleep(5 * time.Second)
		logger.Error("Forced shutdown after timeout")
		os.Exit(130)
	}()

	// Handle cleanup-only mode
	if *cleanupOnly {
		logger.Info("Running cleanup only mode")
		
		s3Client, err := storage.NewS3Client(&cfg.S3, logger)
		if err != nil {
			logger.Error("Failed to initialize S3 client", slog.String("error", err.Error()))
			os.Exit(1)
		}
		
		logger.Info("Starting backup cleanup", slog.Int("retention_count", cfg.Backup.RetentionCount))
		if err := s3Client.CleanupOldBackups(ctx, cfg.Backup.RetentionCount); err != nil {
			logger.Error("Cleanup failed", slog.String("error", err.Error()))
			os.Exit(1)
		}
		
		logger.Info("Cleanup completed successfully")
		os.Exit(0)
	}

	// Handle restore mode
	if *restoreMode || *listBackups {
		if !cfg.Restore.Enabled && !*listBackups {
			logger.Error("Restore feature is not enabled in configuration")
			os.Exit(1)
		}

		restoreManager, err := restore.NewRestoreManager(cfg, logger)
		if err != nil {
			logger.Error("Failed to initialize restore manager", slog.String("error", err.Error()))
			os.Exit(1)
		}

		if *listBackups {
			logger.Info("Listing available backups")
			backups, err := restoreManager.ListAvailableBackups(ctx)
			if err != nil {
				logger.Error("Failed to list backups", slog.String("error", err.Error()))
				os.Exit(1)
			}

			if len(backups) == 0 {
				logger.Info("No backups found")
			} else {
				logger.Info("Available backups:")
				for i, backup := range backups {
					fmt.Printf("%d. %s\n", i+1, backup)
				}
			}
			os.Exit(0)
		}

		logger.Info("Starting restore",
			slog.String("version", version),
			slog.String("config", *configPath),
			slog.String("backup_key", *backupKey))

		startTime := time.Now()
		if err := restoreManager.Run(ctx, *backupKey); err != nil {
			logger.Error("Restore failed",
				slog.String("error", err.Error()),
				slog.Duration("duration", time.Since(startTime)))
			os.Exit(1)
		}

		logger.Info("Restore completed successfully",
			slog.Duration("duration", time.Since(startTime)))
		os.Exit(0)
	}

	// Check if we should run in scheduled mode
	if *scheduleMode || cfg.Schedule.Enabled {
		if !cfg.Schedule.Enabled {
			logger.Error("Schedule mode requested but scheduling is not enabled in configuration")
			os.Exit(1)
		}

		logger.Info("Starting pg_backup in scheduled mode",
			slog.String("version", version),
			slog.String("config", *configPath),
			slog.String("schedule_type", cfg.Schedule.Type),
			slog.String("schedule_expression", cfg.Schedule.Expression))

		scheduler, err := scheduler.NewScheduler(cfg, logger)
		if err != nil {
			logger.Error("Failed to initialize scheduler", slog.String("error", err.Error()))
			os.Exit(1)
		}

		if err := scheduler.Start(ctx); err != nil {
			logger.Error("Scheduler failed", slog.String("error", err.Error()))
			os.Exit(1)
		}

		logger.Info("Scheduler stopped")
		os.Exit(0)
	}

	// Normal backup mode (single run)
	logger.Info("Starting pg_backup",
		slog.String("version", version),
		slog.String("config", *configPath),
		slog.Bool("dry_run", *dryRun))

	backupManager, err := backup.NewBackupManager(cfg, logger)
	if err != nil {
		logger.Error("Failed to initialize backup manager", slog.String("error", err.Error()))
		os.Exit(1)
	}

	backupManager.SetCancelFunc(cancel)

	startTime := time.Now()
	if err := backupManager.Run(ctx, *dryRun); err != nil {
		logger.Error("Backup failed",
			slog.String("error", err.Error()),
			slog.Duration("duration", time.Since(startTime)))

		switch {
		case contains(err.Error(), "exit code 2"):
			os.Exit(2)
		case contains(err.Error(), "exit code 3"):
			os.Exit(3)
		case contains(err.Error(), "exit code 4"):
			os.Exit(4)
		case contains(err.Error(), "exit code 5"):
			os.Exit(5)
		case contains(err.Error(), "cleanup"):
			os.Exit(6)
		default:
			os.Exit(1)
		}
	}

	logger.Info("Backup completed successfully",
		slog.Duration("duration", time.Since(startTime)))
	os.Exit(0)
}

func setupLogger(level string, jsonFormat bool, cfg *config.Config) *slog.Logger {
	var logLevel slog.Level
	switch level {
	case "debug":
		logLevel = slog.LevelDebug
	case "info":
		logLevel = slog.LevelInfo
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: logLevel,
		AddSource: false,
	}

	var writer io.Writer = os.Stdout
	
	// If log file path is configured, set up file logging with rotation
	if cfg.Log.FilePath != "" {
		// Ensure log directory exists
		logDir := filepath.Dir(cfg.Log.FilePath)
		if err := os.MkdirAll(logDir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create log directory %s: %v\n", logDir, err)
			os.Exit(1)
		}
		
		// Configure timberjack for log rotation
		tj := &timberjack.Logger{
			Filename:   cfg.Log.FilePath,
			MaxSize:    cfg.Log.MaxSize,    // megabytes
			MaxBackups: cfg.Log.MaxBackups, // number of backups
			MaxAge:     cfg.Log.MaxAge,     // days
			Compress:   cfg.Log.Compress,   // compress rotated files
			LocalTime:  true,                  // use local time for rotation
		}
		
		// Configure time-based rotation if specified
		if cfg.Log.RotationTime != "" {
			switch cfg.Log.RotationTime {
			case "hourly":
				tj.RotationInterval = time.Hour
				// Rotate at specific minute of each hour
				if cfg.Log.RotationMinute >= 0 && cfg.Log.RotationMinute <= 59 {
					tj.RotateAtMinutes = []int{cfg.Log.RotationMinute}
				}
			case "daily":
				// Rotate at specific time each day (00:00 by default)
				rotateTime := fmt.Sprintf("%02d:%02d", 0, cfg.Log.RotationMinute)
				tj.RotateAt = []string{rotateTime}
			case "weekly":
				tj.RotationInterval = 7 * 24 * time.Hour
			default:
				// Try to parse as duration
				if d, err := time.ParseDuration(cfg.Log.RotationTime); err == nil {
					tj.RotationInterval = d
				} else {
					// Default to daily if invalid
					tj.RotationInterval = 24 * time.Hour
				}
			}
		}
		
		writer = tj
	}

	var handler slog.Handler
	if jsonFormat {
		handler = slog.NewJSONHandler(writer, opts)
	} else {
		handler = slog.NewTextHandler(writer, opts)
	}

	return slog.New(handler)
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[len(s)-len(substr):] == substr || 
		   len(s) >= len(substr) && s[:len(substr)] == substr ||
		   len(s) > len(substr) && containsMiddle(s, substr)
}

func containsMiddle(s, substr string) bool {
	for i := 1; i < len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}