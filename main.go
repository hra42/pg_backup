package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
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
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("pg_backup %s\n", version)
		fmt.Printf("Build time: %s\n", buildTime)
		fmt.Printf("Git commit: %s\n", gitCommit)
		fmt.Printf("Go version: %s\n", runtime.Version())
		os.Exit(0)
	}

	logger := setupLogger(*logLevel, *jsonLogs)

	config, err := LoadConfig(*configPath)
	if err != nil {
		logger.Error("Failed to load configuration", slog.String("error", err.Error()))
		os.Exit(1)
	}

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

	// Handle restore mode
	if *restoreMode || *listBackups {
		if !config.Restore.Enabled && !*listBackups {
			logger.Error("Restore feature is not enabled in configuration")
			os.Exit(1)
		}

		restoreManager, err := NewRestoreManager(config, logger)
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

	// Normal backup mode
	logger.Info("Starting pg_backup",
		slog.String("version", version),
		slog.String("config", *configPath),
		slog.Bool("dry_run", *dryRun))

	backupManager, err := NewBackupManager(config, logger)
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

func setupLogger(level string, jsonFormat bool) *slog.Logger {
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

	var handler slog.Handler
	if jsonFormat {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
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