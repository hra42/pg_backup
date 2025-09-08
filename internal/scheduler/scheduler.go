package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
	"github.com/hra42/pg_backup/internal/backup"
	"github.com/hra42/pg_backup/internal/config"
	"github.com/hra42/pg_backup/internal/restore"
	"github.com/hra42/pg_backup/internal/storage"
)

type Scheduler struct {
	config        *config.Config
	logger        *slog.Logger
	scheduler     gocron.Scheduler
	backupManager *backup.BackupManager
	restoreManager *restore.RestoreManager
	s3Client      *storage.S3Client
	jobs          map[string]uuid.UUID // Map task name to job ID
}

func NewScheduler(cfg *config.Config, logger *slog.Logger) (*Scheduler, error) {
	s, err := gocron.NewScheduler()
	if err != nil {
		return nil, fmt.Errorf("failed to create scheduler: %w", err)
	}

	scheduler := &Scheduler{
		config:    cfg,
		logger:    logger,
		scheduler: s,
		jobs:      make(map[string]uuid.UUID),
	}

	// Initialize managers as needed
	if cfg.Backup.Schedule != nil && cfg.Backup.Schedule.Enabled {
		backupManager, err := backup.NewBackupManager(cfg, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize backup manager: %w", err)
		}
		scheduler.backupManager = backupManager
	}

	if cfg.Restore.Enabled && cfg.Restore.Schedule != nil && cfg.Restore.Schedule.Enabled {
		restoreManager, err := restore.NewRestoreManager(cfg, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize restore manager: %w", err)
		}
		scheduler.restoreManager = restoreManager
	}

	if cfg.Cleanup != nil && cfg.Cleanup.Schedule != nil && cfg.Cleanup.Schedule.Enabled {
		s3Client, err := storage.NewS3Client(&cfg.S3, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize S3 client for cleanup: %w", err)
		}
		scheduler.s3Client = s3Client
	}

	return scheduler, nil
}

func (s *Scheduler) Start(ctx context.Context) error {
	s.logger.Info("Starting scheduler")

	// Schedule backup job if configured
	if s.config.Backup.Schedule != nil && s.config.Backup.Schedule.Enabled {
		job, err := s.scheduleJob("backup", s.config.Backup.Schedule, s.runBackup)
		if err != nil {
			return fmt.Errorf("failed to schedule backup job: %w", err)
		}
		s.jobs["backup"] = job.ID()
		s.logger.Info("Backup job scheduled",
			slog.String("job_id", job.ID().String()),
			slog.String("type", s.config.Backup.Schedule.Type),
			slog.String("expression", s.config.Backup.Schedule.Expression))
	}

	// Schedule restore job if configured
	if s.config.Restore.Enabled && s.config.Restore.Schedule != nil && s.config.Restore.Schedule.Enabled {
		job, err := s.scheduleJob("restore", s.config.Restore.Schedule, s.runRestore)
		if err != nil {
			return fmt.Errorf("failed to schedule restore job: %w", err)
		}
		s.jobs["restore"] = job.ID()
		s.logger.Info("Restore job scheduled",
			slog.String("job_id", job.ID().String()),
			slog.String("type", s.config.Restore.Schedule.Type),
			slog.String("expression", s.config.Restore.Schedule.Expression))
	}

	// Schedule cleanup job if configured
	if s.config.Cleanup != nil && s.config.Cleanup.Schedule != nil && s.config.Cleanup.Schedule.Enabled {
		job, err := s.scheduleJob("cleanup", s.config.Cleanup.Schedule, s.runCleanup)
		if err != nil {
			return fmt.Errorf("failed to schedule cleanup job: %w", err)
		}
		s.jobs["cleanup"] = job.ID()
		s.logger.Info("Cleanup job scheduled",
			slog.String("job_id", job.ID().String()),
			slog.String("type", s.config.Cleanup.Schedule.Type),
			slog.String("expression", s.config.Cleanup.Schedule.Expression))
	}

	if len(s.jobs) == 0 {
		return fmt.Errorf("no scheduled tasks configured")
	}

	// Start the scheduler
	s.scheduler.Start()

	s.logger.Info("Scheduler started",
		slog.Int("scheduled_jobs", len(s.jobs)))

	// Wait for context cancellation
	<-ctx.Done()

	s.logger.Info("Stopping scheduler")
	return s.Stop()
}

func (s *Scheduler) scheduleJob(name string, schedule *config.ScheduleConfig, task func() error) (gocron.Job, error) {
	// Create job definition based on schedule type
	jobDef, err := s.createJobDefinition(schedule)
	if err != nil {
		return nil, fmt.Errorf("failed to create job definition for %s: %w", name, err)
	}

	// Create the job with error handling
	job, err := s.scheduler.NewJob(
		jobDef,
		gocron.NewTask(task),
		gocron.WithName(fmt.Sprintf("pg_%s", name)),
		gocron.WithSingletonMode(gocron.LimitModeReschedule),
		gocron.WithEventListeners(
			gocron.AfterJobRuns(func(jobID uuid.UUID, jobName string) {
				s.afterJobRun(jobID, jobName, name)
			}),
			gocron.AfterJobRunsWithError(func(jobID uuid.UUID, jobName string, err error) {
				s.afterJobError(jobID, jobName, name, err)
			}),
		),
	)

	if err != nil {
		return nil, err
	}

	// If run on start is enabled, trigger the job immediately
	if schedule.RunOnStart {
		s.logger.Info(fmt.Sprintf("Running %s on start as configured", name))
		go func() {
			time.Sleep(2 * time.Second) // Small delay to ensure everything is initialized
			if err := task(); err != nil {
				s.logger.Error(fmt.Sprintf("Failed to run initial %s", name), 
					slog.String("error", err.Error()))
			}
		}()
	}

	return job, nil
}

func (s *Scheduler) createJobDefinition(schedule *config.ScheduleConfig) (gocron.JobDefinition, error) {
	switch schedule.Type {
	case "cron":
		return gocron.CronJob(schedule.Expression, false), nil
	case "interval":
		duration, err := time.ParseDuration(schedule.Expression)
		if err != nil {
			return nil, fmt.Errorf("invalid interval duration: %w", err)
		}
		return gocron.DurationJob(duration), nil
	case "daily":
		// Parse time in HH:MM format
		t, err := time.Parse("15:04", schedule.Expression)
		if err != nil {
			return nil, fmt.Errorf("invalid daily time format (expected HH:MM): %w", err)
		}
		return gocron.DailyJob(1, gocron.NewAtTimes(
			gocron.NewAtTime(uint(t.Hour()), uint(t.Minute()), 0),
		)), nil
	case "weekly":
		// Parse day and time (e.g., "Monday 02:00")
		weekday, timeStr, err := parseWeeklySchedule(schedule.Expression)
		if err != nil {
			return nil, fmt.Errorf("invalid weekly schedule format: %w", err)
		}
		t, err := time.Parse("15:04", timeStr)
		if err != nil {
			return nil, fmt.Errorf("invalid time format in weekly schedule: %w", err)
		}
		return gocron.WeeklyJob(1, 
			gocron.NewWeekdays(weekday),
			gocron.NewAtTimes(
				gocron.NewAtTime(uint(t.Hour()), uint(t.Minute()), 0),
			)), nil
	case "monthly":
		// Parse day and time (e.g., "15 02:00" for 15th at 2 AM)
		day, timeStr, err := parseMonthlySchedule(schedule.Expression)
		if err != nil {
			return nil, fmt.Errorf("invalid monthly schedule format: %w", err)
		}
		t, err := time.Parse("15:04", timeStr)
		if err != nil {
			return nil, fmt.Errorf("invalid time format in monthly schedule: %w", err)
		}
		return gocron.MonthlyJob(1,
			gocron.NewDaysOfTheMonth(day),
			gocron.NewAtTimes(
				gocron.NewAtTime(uint(t.Hour()), uint(t.Minute()), 0),
			)), nil
	default:
		return nil, fmt.Errorf("unsupported schedule type: %s", schedule.Type)
	}
}

func (s *Scheduler) runBackup() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeouts.BackupOp)
	defer cancel()

	s.logger.Info("Starting scheduled backup")
	startTime := time.Now()

	if err := s.backupManager.Run(ctx, false); err != nil {
		s.logger.Error("Scheduled backup failed",
			slog.String("error", err.Error()),
			slog.Duration("duration", time.Since(startTime)))
		return err
	}

	s.logger.Info("Scheduled backup completed successfully",
		slog.Duration("duration", time.Since(startTime)))
	return nil
}

func (s *Scheduler) runRestore() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeouts.BackupOp)
	defer cancel()

	s.logger.Info("Starting scheduled restore")
	startTime := time.Now()

	// Use backup key from config if specified, otherwise use latest
	backupKey := s.config.Restore.BackupKey
	
	if err := s.restoreManager.Run(ctx, backupKey); err != nil {
		s.logger.Error("Scheduled restore failed",
			slog.String("error", err.Error()),
			slog.Duration("duration", time.Since(startTime)))
		return err
	}

	s.logger.Info("Scheduled restore completed successfully",
		slog.Duration("duration", time.Since(startTime)))
	return nil
}

func (s *Scheduler) runCleanup() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeouts.BackupOp)
	defer cancel()

	s.logger.Info("Starting scheduled cleanup",
		slog.Int("retention_count", s.config.Backup.RetentionCount))
	startTime := time.Now()

	if err := s.s3Client.CleanupOldBackups(ctx, s.config.Backup.RetentionCount); err != nil {
		s.logger.Error("Scheduled cleanup failed",
			slog.String("error", err.Error()),
			slog.Duration("duration", time.Since(startTime)))
		return err
	}

	s.logger.Info("Scheduled cleanup completed successfully",
		slog.Duration("duration", time.Since(startTime)))
	return nil
}

func (s *Scheduler) afterJobRun(jobID uuid.UUID, jobName string, taskType string) {
	s.logger.Info(fmt.Sprintf("%s job completed successfully", taskType),
		slog.String("job_id", jobID.String()),
		slog.String("job_name", jobName))
	
	// Get next run time
	jobs := s.scheduler.Jobs()
	for _, job := range jobs {
		if job.ID() == jobID {
			nextRun, err := job.NextRun()
			if err == nil {
				s.logger.Info(fmt.Sprintf("Next %s scheduled", taskType),
					slog.Time("next_run", nextRun))
			}
			break
		}
	}
}

func (s *Scheduler) afterJobError(jobID uuid.UUID, jobName string, taskType string, err error) {
	s.logger.Error(fmt.Sprintf("%s job failed", taskType),
		slog.String("job_id", jobID.String()),
		slog.String("job_name", jobName),
		slog.String("error", err.Error()))
}

func (s *Scheduler) Stop() error {
	s.logger.Info("Shutting down scheduler")
	return s.scheduler.Shutdown()
}

// Helper functions for parsing schedule expressions
func parseWeeklySchedule(expr string) (time.Weekday, string, error) {
	// Expected format: "Monday 02:00"
	var dayStr, timeStr string
	if _, err := fmt.Sscanf(expr, "%s %s", &dayStr, &timeStr); err != nil {
		return 0, "", fmt.Errorf("expected format 'Weekday HH:MM': %w", err)
	}

	weekday, err := parseWeekday(dayStr)
	if err != nil {
		return 0, "", err
	}

	return weekday, timeStr, nil
}

func parseMonthlySchedule(expr string) (int, string, error) {
	// Expected format: "15 02:00" (day time)
	var day int
	var timeStr string
	if _, err := fmt.Sscanf(expr, "%d %s", &day, &timeStr); err != nil {
		return 0, "", fmt.Errorf("expected format 'DD HH:MM': %w", err)
	}

	if day < 1 || day > 31 {
		return 0, "", fmt.Errorf("day must be between 1 and 31")
	}

	return day, timeStr, nil
}

func parseWeekday(s string) (time.Weekday, error) {
	switch s {
	case "Sunday", "sunday", "Sun", "sun":
		return time.Sunday, nil
	case "Monday", "monday", "Mon", "mon":
		return time.Monday, nil
	case "Tuesday", "tuesday", "Tue", "tue":
		return time.Tuesday, nil
	case "Wednesday", "wednesday", "Wed", "wed":
		return time.Wednesday, nil
	case "Thursday", "thursday", "Thu", "thu":
		return time.Thursday, nil
	case "Friday", "friday", "Fri", "fri":
		return time.Friday, nil
	case "Saturday", "saturday", "Sat", "sat":
		return time.Saturday, nil
	default:
		return 0, fmt.Errorf("invalid weekday: %s", s)
	}
}