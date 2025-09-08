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
)

type Scheduler struct {
	config        *config.Config
	logger        *slog.Logger
	scheduler     gocron.Scheduler
	backupManager *backup.BackupManager
	jobID         uuid.UUID
}

func NewScheduler(cfg *config.Config, logger *slog.Logger) (*Scheduler, error) {
	s, err := gocron.NewScheduler()
	if err != nil {
		return nil, fmt.Errorf("failed to create scheduler: %w", err)
	}

	backupManager, err := backup.NewBackupManager(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize backup manager: %w", err)
	}

	return &Scheduler{
		config:        cfg,
		logger:        logger,
		scheduler:     s,
		backupManager: backupManager,
	}, nil
}

func (s *Scheduler) Start(ctx context.Context) error {
	s.logger.Info("Starting scheduler")

	// Schedule backup job based on configuration
	job, err := s.scheduleBackupJob()
	if err != nil {
		return fmt.Errorf("failed to schedule backup job: %w", err)
	}

	s.jobID = job.ID()
	s.logger.Info("Backup job scheduled",
		slog.String("job_id", s.jobID.String()),
		slog.String("schedule", s.config.Schedule.Expression))

	// Start the scheduler
	s.scheduler.Start()

	// Wait for context cancellation
	<-ctx.Done()

	s.logger.Info("Stopping scheduler")
	return s.Stop()
}

func (s *Scheduler) scheduleBackupJob() (gocron.Job, error) {
	// Create job definition based on schedule type
	var jobDef gocron.JobDefinition

	switch s.config.Schedule.Type {
	case "cron":
		jobDef = gocron.CronJob(s.config.Schedule.Expression, false)
	case "interval":
		duration, err := time.ParseDuration(s.config.Schedule.Expression)
		if err != nil {
			return nil, fmt.Errorf("invalid interval duration: %w", err)
		}
		jobDef = gocron.DurationJob(duration)
	case "daily":
		// Parse time in HH:MM format
		t, err := time.Parse("15:04", s.config.Schedule.Expression)
		if err != nil {
			return nil, fmt.Errorf("invalid daily time format (expected HH:MM): %w", err)
		}
		jobDef = gocron.DailyJob(1, gocron.NewAtTimes(
			gocron.NewAtTime(uint(t.Hour()), uint(t.Minute()), 0),
		))
	case "weekly":
		// Parse day and time (e.g., "Monday 02:00")
		weekday, timeStr, err := parseWeeklySchedule(s.config.Schedule.Expression)
		if err != nil {
			return nil, fmt.Errorf("invalid weekly schedule format: %w", err)
		}
		t, err := time.Parse("15:04", timeStr)
		if err != nil {
			return nil, fmt.Errorf("invalid time format in weekly schedule: %w", err)
		}
		jobDef = gocron.WeeklyJob(1, 
			gocron.NewWeekdays(weekday),
			gocron.NewAtTimes(
				gocron.NewAtTime(uint(t.Hour()), uint(t.Minute()), 0),
			))
	case "monthly":
		// Parse day and time (e.g., "15 02:00" for 15th at 2 AM)
		day, timeStr, err := parseMonthlySchedule(s.config.Schedule.Expression)
		if err != nil {
			return nil, fmt.Errorf("invalid monthly schedule format: %w", err)
		}
		t, err := time.Parse("15:04", timeStr)
		if err != nil {
			return nil, fmt.Errorf("invalid time format in monthly schedule: %w", err)
		}
		jobDef = gocron.MonthlyJob(1,
			gocron.NewDaysOfTheMonth(day),
			gocron.NewAtTimes(
				gocron.NewAtTime(uint(t.Hour()), uint(t.Minute()), 0),
			))
	default:
		return nil, fmt.Errorf("unsupported schedule type: %s", s.config.Schedule.Type)
	}

	// Create the job with error handling
	job, err := s.scheduler.NewJob(
		jobDef,
		gocron.NewTask(s.runBackup),
		gocron.WithName("pg_backup"),
		gocron.WithSingletonMode(gocron.LimitModeReschedule),
		gocron.WithEventListeners(
			gocron.AfterJobRuns(s.afterJobRun),
			gocron.AfterJobRunsWithError(s.afterJobError),
		),
	)

	if err != nil {
		return nil, err
	}

	// If run on start is enabled, trigger the job immediately
	if s.config.Schedule.RunOnStart {
		s.logger.Info("Running backup on start as configured")
		go func() {
			time.Sleep(2 * time.Second) // Small delay to ensure everything is initialized
			s.runBackup()
		}()
	}

	return job, nil
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

func (s *Scheduler) afterJobRun(jobID uuid.UUID, jobName string) {
	s.logger.Info("Backup job completed successfully",
		slog.String("job_id", jobID.String()),
		slog.String("job_name", jobName))
	
	// Get next run time
	jobs := s.scheduler.Jobs()
	for _, job := range jobs {
		if job.ID() == jobID {
			nextRun, err := job.NextRun()
			if err == nil {
				s.logger.Info("Next backup scheduled",
					slog.Time("next_run", nextRun))
			}
			break
		}
	}
}

func (s *Scheduler) afterJobError(jobID uuid.UUID, jobName string, err error) {
	s.logger.Error("Backup job failed",
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