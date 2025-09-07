package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	SSH          SSHConfig          `yaml:"ssh"`
	Postgres     PostgresConfig     `yaml:"postgres"`
	S3           S3Config           `yaml:"s3"`
	Backup       BackupConfig       `yaml:"backup"`
	Restore      RestoreConfig      `yaml:"restore"`
	Timeouts     TimeoutConfig      `yaml:"timeouts"`
	Notification NotificationConfig `yaml:"notification"`
	Log          LogConfig          `yaml:"log"`
}

type SSHConfig struct {
	Host       string `yaml:"host"`
	Port       int    `yaml:"port"`
	Username   string `yaml:"username"`
	Password   string `yaml:"password,omitempty"`
	KeyPath    string `yaml:"key_path,omitempty"`
	KnownHosts string `yaml:"known_hosts,omitempty"`
}

type PostgresConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type S3Config struct {
	Endpoint        string `yaml:"endpoint"`
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	Bucket          string `yaml:"bucket"`
	Prefix          string `yaml:"prefix"`
	Region          string `yaml:"region"`
}

type BackupConfig struct {
	TempDir        string `yaml:"temp_dir"`
	RetentionCount int    `yaml:"retention_count"`
	CompressionLvl int    `yaml:"compression_level"`
}

type TimeoutConfig struct {
	SSHConnection time.Duration `yaml:"ssh_connection"`
	BackupOp      time.Duration `yaml:"backup_operation"`
	Transfer      time.Duration `yaml:"transfer"`
	S3Upload      time.Duration `yaml:"s3_upload"`
}

type RestoreConfig struct {
	Enabled          bool       `yaml:"enabled"`
	UseSSH           *bool      `yaml:"use_ssh"`        // Optional: explicitly enable/disable SSH (nil = auto, true = use SSH, false = local)
	AutoInstall      bool       `yaml:"auto_install"`   // Auto-install PostgreSQL client if missing (local restore only)
	SSH              *SSHConfig `yaml:"ssh"`           // Optional SSH settings for restore target
	TargetHost       string     `yaml:"target_host"`
	TargetPort       int        `yaml:"target_port"`
	TargetDatabase   string     `yaml:"target_database"`
	TargetUsername   string     `yaml:"target_username"`
	TargetPassword   string     `yaml:"target_password"`
	DropExisting     bool       `yaml:"drop_existing"`
	ForceDisconnect  bool       `yaml:"force_disconnect"` // Force disconnect existing connections when dropping database
	CreateDB         bool       `yaml:"create_db"`
	Owner            string     `yaml:"owner"`
	Jobs             int        `yaml:"jobs"`
}

type NotificationConfig struct {
	Enabled     bool   `yaml:"enabled"`
	BinaryPath  string `yaml:"binary_path"`
	APIKey      string `yaml:"api_key"`
	From        string `yaml:"from"`
	To          string `yaml:"to"`
	ReplyTo     string `yaml:"reply_to"`
}

type LogConfig struct {
	FilePath       string `yaml:"file_path"`        // Path to log file (empty = stdout)
	MaxSize        int    `yaml:"max_size"`         // Max size in MB before rotation
	MaxBackups     int    `yaml:"max_backups"`      // Max number of old log files to keep
	MaxAge         int    `yaml:"max_age"`          // Max days to retain old log files
	Compress       bool   `yaml:"compress"`         // Whether to compress rotated files
	RotationTime   string `yaml:"rotation_time"`    // Time-based rotation: "hourly", "daily", "weekly", or duration like "24h"
	RotationMinute int    `yaml:"rotation_minute"`  // Minute to rotate (0-59, for hourly/daily/weekly rotation)
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	config := &Config{
		Timeouts: TimeoutConfig{
			SSHConnection: 30 * time.Second,
			BackupOp:      2 * time.Hour,
			Transfer:      1 * time.Hour,
			S3Upload:      2 * time.Hour,
		},
		Backup: BackupConfig{
			TempDir:        "/tmp",
			RetentionCount: 7,
			CompressionLvl: 6,
		},
		Restore: RestoreConfig{
			Enabled:      false,
			DropExisting: false,
			CreateDB:     false,
			Jobs:         1,
		},
		Notification: NotificationConfig{
			Enabled:    false,
			BinaryPath: "/usr/local/bin/go-notification",
		},
		Log: LogConfig{
			FilePath:       "", // Empty means stdout
			MaxSize:        100, // 100 MB
			MaxBackups:     3,
			MaxAge:         30, // 30 days
			Compress:       true,
			RotationTime:   "daily", // Default to daily rotation
			RotationMinute: 0, // Rotate at midnight by default
		},
	}

	if err := yaml.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return config, nil
}

func (c *Config) Validate() error {
	if c.SSH.Host == "" {
		return fmt.Errorf("SSH host is required")
	}
	if c.SSH.Port == 0 {
		c.SSH.Port = 22
	}
	if c.SSH.Username == "" {
		return fmt.Errorf("SSH username is required")
	}
	if c.SSH.Password == "" && c.SSH.KeyPath == "" {
		return fmt.Errorf("either SSH password or key path is required")
	}

	if c.Postgres.Host == "" {
		c.Postgres.Host = "localhost"
	}
	if c.Postgres.Port == 0 {
		c.Postgres.Port = 5432
	}
	if c.Postgres.Database == "" {
		return fmt.Errorf("PostgreSQL database is required")
	}
	if c.Postgres.Username == "" {
		return fmt.Errorf("PostgreSQL username is required")
	}

	if c.S3.Endpoint == "" {
		return fmt.Errorf("S3 endpoint is required")
	}
	if c.S3.AccessKeyID == "" {
		return fmt.Errorf("S3 access key ID is required")
	}
	if c.S3.SecretAccessKey == "" {
		return fmt.Errorf("S3 secret access key is required")
	}
	if c.S3.Bucket == "" {
		return fmt.Errorf("S3 bucket is required")
	}
	if c.S3.Region == "" {
		c.S3.Region = "us-east-1"
	}

	if c.Backup.RetentionCount <= 0 {
		c.Backup.RetentionCount = 7
	}
	if c.Backup.CompressionLvl < 0 || c.Backup.CompressionLvl > 9 {
		c.Backup.CompressionLvl = 6
	}

	// Validate restore config if enabled
	if c.Restore.Enabled {
		// Determine SSH usage
		useSSH := true // Default to using SSH
		if c.Restore.UseSSH != nil {
			useSSH = *c.Restore.UseSSH
		}
		
		if useSSH {
			// If SSH is enabled, validate SSH settings
			if c.Restore.SSH == nil {
				// Use backup SSH config as default
				c.Restore.SSH = &c.SSH
			} else {
				// Validate custom restore SSH settings
				if c.Restore.SSH.Host == "" {
					return fmt.Errorf("restore SSH host is required")
				}
				if c.Restore.SSH.Port == 0 {
					c.Restore.SSH.Port = 22
				}
				if c.Restore.SSH.Username == "" {
					return fmt.Errorf("restore SSH username is required")
				}
				if c.Restore.SSH.Password == "" && c.Restore.SSH.KeyPath == "" {
					return fmt.Errorf("either restore SSH password or key path is required")
				}
			}
		} else {
			// Local restore - SSH config should be nil
			c.Restore.SSH = nil
		}

		// Default to source database settings if not specified
		if c.Restore.TargetHost == "" {
			c.Restore.TargetHost = c.Postgres.Host
		}
		if c.Restore.TargetPort == 0 {
			c.Restore.TargetPort = c.Postgres.Port
		}
		if c.Restore.TargetDatabase == "" {
			c.Restore.TargetDatabase = c.Postgres.Database
		}
		if c.Restore.TargetUsername == "" {
			c.Restore.TargetUsername = c.Postgres.Username
		}
		if c.Restore.TargetPassword == "" {
			c.Restore.TargetPassword = c.Postgres.Password
		}
		if c.Restore.Jobs <= 0 {
			c.Restore.Jobs = 1
		}
		if c.Restore.Jobs > 8 {
			c.Restore.Jobs = 8
		}
	}

	// Validate notification config if enabled
	if c.Notification.Enabled {
		if c.Notification.BinaryPath == "" {
			c.Notification.BinaryPath = "/usr/local/bin/go-notification"
		}
		if c.Notification.APIKey == "" {
			return fmt.Errorf("notification API key is required when notifications are enabled")
		}
		if c.Notification.From == "" {
			return fmt.Errorf("notification from address is required when notifications are enabled")
		}
		if c.Notification.To == "" {
			return fmt.Errorf("notification to address is required when notifications are enabled")
		}
	}

	return nil
}