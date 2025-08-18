package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	SSH      SSHConfig      `yaml:"ssh"`
	Postgres PostgresConfig `yaml:"postgres"`
	S3       S3Config       `yaml:"s3"`
	Backup   BackupConfig   `yaml:"backup"`
	Timeouts TimeoutConfig  `yaml:"timeouts"`
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

	return nil
}