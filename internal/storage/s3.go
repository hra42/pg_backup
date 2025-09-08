package storage

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/hra42/pg_backup/internal/config"
)

type S3Client struct {
	config     *config.S3Config
	client     *s3.Client
	uploader   *manager.Uploader
	downloader *manager.Downloader
	logger     *slog.Logger
}

func NewS3Client(s3Config *config.S3Config, logger *slog.Logger) (*S3Client, error) {
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if service == s3.ServiceID {
			return aws.Endpoint{
				URL:               s3Config.Endpoint,
				SigningRegion:     s3Config.Region,
				HostnameImmutable: true,
			}, nil
		}
		return aws.Endpoint{}, fmt.Errorf("unknown endpoint requested")
	})

	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(s3Config.Region),
		awsconfig.WithEndpointResolverWithOptions(customResolver),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			s3Config.AccessKeyID,
			s3Config.SecretAccessKey,
			"",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load S3 config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = 100 * 1024 * 1024
		u.Concurrency = 3
	})

	downloader := manager.NewDownloader(client, func(d *manager.Downloader) {
		d.PartSize = 100 * 1024 * 1024
		d.Concurrency = 3
	})

	return &S3Client{
		config:     s3Config,
		client:     client,
		uploader:   uploader,
		downloader: downloader,
		logger:     logger,
	}, nil
}

func (s *S3Client) ValidateBucket(ctx context.Context) error {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: &s.config.Bucket,
	})
	if err != nil {
		return fmt.Errorf("S3 bucket validation failed: %w", err)
	}
	return nil
}

func (s *S3Client) UploadFile(ctx context.Context, localPath string, progressFn func(int64)) error {
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open file for upload: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	key := s.generateBackupKey(filepath.Base(localPath))
	s.logger.Info("Starting S3 upload",
		slog.String("file", localPath),
		slog.String("bucket", s.config.Bucket),
		slog.String("key", key),
		slog.Int64("size", stat.Size()))

	progressReader := &progressReader{
		reader:     file,
		size:       stat.Size(),
		progressFn: progressFn,
		logger:     s.logger,
	}

	uploadInput := &s3.PutObjectInput{
		Bucket:      aws.String(s.config.Bucket),
		Key:         aws.String(key),
		Body:        progressReader,
		ContentType: aws.String("application/x-tar"),
		Metadata: map[string]string{
			"backup-time": time.Now().UTC().Format(time.RFC3339),
			"backup-size": fmt.Sprintf("%d", stat.Size()),
		},
	}

	result, err := s.uploader.Upload(ctx, uploadInput)
	if err != nil {
		return fmt.Errorf("S3 upload failed: %w", err)
	}

	headOutput, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to verify uploaded object: %w", err)
	}

	if headOutput.ContentLength == nil || *headOutput.ContentLength != stat.Size() {
		return fmt.Errorf("uploaded file size mismatch")
	}

	s.logger.Info("S3 upload completed successfully",
		slog.String("location", result.Location),
		slog.String("etag", *result.ETag),
		slog.Int64("size", stat.Size()))

	return nil
}

func (s *S3Client) CleanupOldBackups(ctx context.Context, retentionCount int) error {
	s.logger.Info("Starting backup cleanup",
		slog.Int("retention_count", retentionCount))

	prefix := s.config.Prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	// List all backup objects
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.config.Bucket),
		Prefix: aws.String(prefix),
	})

	type backupInfo struct {
		Key          *string
		LastModified *time.Time
	}
	var allBackups []backupInfo

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			s.logger.Error("Failed to list objects", slog.String("error", err.Error()))
			return fmt.Errorf("failed to list backups: %w", err)
		}

		for _, obj := range page.Contents {
			// Only include files that match our backup pattern
			if obj.Key != nil && strings.HasPrefix(filepath.Base(*obj.Key), "backup-") && strings.HasSuffix(*obj.Key, ".dump") {
				allBackups = append(allBackups, backupInfo{
					Key:          obj.Key,
					LastModified: obj.LastModified,
				})
			}
		}
	}

	// Sort by modification time (newest first)
	for i := 0; i < len(allBackups)-1; i++ {
		for j := i + 1; j < len(allBackups); j++ {
			if allBackups[i].LastModified != nil && allBackups[j].LastModified != nil {
				if allBackups[i].LastModified.Before(*allBackups[j].LastModified) {
					allBackups[i], allBackups[j] = allBackups[j], allBackups[i]
				}
			}
		}
	}

	s.logger.Info("Found backups", slog.Int("total", len(allBackups)))

	// Keep only the most recent backups
	if len(allBackups) <= retentionCount {
		s.logger.Info("No backups to delete", 
			slog.Int("current_count", len(allBackups)),
			slog.Int("retention_count", retentionCount))
		return nil
	}

	// Delete older backups
	var objectsToDelete []types.ObjectIdentifier
	for i := retentionCount; i < len(allBackups); i++ {
		objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
			Key: allBackups[i].Key,
		})
		s.logger.Debug("Marking for deletion",
			slog.String("key", *allBackups[i].Key),
			slog.Time("modified", *allBackups[i].LastModified))
	}

	if len(objectsToDelete) > 0 {
		deleteInput := &s3.DeleteObjectsInput{
			Bucket: aws.String(s.config.Bucket),
			Delete: &types.Delete{
				Objects: objectsToDelete,
				Quiet:   aws.Bool(false),
			},
		}

		deleteOutput, err := s.client.DeleteObjects(ctx, deleteInput)
		if err != nil {
			return fmt.Errorf("failed to delete old backups: %w", err)
		}

		for _, deleted := range deleteOutput.Deleted {
			s.logger.Info("Deleted old backup", slog.String("key", *deleted.Key))
		}
		
		var errors []error
		for _, failed := range deleteOutput.Errors {
			s.logger.Error("Failed to delete object",
				slog.String("key", *failed.Key),
				slog.String("error", *failed.Message))
			errors = append(errors, fmt.Errorf("delete failed for %s: %s", *failed.Key, *failed.Message))
		}
		
		if len(errors) > 0 {
			return fmt.Errorf("cleanup completed with %d errors", len(errors))
		}
	}

	s.logger.Info("Cleanup completed",
		slog.Int("deleted_count", len(objectsToDelete)),
		slog.Int("kept_count", retentionCount))

	return nil
}

func (s *S3Client) generateBackupKey(filename string) string {
	timestamp := time.Now().UTC().Format("20060102-150405")
	prefix := s.config.Prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return fmt.Sprintf("%sbackup-%s-%s", prefix, timestamp, filename)
}

type progressReader struct {
	reader     *os.File
	size       int64
	read       int64
	progressFn func(int64)
	lastReport time.Time
	logger     *slog.Logger
}

func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.reader.Read(p)
	if n > 0 {
		pr.read += int64(n)
		if pr.progressFn != nil && time.Since(pr.lastReport) > time.Second {
			pr.progressFn(pr.read)
			percentage := float64(pr.read) / float64(pr.size) * 100
			pr.logger.Info("Upload progress",
				slog.Float64("percentage", percentage),
				slog.Int64("bytes", pr.read),
				slog.Int64("total", pr.size))
			pr.lastReport = time.Now()
		}
	}
	return n, err
}

func (pr *progressReader) Seek(offset int64, whence int) (int64, error) {
	return pr.reader.Seek(offset, whence)
}

func (s *S3Client) DownloadFile(ctx context.Context, key string, localPath string, progressFn func(int64)) error {
	s.logger.Info("Starting S3 download",
		slog.String("bucket", s.config.Bucket),
		slog.String("key", key),
		slog.String("local_path", localPath))

	// Create the local file
	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer file.Close()

	// Get object size for progress tracking
	headOutput, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to get object metadata: %w", err)
	}

	totalSize := *headOutput.ContentLength
	s.logger.Info("Object size", slog.Int64("bytes", totalSize))

	// Download the file with progress tracking
	numBytes, err := s.downloader.Download(ctx, file, &s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
	}, func(d *manager.Downloader) {
		d.PartSize = 100 * 1024 * 1024
		d.Concurrency = 3
	})

	if err != nil {
		return fmt.Errorf("S3 download failed: %w", err)
	}

	// Call progress function with final size
	if progressFn != nil {
		progressFn(numBytes)
	}

	s.logger.Info("S3 download completed successfully",
		slog.String("path", localPath),
		slog.Int64("size", numBytes))

	return nil
}

func (s *S3Client) GetLatestBackup(ctx context.Context) (string, error) {
	s.logger.Info("Getting latest backup from S3")

	prefix := s.config.Prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	// List all backup objects
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.config.Bucket),
		Prefix: aws.String(prefix),
	})

	var latestBackup *types.Object
	var latestTime time.Time

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to list backups: %w", err)
		}

		for _, obj := range page.Contents {
			// Only include backup files
			if obj.Key != nil && strings.Contains(*obj.Key, "backup_") && strings.HasSuffix(*obj.Key, ".dump") {
				if obj.LastModified != nil && obj.LastModified.After(latestTime) {
					latestTime = *obj.LastModified
					latestBackup = &obj
				}
			}
		}
	}

	if latestBackup == nil {
		return "", fmt.Errorf("no backups found in S3")
	}

	s.logger.Info("Found latest backup",
		slog.String("key", *latestBackup.Key),
		slog.Time("modified", *latestBackup.LastModified))

	return *latestBackup.Key, nil
}

func (s *S3Client) ListBackups(ctx context.Context) ([]string, error) {
	s.logger.Info("Listing all backups from S3")

	prefix := s.config.Prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	// List all backup objects
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.config.Bucket),
		Prefix: aws.String(prefix),
	})

	type backupInfo struct {
		Key          string
		LastModified time.Time
	}
	var backups []backupInfo

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list backups: %w", err)
		}

		for _, obj := range page.Contents {
			// Only include backup files
			if obj.Key != nil && strings.Contains(*obj.Key, "backup_") && strings.HasSuffix(*obj.Key, ".dump") {
				backups = append(backups, backupInfo{
					Key:          *obj.Key,
					LastModified: *obj.LastModified,
				})
			}
		}
	}

	// Sort by modification time (newest first)
	for i := 0; i < len(backups)-1; i++ {
		for j := i + 1; j < len(backups); j++ {
			if backups[i].LastModified.Before(backups[j].LastModified) {
				backups[i], backups[j] = backups[j], backups[i]
			}
		}
	}

	// Convert to string slice
	result := make([]string, len(backups))
	for i, backup := range backups {
		result[i] = backup.Key
	}

	return result, nil
}

