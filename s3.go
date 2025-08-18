package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3Client struct {
	config   *S3Config
	client   *s3.Client
	uploader *manager.Uploader
	logger   *slog.Logger
}

func NewS3Client(s3Config *S3Config, logger *slog.Logger) (*S3Client, error) {
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

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(s3Config.Region),
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
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

	return &S3Client{
		config:   s3Config,
		client:   client,
		uploader: uploader,
		logger:   logger,
	}, nil
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

func (s *S3Client) CleanupOldBackups(ctx context.Context, retentionDays int) error {
	cutoffTime := time.Now().AddDate(0, 0, -retentionDays)

	s.logger.Info("Starting backup cleanup",
		slog.Int("retention_days", retentionDays),
		slog.Time("cutoff", cutoffTime))

	prefix := s.config.Prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.config.Bucket),
		Prefix: aws.String(prefix),
	})

	var deletedCount int
	var errors []error

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			s.logger.Error("Failed to list objects", slog.String("error", err.Error()))
			errors = append(errors, err)
			continue
		}

		var objectsToDelete []types.ObjectIdentifier

		for _, obj := range page.Contents {
			if obj.LastModified != nil && obj.LastModified.Before(cutoffTime) {
				objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
					Key: obj.Key,
				})
				s.logger.Debug("Marking for deletion",
					slog.String("key", *obj.Key),
					slog.Time("modified", *obj.LastModified))
			}
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
				s.logger.Error("Failed to delete objects", slog.String("error", err.Error()))
				errors = append(errors, err)
			} else {
				deletedCount += len(deleteOutput.Deleted)
				for _, deleted := range deleteOutput.Deleted {
					s.logger.Info("Deleted old backup", slog.String("key", *deleted.Key))
				}
				for _, failed := range deleteOutput.Errors {
					s.logger.Error("Failed to delete object",
						slog.String("key", *failed.Key),
						slog.String("error", *failed.Message))
					errors = append(errors, fmt.Errorf("delete failed for %s: %s", *failed.Key, *failed.Message))
				}
			}
		}
	}

	s.logger.Info("Cleanup completed",
		slog.Int("deleted_count", deletedCount),
		slog.Int("error_count", len(errors)))

	if len(errors) > 0 {
		return fmt.Errorf("cleanup completed with %d errors", len(errors))
	}

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