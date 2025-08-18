package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type RsyncClient struct {
	config *SSHConfig
	logger *slog.Logger
}

func NewRsyncClient(config *SSHConfig, logger *slog.Logger) *RsyncClient {
	return &RsyncClient{
		config: config,
		logger: logger,
	}
}

func (r *RsyncClient) DownloadFile(remotePath, localPath string, timeout time.Duration, progressFn func(int64, int64)) error {
	// Ensure local directory exists
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return fmt.Errorf("failed to create local directory: %w", err)
	}

	// Build rsync command
	sshCmd := r.buildSSHCommand()
	remoteSpec := fmt.Sprintf("%s@%s:%s", r.config.Username, r.config.Host, remotePath)
	
	args := []string{
		"-avz",          // archive, verbose, compress
		"--progress",    // show progress
		"--partial",     // keep partial files
		"-e", sshCmd,    // SSH command
		remoteSpec,
		localPath,
	}

	r.logger.Info("Starting rsync transfer",
		slog.String("remote", remotePath),
		slog.String("local", localPath))

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "rsync", args...)
	
	// Capture stderr for errors
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Capture stdout for progress
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start rsync: %w", err)
	}

	// Parse progress output
	progressRegex := regexp.MustCompile(`\s+(\d+)\s+(\d+)%`)
	scanner := bufio.NewScanner(stdout)
	
	go func() {
		var totalSize int64
		for scanner.Scan() {
			line := scanner.Text()
			r.logger.Debug("rsync output", slog.String("line", line))
			
			// Parse progress info
			if matches := progressRegex.FindStringSubmatch(line); len(matches) >= 3 {
				if transferred, err := strconv.ParseInt(matches[1], 10, 64); err == nil {
					if progressFn != nil && totalSize > 0 {
						progressFn(transferred, totalSize)
					}
				}
			}
			
			// Try to extract total size from initial output
			if strings.Contains(line, "total size") {
				parts := strings.Fields(line)
				for i, part := range parts {
					if part == "size" && i+2 < len(parts) {
						if size, err := strconv.ParseInt(strings.ReplaceAll(parts[i+2], ",", ""), 10, 64); err == nil {
							totalSize = size
						}
					}
				}
			}
		}
	}()

	// Collect stderr
	stderrScanner := bufio.NewScanner(stderr)
	var stderrLines []string
	go func() {
		for stderrScanner.Scan() {
			stderrLines = append(stderrLines, stderrScanner.Text())
		}
	}()

	// Wait for command to complete
	if err := cmd.Wait(); err != nil {
		stderrOutput := strings.Join(stderrLines, "\n")
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("rsync timed out after %v", timeout)
		}
		return fmt.Errorf("rsync failed: %w\nstderr: %s", err, stderrOutput)
	}

	// Verify file exists and has content
	stat, err := os.Stat(localPath)
	if err != nil {
		return fmt.Errorf("failed to verify downloaded file: %w", err)
	}

	if stat.Size() == 0 {
		os.Remove(localPath)
		return fmt.Errorf("downloaded file is empty")
	}

	r.logger.Info("Rsync transfer completed successfully",
		slog.String("local", localPath),
		slog.Int64("size", stat.Size()))

	return nil
}

func (r *RsyncClient) buildSSHCommand() string {
	sshArgs := []string{"ssh"}
	
	// Add port if not default
	if r.config.Port != 22 {
		sshArgs = append(sshArgs, "-p", fmt.Sprintf("%d", r.config.Port))
	}

	// Add known hosts file if specified
	if r.config.KnownHosts != "" {
		sshArgs = append(sshArgs, "-o", fmt.Sprintf("UserKnownHostsFile=%s", r.config.KnownHosts))
	} else {
		// Skip host key checking if no known hosts file
		sshArgs = append(sshArgs, "-o", "StrictHostKeyChecking=no")
	}

	// Add identity file if using key authentication
	if r.config.KeyPath != "" {
		sshArgs = append(sshArgs, "-i", r.config.KeyPath)
	}

	// Batch mode to prevent password prompts when using key auth
	if r.config.KeyPath != "" {
		sshArgs = append(sshArgs, "-o", "BatchMode=yes")
	}

	// If password auth, we need to use sshpass
	if r.config.Password != "" && r.config.KeyPath == "" {
		// Return sshpass command that wraps ssh
		return fmt.Sprintf("sshpass -p '%s' %s", r.config.Password, strings.Join(sshArgs, " "))
	}

	return strings.Join(sshArgs, " ")
}