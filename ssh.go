package main

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

type SSHClient struct {
	config *SSHConfig
	client *ssh.Client
	logger *slog.Logger
}

func NewSSHClient(config *SSHConfig, logger *slog.Logger) (*SSHClient, error) {
	return &SSHClient{
		config: config,
		logger: logger,
	}, nil
}

func (s *SSHClient) Connect(timeout time.Duration) error {
	s.logger.Info("Establishing SSH connection",
		slog.String("host", s.config.Host),
		slog.Int("port", s.config.Port))

	sshConfig := &ssh.ClientConfig{
		User:            s.config.Username,
		Timeout:         timeout,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	if s.config.KnownHosts != "" {
		hostKeyCallback, err := knownhosts.New(s.config.KnownHosts)
		if err != nil {
			return fmt.Errorf("failed to parse known_hosts: %w", err)
		}
		sshConfig.HostKeyCallback = hostKeyCallback
	}

	if s.config.Password != "" {
		sshConfig.Auth = []ssh.AuthMethod{
			ssh.Password(s.config.Password),
		}
	} else if s.config.KeyPath != "" {
		key, err := os.ReadFile(s.config.KeyPath)
		if err != nil {
			return fmt.Errorf("failed to read SSH key: %w", err)
		}

		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return fmt.Errorf("failed to parse SSH key: %w", err)
		}

		sshConfig.Auth = []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		}
	}

	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	client, err := ssh.Dial("tcp", addr, sshConfig)
	if err != nil {
		return fmt.Errorf("SSH connection failed: %w", err)
	}

	s.client = client
	s.logger.Info("SSH connection established successfully")
	return nil
}

func (s *SSHClient) ExecuteCommand(cmd string, timeout time.Duration) (string, error) {
	if s.client == nil {
		return "", fmt.Errorf("SSH client not connected")
	}

	session, err := s.client.NewSession()
	if err != nil {
		return "", fmt.Errorf("failed to create SSH session: %w", err)
	}
	defer session.Close()

	var stdout, stderr bytes.Buffer
	session.Stdout = &stdout
	session.Stderr = &stderr

	done := make(chan error, 1)
	go func() {
		done <- session.Run(cmd)
	}()

	select {
	case err := <-done:
		if err != nil {
			stderrStr := stderr.String()
			if stderrStr != "" {
				return "", fmt.Errorf("command failed: %w\nstderr: %s", err, stderrStr)
			}
			return "", fmt.Errorf("command failed: %w", err)
		}
		return stdout.String(), nil
	case <-time.After(timeout):
		session.Signal(ssh.SIGTERM)
		time.Sleep(5 * time.Second)
		session.Signal(ssh.SIGKILL)
		return "", fmt.Errorf("command timed out after %v", timeout)
	}
}

func (s *SSHClient) RemoveRemoteFile(remotePath string) error {
	// Use SSH command to remove the file
	_, err := s.ExecuteCommand(fmt.Sprintf("rm -f %s", remotePath), 10*time.Second)
	if err != nil {
		return fmt.Errorf("failed to remove remote file: %w", err)
	}

	s.logger.Info("Remote file deleted", slog.String("path", remotePath))
	return nil
}

func (s *SSHClient) Close() {
	if s.client != nil {
		s.client.Close()
		s.client = nil
	}
	s.logger.Info("SSH connection closed")
}