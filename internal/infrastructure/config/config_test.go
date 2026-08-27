package config

import (
	"path/filepath"
	"testing"
)

func TestUnixSocketValidation(t *testing.T) {
	// Test placeholder path fail
	err1 := validateUnixSocketPath("/path/to/dnstap.sock", "relay.input.address")
	if err1 == nil {
		t.Errorf("expected error for placeholder path '/path/to/dnstap.sock'")
	}

	// Test non-existent parent directory fail
	err2 := validateUnixSocketPath("/nonexistent_dir_xyz123/dnstap.sock", "dnstap.unix_socket")
	if err2 == nil {
		t.Errorf("expected error for non-existent parent directory")
	}

	// Test valid existing parent directory pass
	tempDir := t.TempDir()
	validSockPath := filepath.Join(tempDir, "dnstap.sock")
	err3 := validateUnixSocketPath(validSockPath, "dnstap.unix_socket")
	if err3 != nil {
		t.Errorf("expected no error for valid directory, got: %v", err3)
	}
}
