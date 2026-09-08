package socketalias

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
)

// baseDirectory is the parent directory for transient socket aliases.
//
// To keep the Unix socket path short enough and fit the platform limit,
// we're specifically requesting "/var/tmp" instead of "/var/folders/.../T/".
const baseDirectory = "/var/tmp"

// Create creates a short Unix socket alias in runtimeDir that points to socketPath.
//
// The alias remains valid until runtimeDir is removed.
func Create(runtimeDir string, socketPath string) (string, error) {
	// Make the symlink target absolute because a relative TART_HOME would
	// otherwise be resolved from runtimeDir, breaking the symlink
	absoluteSocketPath, err := filepath.Abs(socketPath)
	if err != nil {
		return "", err
	}

	// Place the VM's control socket alias in the runtime directory
	aliasSocketPath := filepath.Join(runtimeDir, "vm.sock")
	if err := os.Symlink(absoluteSocketPath, aliasSocketPath); err != nil {
		return "", err
	}

	return aliasSocketPath, nil
}

// DialContext connects to socketPath through a short transient alias.
func DialContext(ctx context.Context, socketPath string) (net.Conn, error) {
	// Create a directory where the transient Unix socket alias will live
	runtimeDir, err := os.MkdirTemp(baseDirectory, "orchard-socket-")
	if err != nil {
		return nil, err
	}

	// Route the socket through the short runtime directory
	aliasSocketPath, err := Create(runtimeDir, socketPath)
	if err != nil {
		return nil, errors.Join(err, os.RemoveAll(runtimeDir))
	}

	// Connect through the short alias
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", aliasSocketPath)

	// Remove the transient alias after the connection attempt
	cleanupErr := os.RemoveAll(runtimeDir)
	if err != nil {
		return nil, errors.Join(err, cleanupErr)
	}

	// Avoid returning a live connection when its alias cannot be removed
	if cleanupErr != nil {
		return nil, errors.Join(cleanupErr, conn.Close())
	}

	return conn, nil
}
