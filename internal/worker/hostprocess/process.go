//nolint:err113,mnd // Preserve the original host-process errors and retry policy.
package hostprocess

import (
	"context"
	"errors"
	"maps"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/avast/retry-go/v4"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
)

type Process struct {
	cancel     context.CancelFunc
	socketPath string
	done       chan struct{}
}

func NewProcess(
	spec v1.HostProcess,
	workerName string,
	vmName string,
	controlSocket string,
) (*Process, error) {
	// Create a directory where the host process will create its Unix socket
	//
	// To keep the Unix socket path short enough and fit the platform limit,
	// we're specifically requesting "/var/tmp" instead of "/var/folders/.../T/".
	//
	// And unlike "/tmp", "/var/tmp" is not subject to macOS's nightly cleanup.
	runtimeDir, err := os.MkdirTemp("/var/tmp", "orchard-hp-")
	if err != nil {
		return nil, err
	}

	// Use a fixed socket name within the process-specific runtime directory
	socketPath := filepath.Join(runtimeDir, "process.sock")

	// Route the Tart control socket through the runtime directory as well
	// to work around macOS's 104-byte Unix-domain socket limit
	controlSocket, err = shortenControlSocketPath(runtimeDir, controlSocket)
	if err != nil {
		return nil, errors.Join(err, os.RemoveAll(runtimeDir))
	}

	// Give the process an independently cancellable lifetime
	cmdCtx, cmdCtxCancel := context.WithCancel(context.Background())

	//nolint:gosec // Executing the configured host process is the purpose of this API
	command := exec.CommandContext(
		cmdCtx,
		spec.Program,
		expandArgs(spec.Args, workerName, vmName, controlSocket, socketPath)...,
	)
	command.Dir = runtimeDir

	// Ensure that we terminate the host process and any children it spawns
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		return syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	}

	// Craft the environment starting from the host process specification
	env := make(map[string]string)

	maps.Copy(env, spec.Env)

	// Preserve only PATH from the worker and make Orchard-managed variables authoritative
	env["PATH"] = os.Getenv("PATH")
	env["ORCHARD_WORKER_NAME"] = workerName
	env["ORCHARD_VM_NAME"] = vmName
	env["ORCHARD_VM_CONTROL_SOCKET"] = controlSocket
	env["ORCHARD_PROCESS_SOCKET"] = socketPath

	// Convert the environment to the format expected by exec.Cmd
	for name, value := range env {
		command.Env = append(command.Env, name+"="+value)
	}

	if err := command.Start(); err != nil {
		cmdCtxCancel()

		return nil, errors.Join(err, os.RemoveAll(runtimeDir))
	}

	process := &Process{
		cancel:     cmdCtxCancel,
		socketPath: socketPath,
		done:       make(chan struct{}),
	}

	go func() {
		defer close(process.done)

		// Reap the process; callers observe termination through done regardless of exit status
		_ = command.Wait()
		_ = command.Cancel()
		_ = os.RemoveAll(runtimeDir)
	}()

	return process, nil
}

func shortenControlSocketPath(runtimeDir string, controlSocketPath string) (string, error) {
	// Make the symlink target absolute because a relative TART_HOME would
	// otherwise be resolved from runtimeDir, breaking the symlink
	absoluteControlSocketPath, err := filepath.Abs(controlSocketPath)
	if err != nil {
		return "", err
	}

	// Place the VM's control socket alias in the runtime directory
	aliasControlSocketPath := filepath.Join(runtimeDir, "vm.sock")

	if err := os.Symlink(absoluteControlSocketPath, aliasControlSocketPath); err != nil {
		return "", err
	}

	return aliasControlSocketPath, nil
}

func (process *Process) Dial(ctx context.Context) (net.Conn, error) {
	var dialer net.Dialer

	return retry.DoWithData(func() (net.Conn, error) {
		select {
		case <-process.done:
			return nil, retry.Unrecoverable(errors.New("process exited before accepting connections"))
		default:
			// Continue
		}

		return dialer.DialContext(ctx, "unix", process.socketPath)
	},
		retry.Context(ctx),
		retry.Attempts(200),
		retry.Delay(50*time.Millisecond),
		retry.DelayType(retry.FixedDelay),
		retry.LastErrorOnly(true),
	)
}

func (process *Process) Close() {
	process.cancel()
	<-process.done
}

func expandArgs(args []string, workerName string, vmName string, controlSocket string, socketPath string) []string {
	replacer := strings.NewReplacer(
		"${ORCHARD_WORKER_NAME}", workerName,
		"${ORCHARD_VM_NAME}", vmName,
		"${ORCHARD_VM_CONTROL_SOCKET}", controlSocket,
		"${ORCHARD_PROCESS_SOCKET}", socketPath,
	)

	var expanded []string

	for _, arg := range args {
		expanded = append(expanded, replacer.Replace(arg))
	}

	return expanded
}
