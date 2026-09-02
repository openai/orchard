package base

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/cirruslabs/orchard/internal/dialer"
	"github.com/cirruslabs/orchard/internal/worker/endpoint"
	"github.com/cirruslabs/orchard/pkg/client"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	mapset "github.com/deckarep/golang-set/v2"
	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
)

var ErrVMFailed = errors.New("VM failed")

type VM struct {
	// Backward compatibility with v1.VM specification's "Status" field
	//
	// "started" is always true after the first "tart run",
	// whereas ConditionReady can be used to tell if a VM
	// is really running or not.
	started atomic.Bool

	// A more orthogonal alternative to v1.VM specification's "Status" field,
	// which allows a VM to have more than one state.
	//
	// For example, a VM can be both in ConditionReady and ConditionSuspending/
	// ConditionStopping states for a short time. This way in run() we know
	// that we're in a process of rebooting a VM, so we can avoid throwing
	// an error about unexpected VM termination.
	conditions mapset.Set[v1.ConditionType]

	statusMessage atomic.Pointer[string]
	err           atomic.Pointer[error]
	endpoints     *endpoint.Set

	logger *zap.SugaredLogger
}

func NewVM(logger *zap.SugaredLogger) *VM {
	return &VM{
		conditions: mapset.NewSet(v1.ConditionTypeCloning),
		endpoints:  endpoint.NewSet(logger),
		logger:     logger,
	}
}

func (vm *VM) EndpointSet() *endpoint.Set {
	return vm.endpoints
}

func (vm *VM) SetStarted(val bool) {
	vm.started.Store(val)
}

func (vm *VM) Status() v1.VMStatus {
	if vm.Err() != nil {
		return v1.VMStatusFailed
	}

	if vm.started.Load() {
		return v1.VMStatusRunning
	}

	return v1.VMStatusPending
}

func (vm *VM) StatusMessage() string {
	status := vm.statusMessage.Load()

	if status != nil {
		return *status
	}

	return ""
}

func (vm *VM) SetStatusMessage(status string) {
	vm.logger.Debugf(status)
	vm.statusMessage.Store(&status)
}

func (vm *VM) Err() error {
	if err := vm.err.Load(); err != nil {
		return *err
	}

	return nil
}

func (vm *VM) SetErr(err error) {
	if vm.err.CompareAndSwap(nil, &err) {
		vm.logger.Error(err)
	}
}

func (vm *VM) ConditionsSet() mapset.Set[v1.ConditionType] {
	return vm.conditions
}

func (vm *VM) Conditions() []v1.Condition {
	// The worker must observe transitions before applying a new specification.
	return []v1.Condition{
		vm.conditionTypeToCondition(v1.ConditionTypeRunning),
		vm.conditionTypeToCondition(v1.ConditionTypeSuspending),
		vm.conditionTypeToCondition(v1.ConditionTypeStopping),
	}
}

func (vm *VM) conditionTypeToCondition(conditionType v1.ConditionType) v1.Condition {
	var conditionState v1.ConditionState

	if vm.ConditionsSet().ContainsOne(conditionType) {
		conditionState = v1.ConditionStateTrue
	} else {
		conditionState = v1.ConditionStateFalse
	}

	return v1.Condition{
		Type:  conditionType,
		State: conditionState,
	}
}

func (vm *VM) Shell(
	ctx context.Context,
	sshUser string,
	sshPassword string,
	script string,
	env map[string]string,
	consumeLine func(line string),
	dialer dialer.Dialer,
	getIP func(ctx context.Context) (string, error),
) error {
	var sess *ssh.Session

	// Set default user and password if not provided
	if sshUser == "" && sshPassword == "" {
		sshUser = "admin"
		sshPassword = "admin"
	}

	// Configure SSH client
	sshConfig := &ssh.ClientConfig{
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			return nil
		},
		User: sshUser,
		Auth: []ssh.AuthMethod{
			ssh.Password(sshPassword),
		},
	}

	if err := retry.Do(func() error {
		ip, err := getIP(ctx)
		if err != nil {
			return fmt.Errorf("failed to get VM's IP: %w", err)
		}

		addr := ip + ":22"

		dialCtx, dialCtxCancel := context.WithTimeout(ctx, 5*time.Second)
		defer dialCtxCancel()

		var netConn net.Conn

		if dialer != nil {
			netConn, err = dialer.DialContext(dialCtx, "tcp", addr)
		} else {
			dialer := net.Dialer{}

			netConn, err = dialer.DialContext(dialCtx, "tcp", addr)
		}
		if err != nil {
			return fmt.Errorf("failed to dial %s: %w", addr, err)
		}

		sshConn, chans, reqs, err := ssh.NewClientConn(netConn, addr, sshConfig)
		if err != nil {
			return fmt.Errorf("SSH handshake with %s failed: %w", addr, err)
		}

		sshClient := ssh.NewClient(sshConn, chans, reqs)

		sess, err = sshClient.NewSession()
		if err != nil {
			return fmt.Errorf("failed to open an SSH session on %s: %w", addr, err)
		}

		return nil
	}, retry.Context(ctx), retry.OnRetry(func(n uint, err error) {
		consumeLine(fmt.Sprintf("attempt %d to establish SSH connection failed: %v", n, err))
	})); err != nil {
		return fmt.Errorf("failed to establish SSH connection: %w", err)
	}

	// Log output from the virtual machine
	stdout, err := sess.StdoutPipe()
	if err != nil {
		return fmt.Errorf("%w: while opening stdout pipe: %v", ErrVMFailed, err)
	}
	stderr, err := sess.StderrPipe()
	if err != nil {
		return fmt.Errorf("%w: while opening stderr pipe: %v", ErrVMFailed, err)
	}
	var outputReaderWG sync.WaitGroup
	outputReaderWG.Add(1)
	go func() {
		output := io.MultiReader(stdout, stderr)

		scanner := bufio.NewScanner(output)

		for scanner.Scan() {
			consumeLine(scanner.Text())
		}
		outputReaderWG.Done()
	}()

	stdinBuf, err := sess.StdinPipe()
	if err != nil {
		return fmt.Errorf("%w: while opening stdin pipe: %v", ErrVMFailed, err)
	}

	// start a login shell so all the customization from ~/.zprofile will be picked up
	err = sess.Shell()
	if err != nil {
		return fmt.Errorf("%w: failed to start a shell: %v", ErrVMFailed, err)
	}

	var scriptBuilder strings.Builder

	scriptBuilder.WriteString("set -e\n")
	// don't use sess.Setenv since it requires non-default SSH server configuration
	for key, value := range env {
		scriptBuilder.WriteString("export " + key + "=\"" + value + "\"\n")
	}
	scriptBuilder.WriteString(script)
	scriptBuilder.WriteString("\nexit\n")

	_, err = stdinBuf.Write([]byte(scriptBuilder.String()))
	if err != nil {
		return fmt.Errorf("%w: failed to start script: %v", ErrVMFailed, err)
	}
	outputReaderWG.Wait()
	return sess.Wait()
}

func (vm *VM) RunScript(
	ctx context.Context,
	sshUser string,
	sshPassword string,
	script *v1.VMScript,
	eventStreamer *client.EventStreamer,
	dialer dialer.Dialer,
	getIP func(ctx context.Context) (string, error),
) {
	if eventStreamer != nil {
		defer func() {
			if err := eventStreamer.Close(); err != nil {
				vm.logger.Errorf("errored during streaming events for startup script: %v", err)
			}
		}()
	}

	consumeLine := func(line string) {
		if eventStreamer == nil {
			return
		}

		eventStreamer.Stream(v1.Event{
			Kind:      v1.EventKindLogLine,
			Timestamp: time.Now().Unix(),
			Payload:   line,
		})
	}

	err := vm.Shell(ctx, sshUser, sshPassword, script.ScriptContent, script.Env, consumeLine, dialer, getIP)
	if err != nil {
		vm.SetErr(fmt.Errorf("%w: failed to run startup script: %v", ErrVMFailed, err))
	}
}
