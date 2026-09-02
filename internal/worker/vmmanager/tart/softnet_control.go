//nolint:err113,perfsprint,staticcheck // preserve the original Softnet control implementation
package tart

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"golang.org/x/exp/jsonrpc2"
	"golang.org/x/sys/unix"
)

const (
	softnetControlFD      = 3
	softnetControlTimeout = 5 * time.Second
)

type softnetPolicyControl struct {
	conn *jsonrpc2.Connection
}

func (control *softnetPolicyControl) close() {
	_ = control.conn.Close()
}

type softnetPolicyTransport struct {
	net.Conn
}

func (transport *softnetPolicyTransport) Dial(context.Context) (io.ReadWriteCloser, error) {
	return transport, nil
}

func (transport *softnetPolicyTransport) Write(data []byte) (int, error) {
	n, err := transport.Conn.Write(data)
	if err != nil {
		return n, err
	}
	if n != len(data) {
		return n, io.ErrShortWrite
	}

	_, err = io.WriteString(transport.Conn, "\n")
	return n, err
}

type softnetPolicyParams struct {
	Allow []string `json:"allow"`
	Block []string `json:"block"`
}

type softnetPolicyResult struct {
	Allow []string `json:"allow"`
	Block []string `json:"block"`
}

func newSoftnetPolicyControl() (*softnetPolicyControl, *os.File, error) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		return nil, nil, err
	}

	unix.CloseOnExec(fds[0])
	unix.CloseOnExec(fds[1])

	workerFile := os.NewFile(uintptr(fds[0]), "orchard-softnet-control")
	tartFile := os.NewFile(uintptr(fds[1]), "tart-softnet-control")

	conn, err := net.FileConn(workerFile)
	_ = workerFile.Close()
	if err != nil {
		_ = tartFile.Close()
		return nil, nil, err
	}

	rpcConn, err := jsonrpc2.Dial(
		context.Background(),
		&softnetPolicyTransport{Conn: conn},
		jsonrpc2.ConnectionOptions{
			Framer: jsonrpc2.RawFramer(),
		},
	)
	if err != nil {
		_ = conn.Close()
		_ = tartFile.Close()
		return nil, nil, err
	}

	return &softnetPolicyControl{conn: rpcConn}, tartFile, nil
}

func (control *softnetPolicyControl) setPolicy(
	ctx context.Context,
	allow []string,
	block []string,
) error {
	if allow == nil {
		allow = []string{}
	}
	if block == nil {
		block = []string{}
	}

	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, softnetControlTimeout)
		defer cancel()
	}

	var result *softnetPolicyResult
	call := control.conn.Call(ctx, "softnet.policy.set", softnetPolicyParams{
		Allow: allow,
		Block: block,
	})
	if err := call.Await(ctx, &result); err != nil {
		return fmt.Errorf("failed to update Softnet policy: %w", err)
	}

	if result == nil {
		return fmt.Errorf("invalid Softnet policy response: missing result")
	}

	return nil
}

func (vm *VM) installSoftnetPolicyControl(control *softnetPolicyControl) {
	vm.softnetControlMtx.Lock()
	defer vm.softnetControlMtx.Unlock()

	if vm.softnetControl != nil {
		vm.softnetControl.close()
	}
	vm.softnetControl = control
}

func (vm *VM) removeSoftnetPolicyControl(control *softnetPolicyControl) {
	vm.softnetControlMtx.Lock()
	defer vm.softnetControlMtx.Unlock()

	if vm.softnetControl == control {
		control.close()
		vm.softnetControl = nil
	}
}

func (vm *VM) UpdateSoftnetPolicy(ctx context.Context, allow []string, block []string) error {
	vm.softnetControlMtx.Lock()
	defer vm.softnetControlMtx.Unlock()

	if vm.softnetControl == nil {
		return fmt.Errorf("Softnet policy control is unavailable")
	}

	return vm.softnetControl.setPolicy(ctx, allow, block)
}
