//go:build unix

package localnetworkhelper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"
)

type helper struct {
	err atomic.Pointer[error]
}

// Serve implements a privileged component of the macOS "Local Network" permission helper.
//
// It listens for net.Dial requests, performs the dialing and sends the results back.
func Serve(fd int) error {
	helper := &helper{}

	// Convert our end of the socketpair(2) to a *unix.Conn
	file := os.NewFile(uintptr(fd), "")

	conn, err := net.FileConn(file)
	if err != nil {
		return err
	}

	// We can safely close the fd now as it was dup(2)'ed by the net.FileConn
	if err := file.Close(); err != nil {
		return err
	}

	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return fmt.Errorf("expected *net.UnixConn, got %T", conn)
	}

	// A request can span multiple reads from the stream socket.
	decoder := json.NewDecoder(unixConn)

	for {
		// Check for global local network helper error first
		if err := helper.err.Load(); err != nil {
			return *err
		}

		var request socketRequest
		if err := decoder.Decode(&request); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return fmt.Errorf("failed to unmarshal privileged socket request: %w", err)
		}

		// Handle request concurrently
		go func() {
			if err := helper.handleRequest(unixConn, request); err != nil {
				helper.err.CompareAndSwap(nil, &err)
			}
		}()
	}
}

func (helper *helper) handleRequest(unixConn *net.UnixConn, privilegedSocketRequest socketRequest) error {
	privilegedSocketResponse := socketResponse{
		Token: privilegedSocketRequest.Token,
	}

	var oob []byte

	netConn, err := (&net.Dialer{}).DialContext(
		context.Background(),
		privilegedSocketRequest.Network,
		privilegedSocketRequest.Addr,
	)
	if err != nil {
		privilegedSocketResponse.Error = err.Error()
	}

	if netConn != nil {
		defer netConn.Close()

		var syscallConn syscall.RawConn

		switch typedNetConn := netConn.(type) {
		case *net.TCPConn:
			syscallConn, err = typedNetConn.SyscallConn()
		case *net.UDPConn:
			syscallConn, err = typedNetConn.SyscallConn()
		default:
			err = fmt.Errorf("unsupported net.Conn type: %T", netConn)
		}
		if err != nil {
			privilegedSocketResponse.Error = err.Error()
		}

		if syscallConn != nil {
			if err := syscallConn.Control(func(fd uintptr) {
				oob = unix.UnixRights(int(fd))
			}); err != nil {
				privilegedSocketResponse.Error = err.Error()
			}
		}
	}

	privilegedSocketResponseJSONBytes, err := json.Marshal(privilegedSocketResponse)
	if err != nil {
		return fmt.Errorf("failed to marshal privileged socket response: %w", err)
	}

	_, _, err = unixConn.WriteMsgUnix(privilegedSocketResponseJSONBytes, oob, nil)
	if err != nil {
		return fmt.Errorf("failed to write to unix socket: %w", err)
	}

	return nil
}
