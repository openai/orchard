//go:build unix

package localnetworkhelper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/puzpuzpuz/xsync/v4"
	"golang.org/x/sys/unix"
)

const (
	CommandName        = "localnetworkhelper"
	responseBufferSize = 4096
)

type socketRequest struct {
	Token   string `json:"token"`
	Network string `json:"network"`
	Addr    string `json:"addr"`
}

type socketResponse struct {
	Token string `json:"token"`
	Error string `json:"error"`
}

type dialRequest struct {
	network string
	addr    string
	replyCh chan dialReply
}

type dialReply struct {
	conn net.Conn
	err  error
}

var _ io.Closer = (*LocalNetworkHelper)(nil)

type LocalNetworkHelper struct {
	unixConn            *net.UnixConn
	requestsUnprocessed chan dialRequest
	requestsProcessed   *xsync.Map[string, dialRequest]
	err                 atomic.Pointer[error]
}

// New starts a privileged part of the macOS "Local Network" permission
// helper as a child process and enables communication with it.
func New(ctx context.Context) (*LocalNetworkHelper, error) {
	localNetworkHelper := &LocalNetworkHelper{
		requestsUnprocessed: make(chan dialRequest),
		requestsProcessed:   xsync.NewMap[string, dialRequest](),
	}

	// Create a socketpair(2) for communicating with the helper process
	//
	// We could've used datagram socket to simplify message boundary handling,
	// but that would make the connection close detection impossible.
	socketPair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		return nil, err
	}

	// Set FD_CLOEXEC to prevent file descriptor leakage to the child process
	//
	// Golang normally sets this flag when using higher-level OS functions,
	// but this is not the case with unix/syscall packages.
	//
	// See https://github.com/golang/go/issues/37857#issuecomment-599174378 for more details.
	if _, err := unix.FcntlInt(uintptr(socketPair[0]), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
		return nil, fmt.Errorf("failed to set FD_CLOEXEC on a first socketpair(2) socket: %v", err)
	}
	if _, err := unix.FcntlInt(uintptr(socketPair[1]), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
		return nil, fmt.Errorf("failed to set FD_CLOEXEC on a second socketpair(2) socket: %v", err)
	}

	// Launch our executable as a child process
	//
	// We're specifying the CommandName argument,
	// so that the child will jump to Serve()
	// and will wait for us.
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}

	// #nosec G204 -- The executable is Orchard itself, with a fixed helper subcommand.
	cmd := exec.CommandContext(ctx, executable, CommandName)

	extraFile := os.NewFile(uintptr(socketPair[1]), "")

	cmd.ExtraFiles = []*os.File{
		extraFile,
	}

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	cmd.WaitDelay = time.Second

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	// We can safely close the socketPair[1] now as it was inherited by the child
	if err := extraFile.Close(); err != nil {
		return nil, err
	}

	go func() {
		if err := cmd.Wait(); err != nil && !errors.Is(ctx.Err(), context.Canceled) {
			localNetworkHelper.err.CompareAndSwap(nil, &err)
		}
	}()

	// Convert our end of the socketpair(2) to a *unix.Conn
	file := os.NewFile(uintptr(socketPair[0]), "")

	conn, err := net.FileConn(file)
	if err != nil {
		return nil, err
	}

	// We can safely close the socketPair[0] now as it was dup(2)'ed by the net.FileConn
	if err := file.Close(); err != nil {
		return nil, err
	}

	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return nil, fmt.Errorf("expected *net.UnixConn, got %T", conn)
	}
	localNetworkHelper.unixConn = unixConn

	go localNetworkHelper.handleRequests()
	go localNetworkHelper.handleResponses()

	return localNetworkHelper, nil
}

func (localNetworkHelper *LocalNetworkHelper) PrivilegedDialContext(
	ctx context.Context,
	network string,
	addr string,
) (net.Conn, error) {
	// Check for global local network error first
	if err := localNetworkHelper.err.Load(); err != nil {
		return nil, *err
	}

	replyCh := make(chan dialReply, 1)

	localNetworkHelper.requestsUnprocessed <- dialRequest{
		network: network,
		addr:    addr,
		replyCh: replyCh,
	}

	select {
	case reply := <-replyCh:
		return reply.conn, reply.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (localNetworkHelper *LocalNetworkHelper) Close() error {
	close(localNetworkHelper.requestsUnprocessed)

	return localNetworkHelper.unixConn.Close()
}

func (localNetworkHelper *LocalNetworkHelper) handleRequests() {
	for request := range localNetworkHelper.requestsUnprocessed {
		privilegedSocketRequest := socketRequest{
			Token:   uuid.New().String(),
			Network: request.network,
			Addr:    request.addr,
		}

		privilegedSocketRequestJSONBytes, err := json.Marshal(&privilegedSocketRequest)
		if err != nil {
			request.replyCh <- dialReply{
				err: fmt.Errorf("failed to marshal privileged socket request: %v", err),
			}

			continue
		}

		// We need mark the request as processed before we send it,
		// otherwise the handleResponses() will trip up because it
		// won't be able to find this request
		localNetworkHelper.requestsProcessed.Store(privilegedSocketRequest.Token, request)

		_, err = localNetworkHelper.unixConn.Write(privilegedSocketRequestJSONBytes)
		if err != nil {
			// Unmark the request as processed because send failed
			localNetworkHelper.requestsProcessed.Delete(privilegedSocketRequest.Token)

			request.replyCh <- dialReply{
				err: fmt.Errorf("failed to send privileged socket request: %v", err),
			}
		}
	}
}

func (localNetworkHelper *LocalNetworkHelper) handleResponses() {
	buf := make([]byte, responseBufferSize)
	oob := make([]byte, responseBufferSize)

	for {
		n, oobn, _, _, err := localNetworkHelper.unixConn.ReadMsgUnix(buf, oob)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}

			err = fmt.Errorf("failed to read from the local network helper: %w", err)

			localNetworkHelper.err.CompareAndSwap(nil, &err)

			return
		}

		// Parse response(s)
		decoder := json.NewDecoder(bytes.NewReader(buf[:n]))

		for i := 0; ; i++ {
			var privilegedSocketResponse socketResponse

			if err := decoder.Decode(&privilegedSocketResponse); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				err = fmt.Errorf("failed to unmarshal privileged socket response: %w", err)

				localNetworkHelper.err.CompareAndSwap(nil, &err)

				return
			}

			processedRequest, ok := localNetworkHelper.requestsProcessed.LoadAndDelete(privilegedSocketResponse.Token)
			if !ok {
				err := fmt.Errorf("got response for a non-existent request %q", privilegedSocketResponse.Token)

				localNetworkHelper.err.CompareAndSwap(nil, &err)

				return
			}

			// As per POSIX.1-2017[1] (found in StackOverflow question[2]):
			//
			// >Ancillary data is received as if it were queued along with
			// >the first normal data octet in the segment (if any).
			//
			// This means that a single ReadMsgUnix() may yield multiple responses,
			// but oob will be only valid for the first response.
			//
			// [1]: https://pubs.opengroup.org/onlinepubs/9699919799/functions/V2_chap02.html
			// [2]: https://unix.stackexchange.com/questions/185011/what-happens-with-unix-stream-ancillary-data-on-partial-reads
			var currentOOB []byte

			if i == 0 {
				currentOOB = oob[:oobn]
			}

			netConn, err := handleResponse(privilegedSocketResponse, currentOOB)
			processedRequest.replyCh <- dialReply{
				conn: netConn,
				err:  err,
			}
		}
	}
}

func handleResponse(privilegedSocketResponse socketResponse, oob []byte) (net.Conn, error) {
	if privilegedSocketResponse.Error != "" {
		return nil, fmt.Errorf("%s", privilegedSocketResponse.Error)
	}

	socketControlMessages, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return nil, fmt.Errorf("failed to parse socket control message: %w", err)
	}

	if len(socketControlMessages) != 1 {
		return nil, fmt.Errorf("expected exactly one socket control message, got %d", len(socketControlMessages))
	}

	unixRights, err := unix.ParseUnixRights(&socketControlMessages[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse unix rights: %w", err)
	}

	if len(unixRights) != 1 {
		return nil, fmt.Errorf("expected exactly one unix right, got %d", len(unixRights))
	}

	netFile := os.NewFile(uintptr(unixRights[0]), "")

	netConn, err := net.FileConn(netFile)
	if err != nil {
		return nil, fmt.Errorf("failed to convert netFile to netConn: %w", err)
	}

	// We can safely close the netFile now as it was dup(2)'ed by the net.FileConn
	if err := netFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close netFile: %w", err)
	}

	return netConn, nil
}
