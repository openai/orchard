package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/avast/retry-go/v5"
	"github.com/cirruslabs/orchard/internal/controller/sshexec"
	"github.com/cirruslabs/orchard/internal/execstream"
	"github.com/cirruslabs/orchard/internal/responder"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
)

func (controller *Controller) execVM(ctx *gin.Context) responder.Responder {
	if responder := controller.authorizeAny(ctx, v1.ServiceAccountRoleComputeWrite,
		v1.ServiceAccountRoleComputeConnect); responder != nil {
		return responder
	}

	// Retrieve and parse path and query parameters
	name := ctx.Param("name")
	command := ctx.Query("command")
	if command == "" {
		return responder.JSON(http.StatusBadRequest,
			NewErrorResponse("\"command\" parameter cannot be empty"))
	}

	options, runCommand, err := parseExecOptions(ctx, command)
	if err != nil {
		return responder.JSON(http.StatusBadRequest, NewErrorResponse("%v", err))
	}

	waitRaw := ctx.DefaultQuery("wait", "10")
	wait, err := strconv.ParseUint(waitRaw, 10, 16)
	if err != nil {
		return responder.Code(http.StatusBadRequest)
	}

	// Look-up the VM
	waitContext, waitContextCancel := context.WithTimeout(ctx, time.Duration(wait)*time.Second)
	defer waitContextCancel()

	vm, responderImpl := controller.waitForVM(waitContext, name)
	if responderImpl != nil {
		return responderImpl
	}

	exec, err := controller.newSSHExec(waitContext, vm, options)
	if err != nil {
		return responder.JSON(http.StatusServiceUnavailable, NewErrorResponse("%v", err))
	}

	// Upgrade HTTP request to a WebSocket connection
	wsConn, err := websocket.Accept(ctx.Writer, ctx.Request, &websocket.AcceptOptions{
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		_ = exec.Close()

		return responder.Error(err)
	}
	defer func() {
		// Ensure that we always close the accepted WebSocket connection,
		// otherwise resource leak is possible[1]
		//
		// [1]: https://github.com/coder/websocket/issues/445#issuecomment-2053792044
		_ = wsConn.CloseNow()
	}()

	return controller.serveExec(ctx, wsConn, exec, runCommand)
}

func (controller *Controller) newSSHExec(
	waitContext context.Context,
	vm *v1.VM,
	options sshexec.Options,
) (*sshexec.Exec, error) {
	return retry.NewWithData[*sshexec.Exec](
		retry.Context(waitContext),
		retry.DelayType(retry.FixedDelay),
		retry.Delay(time.Second),
		retry.Attempts(0),
		retry.LastErrorOnly(true),
	).Do(func() (*sshexec.Exec, error) {
		exec, err := controller.execSSHClients.newExec(vm.UID, options, func() (sshExecClient, error) {
			portForwardConn, err := controller.portForwardConnection(
				context.Background(),
				waitContext,
				vm.Worker,
				vm.UID,
				22,
				"",
				"",
			)
			if err != nil {
				return nil, err
			}

			client, err := sshexec.NewClient(portForwardConn, vm.SSHUsername(), vm.SSHPassword())
			if err != nil {
				_ = portForwardConn.Close()

				return nil, fmt.Errorf("failed to establish SSH connection to a VM: %w", err)
			}

			return client, nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to establish SSH connection to a VM: %w", err)
		}

		return exec, nil
	})
}

func (controller *Controller) serveExec(
	ctx *gin.Context,
	wsConn *websocket.Conn,
	exec *sshexec.Exec,
	command string,
) *responder.EmptyResponder {
	execContext, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		_ = exec.Close()
	}()

	// A bounded channel applies backpressure without dropping output. The SSH
	// runner drains stdout/stderr before sending exit, then we drain this channel.
	outgoingFrames := make(chan *execstream.Frame, 128)
	go func() {
		defer close(outgoingFrames)

		if err := exec.Run(execContext, command, outgoingFrames); err != nil && !errors.Is(err, context.Canceled) {
			select {
			case outgoingFrames <- &execstream.Frame{Type: execstream.FrameTypeError, Error: err.Error()}:
			case <-execContext.Done():
			}
		}
	}()

	readFramesErrCh := make(chan error, 1)
	go func() {
		readFramesErrCh <- readExecFrames(ctx, wsConn, exec)
	}()

	for {
		select {
		case readFramesErr := <-readFramesErrCh:
			if readFramesErr != nil {
				controller.logger.Warnf("failed to read and process exec frames from WebSocket: %v",
					readFramesErr)
			}

			return responder.Empty()
		case outgoingFrame, ok := <-outgoingFrames:
			if !ok {
				if err := wsConn.Close(websocket.StatusNormalClosure, "Command finished"); err != nil {
					controller.logger.Warnf("exec: failed to close WebSocket cleanly: %v", err)
				}

				return responder.Empty()
			}

			if err := execstream.WriteFrame(ctx, wsConn, outgoingFrame); err != nil {
				controller.logger.Warnf("failed to write exec frame to the client: %v", err)

				return responder.Empty()
			}
		case <-time.After(controller.pingInterval):
			pingCtx, pingCtxCancel := context.WithTimeout(ctx, 5*time.Second)

			if err := wsConn.Ping(pingCtx); err != nil {
				controller.logger.Warnf("exec: failed to ping the client, "+
					"connection might time out: %v", err)
			}

			pingCtxCancel()
		case <-ctx.Done():
			controller.logger.Warnf("client disconnected prematurely")

			return responder.Empty()
		}
	}
}

func parseExecOptions(ctx *gin.Context, command string) (sshexec.Options, string, error) {
	interactive, err := parseExecInteractive(ctx)
	if err != nil {
		return sshexec.Options{}, "", err
	}

	tty, err := parseExecBool(ctx, "tty")
	if err != nil {
		return sshexec.Options{}, "", err
	}
	if tty {
		interactive = true
	}

	rows, err := parseExecUint32(ctx.Query("rows"), "rows")
	if err != nil {
		return sshexec.Options{}, "", err
	}
	cols, err := parseExecUint32(ctx.Query("cols"), "cols")
	if err != nil {
		return sshexec.Options{}, "", err
	}
	if (rows == 0) != (cols == 0) {
		return sshexec.Options{}, "", errors.New("\"rows\" and \"cols\" must be provided together")
	}

	options := sshexec.Options{
		Interactive: interactive,
		TTY:         tty,
		Rows:        rows,
		Cols:        cols,
		Env:         ctx.QueryMap("env"),
		Workdir:     ctx.Query("workdir"),
	}

	runCommand, err := sshexec.CommandWithOptions(command, options)
	if err != nil {
		return sshexec.Options{}, "", err
	}

	return options, runCommand, nil
}

func parseExecInteractive(ctx *gin.Context) (bool, error) {
	interactive, err := parseExecBool(ctx, "interactive")
	if err != nil {
		return false, err
	}

	interactiveRaw, interactivePresent := ctx.GetQuery("interactive")
	stdinRaw, stdinPresent := ctx.GetQuery("stdin")
	if !stdinPresent {
		return interactive, nil
	}

	stdin, err := strconv.ParseBool(stdinRaw)
	if err != nil {
		return false, errors.New("\"stdin\" parameter must be a boolean")
	}

	if interactivePresent {
		parsedInteractive, _ := strconv.ParseBool(interactiveRaw)
		if stdin != parsedInteractive {
			return false, errors.New("\"interactive\" and \"stdin\" parameters cannot conflict")
		}
	}

	if !interactivePresent {
		interactive = stdin
	}

	return interactive, nil
}

func parseExecBool(ctx *gin.Context, name string) (bool, error) {
	raw, present := ctx.GetQuery(name)
	if !present {
		return false, nil
	}

	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%q parameter must be a boolean", name)
	}

	return value, nil
}

func parseExecUint32(raw string, name string) (uint32, error) {
	if raw == "" {
		return 0, nil
	}

	value, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%q parameter must be an unsigned integer", name)
	}

	return uint32(value), nil
}

func readExecFrames(ctx context.Context, wsConn *websocket.Conn, exec *sshexec.Exec) error {
	stdin := exec.Stdin()
	stdinClosed := false

	for {
		var frame execstream.Frame

		messageType, payloadBytes, err := wsConn.Read(ctx)
		if err != nil {
			var closeErr websocket.CloseError
			if errors.As(err, &closeErr) && closeErr.Code == websocket.StatusNormalClosure {
				return nil
			}

			return fmt.Errorf("failed to read next frame from WebSocket: %w", err)
		}

		if messageType != websocket.MessageText {
			continue
		}

		if err := json.Unmarshal(payloadBytes, &frame); err != nil {
			return err
		}

		switch frame.Type {
		case execstream.FrameTypeStdin:
			if stdin == nil || stdinClosed {
				return fmt.Errorf("failed to handle %q frame: this exec session has no stdin enabled or it is already closed",
					frame.Type)
			}

			if len(frame.Data) == 0 {
				err = stdin.Close()
				stdinClosed = true
			} else {
				_, err = stdin.Write(frame.Data)
			}
			if err != nil {
				return fmt.Errorf("failed to handle %q frame: %w", frame.Type, err)
			}
		case execstream.FrameTypeResize:
			if frame.Terminal == nil {
				return fmt.Errorf("failed to handle %q frame: terminal size is required", frame.Type)
			}

			if err := exec.Resize(frame.Terminal.Rows, frame.Terminal.Cols); err != nil {
				return fmt.Errorf("failed to handle %q frame: %w", frame.Type, err)
			}
		default:
			return fmt.Errorf("unexpected frame type received: %q", frame.Type)
		}
	}
}
