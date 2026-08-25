package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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
	sessionID := ctx.Query("session")
	if sessionID == "" {
		sessionID = ctx.Query("cmux_session_id")
	}

	command := ctx.Query("command")
	if sessionID == "" && command == "" {
		return responder.JSON(http.StatusBadRequest,
			NewErrorResponse("\"command\" parameter cannot be empty"))
	}

	spec, runCommand, err := parseExecSessionSpec(ctx, command)
	if err != nil {
		return responder.JSON(http.StatusBadRequest, NewErrorResponse("%v", err))
	}

	waitRaw := ctx.DefaultQuery("wait", "10")
	wait, err := strconv.ParseUint(waitRaw, 10, 16)
	if err != nil {
		return responder.Code(http.StatusBadRequest)
	}

	if sessionID != "" {
		return controller.execVMReconnectable(ctx, name, sessionID, spec, runCommand, wait)
	}

	return controller.execVMLegacy(ctx, name, spec, runCommand, wait)
}

func (controller *Controller) execVMLegacy(
	ctx *gin.Context,
	name string,
	spec execSessionSpec,
	runCommand string,
	wait uint64,
) responder.Responder {
	// Look-up the VM
	waitContext, waitContextCancel := context.WithTimeout(ctx, time.Duration(wait)*time.Second)
	defer waitContextCancel()

	vm, responderImpl := controller.waitForVM(waitContext, name)
	if responderImpl != nil {
		return responderImpl
	}

	session, err := controller.newSSHExecSession(
		ctx,
		waitContext,
		vm,
		execSessionKey{vmName: name},
		spec,
		runCommand,
		nil,
		legacyExecSessionPolicy,
	)
	if err != nil {
		return responder.JSON(http.StatusServiceUnavailable, NewErrorResponse("%v", err))
	}

	// Upgrade HTTP request to a WebSocket connection
	wsConn, err := websocket.Accept(ctx.Writer, ctx.Request, &websocket.AcceptOptions{
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		session.closeIfUnused()

		return responder.Error(err)
	}
	defer func() {
		// Ensure that we always close the accepted WebSocket connection,
		// otherwise resource leak is possible[1]
		//
		// [1]: https://github.com/coder/websocket/issues/445#issuecomment-2053792044
		_ = wsConn.CloseNow()
	}()

	return controller.serveExecSession(ctx, wsConn, session)
}

func (controller *Controller) execVMReconnectable(
	ctx *gin.Context,
	name string,
	sessionID string,
	spec execSessionSpec,
	runCommand string,
	wait uint64,
) responder.Responder {
	key := execSessionKey{
		vmName:    name,
		sessionID: sessionID,
	}

	session, ok := controller.execSessions.get(key)
	if ok {
		if !session.specMatches(spec) {
			return responder.JSON(http.StatusConflict,
				NewErrorResponse("exec session %q is already running with different options", sessionID))
		}
	} else {
		if spec.command == "" {
			return responder.JSON(http.StatusNotFound,
				NewErrorResponse("exec session %q does not exist", sessionID))
		}

		waitContext, waitContextCancel := context.WithTimeout(ctx, time.Duration(wait)*time.Second)
		defer waitContextCancel()

		vm, responderImpl := controller.waitForVM(waitContext, name)
		if responderImpl != nil {
			return responderImpl
		}

		var err error
		session, _, err = controller.execSessions.getOrCreate(waitContext, key, func() (*execSession, error) {
			return controller.newSSHExecSession(
				ctx,
				waitContext,
				vm,
				key,
				spec,
				runCommand,
				controller.execSessions,
				reconnectableExecSessionPolicy,
			)
		})
		if err != nil {
			return responder.JSON(http.StatusServiceUnavailable, NewErrorResponse("%v", err))
		}

		if !session.specMatches(spec) {
			return responder.JSON(http.StatusConflict,
				NewErrorResponse("exec session %q is already running with different options", sessionID))
		}
	}

	wsConn, err := websocket.Accept(ctx.Writer, ctx.Request, &websocket.AcceptOptions{
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		session.closeIfUnused()

		return responder.Error(err)
	}
	defer func() {
		_ = wsConn.CloseNow()
	}()

	return controller.serveExecSession(ctx, wsConn, session)
}

func (controller *Controller) newSSHExecSession(
	_ *gin.Context,
	waitContext context.Context,
	vm *v1.VM,
	key execSessionKey,
	spec execSessionSpec,
	runCommand string,
	registry *execSessionRegistry,
	policy execSessionPolicy,
) (*execSession, error) {
	sessionContext, sessionContextCancel := context.WithCancel(context.Background())

	type sshExecAttempt struct {
		exec *sshexec.Exec
	}

	attempt, err := retry.NewWithData[sshExecAttempt](
		retry.Context(waitContext),
		retry.DelayType(retry.FixedDelay),
		retry.Delay(time.Second),
		retry.Attempts(0),
		retry.LastErrorOnly(true),
	).Do(func() (sshExecAttempt, error) {
		exec, err := controller.execSSHClients.newExec(vm.UID, sshexec.Options{
			Interactive: spec.interactive,
			TTY:         spec.tty,
			Rows:        spec.rows,
			Cols:        spec.cols,
		}, func() (sshExecClient, error) {
			portForwardConn, err := controller.portForwardConnection(
				context.Background(),
				waitContext,
				vm.Worker,
				vm.UID,
				22,
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
			return sshExecAttempt{}, fmt.Errorf("failed to establish SSH connection to a VM: %w", err)
		}

		return sshExecAttempt{
			exec: exec,
		}, nil
	})
	if err != nil {
		sessionContextCancel()

		return nil, err
	}

	return newExecSessionWithContextAndSpec(
		sessionContext,
		sessionContextCancel,
		key,
		spec,
		runCommand,
		attempt.exec,
		nil,
		registry,
		controller.execSessionRetentionTTL,
		policy,
	), nil
}

func (controller *Controller) serveExecSession(
	ctx *gin.Context,
	wsConn *websocket.Conn,
	session *execSession,
) responder.Responder {
	subscriber, err := session.attach()
	if err != nil {
		_ = wsConn.Close(websocket.StatusNormalClosure, err.Error())

		return responder.Empty()
	}
	defer session.detach(subscriber)
	session.start()

	readFramesErrCh := make(chan error, 1)
	workerInputErrCh := make(chan error, 1)
	go func() {
		readFramesErrCh <- controller.readExecSessionFrames(ctx, wsConn, session, subscriber, workerInputErrCh)
	}()

	return controller.serveExecSessionFrames(ctx, wsConn, subscriber.frames, readFramesErrCh, workerInputErrCh)
}

func (controller *Controller) serveExecSessionFrames(
	ctx context.Context,
	wsConn *websocket.Conn,
	outgoingFrames <-chan *execstream.Frame,
	readFramesErrCh <-chan error,
	workerInputErrCh <-chan error,
) *responder.EmptyResponder {
	for {
		select {
		case workerInputErr := <-workerInputErrCh:
			controller.logger.Debugf("exec session: ignoring failed worker input: %v", workerInputErr)
		case readFramesErr := <-readFramesErrCh:
			if errors.Is(readFramesErr, errExecSessionClosed) {
				controller.closeExecWebSocket(ctx, wsConn,
					drainExecWebSocketFrames(ctx, wsConn), "Command finished")

				return responder.Empty()
			}

			if readFramesErr != nil &&
				!errors.Is(readFramesErr, errExecSessionDetached) {
				controller.logger.Warnf("failed to read and process exec frames from WebSocket: %v",
					readFramesErr)
			}

			return responder.Empty()
		case outgoingFrame, ok := <-outgoingFrames:
			if !ok {
				controller.closeExecWebSocket(ctx, wsConn, readFramesErrCh, "Command finished")

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

func (controller *Controller) closeExecWebSocket(
	ctx context.Context,
	wsConn *websocket.Conn,
	readErrCh <-chan error,
	reason string,
) {
	const execWebSocketDeliveryTimeout = 30 * time.Second

	pingCtx, pingCancel := context.WithTimeout(ctx, execWebSocketDeliveryTimeout)
	defer pingCancel()

	// A pong acknowledges the output and exit frames preceding this ping.
	pongErrCh := make(chan error, 1)
	go func() {
		pongErrCh <- wsConn.Ping(pingCtx)
	}()

	for {
		select {
		case readErr := <-readErrCh:
			if errors.Is(readErr, errExecWorkerInput) || errors.Is(readErr, errExecSessionClosed) {
				readErrCh = drainExecWebSocketFrames(ctx, wsConn)

				continue
			}

			var closeErr websocket.CloseError
			if errors.As(readErr, &closeErr) || errors.Is(readErr, io.EOF) ||
				errors.Is(readErr, errExecSessionDetached) {
				return
			}

			readErrCh = drainExecWebSocketFrames(ctx, wsConn)
		case pingErr := <-pongErrCh:
			if pingErr != nil {
				if errors.Is(pingErr, net.ErrClosed) || ctx.Err() != nil {
					return
				}

				controller.logger.Warnf("exec session: failed to acknowledge delivered output: %v", pingErr)
			}

			if err := wsConn.Close(websocket.StatusNormalClosure, reason); err != nil {
				controller.logger.Warnf("exec session: failed to close WebSocket connection: %v", err)
			}

			return
		case <-ctx.Done():
			return
		}
	}
}

func drainExecWebSocketFrames(ctx context.Context, wsConn *websocket.Conn) <-chan error {
	readErrCh := make(chan error, 1)

	go func() {
		for {
			if _, _, err := wsConn.Read(ctx); err != nil {
				readErrCh <- err

				return
			}
		}
	}()

	return readErrCh
}

var (
	errExecSessionDetached = errors.New("exec session detached")
	errExecSessionClosed   = errors.New("exec session closed")
	errExecWorkerInput     = errors.New("exec worker rejected client input")
)

func parseExecSessionSpec(ctx *gin.Context, command string) (execSessionSpec, string, error) {
	interactive, err := parseExecInteractive(ctx)
	if err != nil {
		return execSessionSpec{}, "", err
	}

	tty, err := parseExecBool(ctx, "tty")
	if err != nil {
		return execSessionSpec{}, "", err
	}
	if tty {
		interactive = true
	}

	rows, err := parseExecUint32(ctx.Query("rows"), "rows")
	if err != nil {
		return execSessionSpec{}, "", err
	}
	cols, err := parseExecUint32(ctx.Query("cols"), "cols")
	if err != nil {
		return execSessionSpec{}, "", err
	}
	if (rows == 0) != (cols == 0) {
		return execSessionSpec{}, "", errors.New("\"rows\" and \"cols\" must be provided together")
	}

	spec := execSessionSpec{
		command:     command,
		interactive: interactive,
		tty:         tty,
		rows:        rows,
		cols:        cols,
		env:         ctx.QueryMap("env"),
		workdir:     ctx.Query("workdir"),
	}

	runCommand, err := sshexec.CommandWithOptions(command, sshexec.Options{
		Env:     spec.env,
		Workdir: spec.workdir,
	})
	if err != nil {
		return execSessionSpec{}, "", err
	}

	return spec, runCommand, nil
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

func (controller *Controller) readExecSessionFrames(
	ctx context.Context,
	wsConn *websocket.Conn,
	session *execSession,
	subscriber *execSessionSubscriber,
	workerInputErrCh chan<- error,
) error {
	for {
		var frame execstream.Frame

		messageType, payloadBytes, err := wsConn.Read(ctx)
		if err != nil {
			var closeErr websocket.CloseError
			if errors.As(err, &closeErr) && closeErr.Code == websocket.StatusNormalClosure {
				return errExecSessionDetached
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
			if err := session.writeStdin(frame.Data); err != nil {
				if errors.Is(err, errExecSessionStdinUnavailable) {
					return fmt.Errorf("failed to handle %q frame: %w", frame.Type, err)
				}

				select {
				case workerInputErrCh <- fmt.Errorf("%w: failed to handle %q frame: %w",
					errExecWorkerInput, frame.Type, err):
				default:
				}
			}
		case execstream.FrameTypeResize:
			if frame.Terminal == nil {
				return fmt.Errorf("failed to handle %q frame: terminal size is required", frame.Type)
			}

			if err := session.resize(frame.Terminal.Rows, frame.Terminal.Cols); err != nil {
				if errors.Is(err, errExecSessionNoTTY) {
					return fmt.Errorf("failed to handle %q frame: %w", frame.Type, err)
				}

				select {
				case workerInputErrCh <- fmt.Errorf("%w: failed to handle %q frame: %w",
					errExecWorkerInput, frame.Type, err):
				default:
				}
			}
		case execstream.FrameTypeHistory:
			if !session.policy.replayEnabled {
				return fmt.Errorf("unexpected frame type received: %q", frame.Type)
			}

			session.sendHistory(subscriber, frame.Watermark)
		case execstream.FrameTypeAck:
			if !session.policy.replayEnabled {
				return fmt.Errorf("unexpected frame type received: %q", frame.Type)
			}

			session.ack(frame.Watermark)
		case execstream.FrameTypeDetach:
			if !session.policy.replayEnabled {
				return fmt.Errorf("unexpected frame type received: %q", frame.Type)
			}

			return errExecSessionDetached
		case execstream.FrameTypeClose:
			if !session.policy.replayEnabled {
				return fmt.Errorf("unexpected frame type received: %q", frame.Type)
			}

			session.close()

			return errExecSessionClosed
		default:
			return fmt.Errorf("unexpected frame type received: %q", frame.Type)
		}
	}
}
