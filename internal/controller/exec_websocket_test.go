//nolint:testpackage // exercising exec shutdown requires controller internals
package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cirruslabs/orchard/internal/execstream"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestExecWebSocketWaitsForSlowClientBeforeClosing(t *testing.T) {
	controller := new(Controller)
	controller.logger = zap.NewNop().Sugar()
	controller.pingInterval = 5 * time.Second
	exitWritten := make(chan struct{})
	serverClosed := make(chan struct{})

	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			t.Errorf("accept exec websocket: %v", err)

			return
		}
		defer connection.CloseNow()
		defer close(serverClosed)

		readErrCh := make(chan error, 1)
		go func(ctx context.Context) {
			_, _, readErr := connection.Read(ctx)
			readErrCh <- readErr
		}(request.Context())

		for range 24 {
			payload, marshalErr := json.Marshal(struct {
				Type string `json:"type"`
				Data []byte `json:"data"`
			}{Type: "stdout", Data: bytes.Repeat([]byte("x"), 4096)})
			if marshalErr != nil {
				t.Errorf("marshal exec output: %v", marshalErr)

				return
			}
			if writeErr := connection.Write(request.Context(), websocket.MessageText, payload); writeErr != nil {
				t.Errorf("write exec output: %v", writeErr)

				return
			}
		}

		exitFrame := []byte(`{"type":"exit","exit":{"code":0}}`)
		if err := connection.Write(request.Context(), websocket.MessageText, exitFrame); err != nil {
			t.Errorf("write exec exit frame: %v", err)

			return
		}
		close(exitWritten)

		controller.closeExecWebSocket(request.Context(), connection, readErrCh, "command exited with code 0")
	}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	dialOptions := new(websocket.DialOptions)
	dialOptions.HTTPClient = server.Client()
	client, _, err := websocket.Dial(ctx, url, dialOptions) //nolint:bodyclose // websocket.Dial owns its response body
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.CloseNow()
	})

	select {
	case <-exitWritten:
	case <-ctx.Done():
		t.Fatal("server did not finish writing exec output")
	}

	select {
	case <-serverClosed:
		t.Fatal("server closed before the slow client consumed the exit frame")
	case <-time.After(5250 * time.Millisecond):
	case <-ctx.Done():
		t.Fatal("timed out before simulating a slow exec client")
	}

	for range 24 {
		_, payload, readErr := client.Read(ctx)
		require.NoError(t, readErr)

		var frame struct {
			Type string `json:"type"`
		}
		require.NoError(t, json.Unmarshal(payload, &frame))
		require.Equal(t, "stdout", frame.Type)
	}

	_, payload, err := client.Read(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `{"type":"exit","exit":{"code":0}}`, string(payload))
	require.NoError(t, client.Close(websocket.StatusNormalClosure, "exit received"))

	select {
	case <-serverClosed:
	case <-ctx.Done():
		t.Fatal("server did not exit after the client acknowledged the exit frame")
	}
}

func TestExecWebSocketDrainsLateClientFramesAfterForwardingError(t *testing.T) {
	testExecWebSocketDrainsLateClientFrames(t, io.ErrClosedPipe)
}

func TestExecWebSocketDrainsLateClientFramesAfterWorkerEOF(t *testing.T) {
	testExecWebSocketDrainsLateClientFrames(t, io.EOF)
}

func testExecWebSocketDrainsLateClientFrames(t *testing.T, forwardingErr error) {
	t.Helper()

	controller := new(Controller)
	controller.logger = zap.NewNop().Sugar()
	controller.pingInterval = 2 * time.Second
	exitWritten := make(chan struct{})
	clientFrameForwarded := make(chan struct{})
	serverClosed := make(chan struct{})

	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			t.Errorf("accept exec websocket: %v", err)

			return
		}
		defer connection.CloseNow()
		defer close(serverClosed)

		readErrCh := make(chan error, 1)
		go func(ctx context.Context) {
			_, _, readErr := connection.Read(ctx)
			if readErr != nil {
				readErrCh <- readErr

				return
			}

			close(clientFrameForwarded)
			readErrCh <- fmt.Errorf("%w: %w", errExecWorkerInput, forwardingErr)
		}(request.Context())

		for _, payload := range [][]byte{
			[]byte(`{"type":"stdout","data":"eA=="}`),
			[]byte(`{"type":"exit","exit":{"code":0}}`),
		} {
			if writeErr := connection.Write(request.Context(), websocket.MessageText, payload); writeErr != nil {
				t.Errorf("write exec frame: %v", writeErr)

				return
			}
		}
		close(exitWritten)

		controller.closeExecWebSocket(request.Context(), connection, readErrCh, "command exited with code 0")
	}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	dialOptions := new(websocket.DialOptions)
	dialOptions.HTTPClient = server.Client()
	client, _, err := websocket.Dial(ctx, url, dialOptions) //nolint:bodyclose // websocket.Dial owns its response body
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.CloseNow()
	})

	select {
	case <-exitWritten:
	case <-ctx.Done():
		t.Fatal("server did not write the exit frame")
	}

	require.NoError(t, client.Write(ctx, websocket.MessageText, []byte(`{"type":"stdin","data":"eA=="}`)))

	select {
	case <-clientFrameForwarded:
	case <-ctx.Done():
		t.Fatal("server did not process the late stdin frame")
	}

	select {
	case <-serverClosed:
		t.Fatal("server treated the worker forwarding error as a client close")
	case <-time.After(100 * time.Millisecond):
	}

	require.NoError(t, client.Write(ctx, websocket.MessageText,
		[]byte(`{"type":"resize","terminal":{"rows":24,"cols":80}}`)))

	for _, expectedType := range []string{"stdout", "exit"} {
		_, payload, readErr := client.Read(ctx)
		require.NoError(t, readErr)

		var frame struct {
			Type string `json:"type"`
		}
		require.NoError(t, json.Unmarshal(payload, &frame))
		require.Equal(t, expectedType, frame.Type)
	}

	require.NoError(t, client.Close(websocket.StatusNormalClosure, "exit received"))

	select {
	case <-serverClosed:
	case <-ctx.Done():
		t.Fatal("server did not exit after the client acknowledged the exit frame")
	}
}

func TestExecWebSocketServerClosesPromptlyAfterOutputIsAcknowledged(t *testing.T) {
	controller := new(Controller)
	controller.logger = zap.NewNop().Sugar()
	controller.pingInterval = 30 * time.Second
	serverClosed := make(chan struct{})

	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			t.Errorf("accept exec websocket: %v", err)

			return
		}
		defer connection.CloseNow()
		defer close(serverClosed)

		readErrCh := make(chan error, 1)
		go func(ctx context.Context) {
			_, _, readErr := connection.Read(ctx)
			readErrCh <- readErr
		}(request.Context())

		exitFrame := []byte(`{"type":"exit","exit":{"code":0}}`)
		if err := connection.Write(request.Context(), websocket.MessageText, exitFrame); err != nil {
			t.Errorf("write exec exit frame: %v", err)

			return
		}

		controller.closeExecWebSocket(request.Context(), connection, readErrCh, "command exited with code 0")
	}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	dialOptions := new(websocket.DialOptions)
	dialOptions.HTTPClient = server.Client()
	client, _, err := websocket.Dial(ctx, url, dialOptions) //nolint:bodyclose // websocket.Dial owns its response body
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.CloseNow()
	})

	_, payload, err := client.Read(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `{"type":"exit","exit":{"code":0}}`, string(payload))

	closeCtx, closeCancel := context.WithTimeout(ctx, time.Second)
	t.Cleanup(closeCancel)
	_, _, err = client.Read(closeCtx)

	var closeErr websocket.CloseError
	require.ErrorAs(t, err, &closeErr)
	require.Equal(t, websocket.StatusNormalClosure, closeErr.Code)

	select {
	case <-serverClosed:
	case <-ctx.Done():
		t.Fatal("server did not exit after closing the acknowledged exec session")
	}
}

const (
	execWorkerErrorFirst = "worker-first"
	execExitFirst        = "exit-first"
	execEventsTogether   = "simultaneous"
)

func TestExecSessionPreservesExitWhenWorkerRejectsLateInput(t *testing.T) {
	for _, ordering := range []string{execWorkerErrorFirst, execExitFirst, execEventsTogether} {
		t.Run(ordering, func(t *testing.T) {
			testExecSessionPreservesExitWhenWorkerRejectsLateInput(t, ordering)
		})
	}
}

func testExecSessionPreservesExitWhenWorkerRejectsLateInput(t *testing.T, ordering string) {
	t.Helper()

	controller := new(Controller)
	controller.logger = zap.NewNop().Sugar()
	controller.pingInterval = 30 * time.Second
	outgoingFrames := make(chan *execstream.Frame, 2)
	readErrCh := make(chan error, 1)
	workerInputErrCh := make(chan error, 1)
	eventsReady := make(chan struct{})
	releaseSecondEvent := make(chan struct{})
	serverClosed := make(chan struct{})

	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			t.Errorf("accept exec websocket: %v", err)

			return
		}
		defer connection.CloseNow()
		defer close(serverClosed)
		go func(ctx context.Context) {
			for {
				if _, _, readErr := connection.Read(ctx); readErr != nil {
					readErrCh <- readErr

					return
				}
			}
		}(request.Context())

		enqueueOutput := func() {
			outputFrame := new(execstream.Frame)
			outputFrame.Type = execstream.FrameTypeStdout
			outputFrame.Data = []byte("x")
			outgoingFrames <- outputFrame

			exitFrame := new(execstream.Frame)
			exitFrame.Type = execstream.FrameTypeExit
			exitFrame.Exit = &execstream.Exit{Code: 0}
			outgoingFrames <- exitFrame
			close(outgoingFrames)
		}
		workerInputErr := fmt.Errorf("%w: %w", errExecWorkerInput, io.EOF)

		switch ordering {
		case execWorkerErrorFirst:
			workerInputErrCh <- workerInputErr
		case execExitFirst:
			enqueueOutput()
		case execEventsTogether:
			workerInputErrCh <- workerInputErr
			enqueueOutput()
		}

		if ordering != execEventsTogether {
			go func(ctx context.Context) {
				select {
				case <-releaseSecondEvent:
					if ordering == execWorkerErrorFirst {
						enqueueOutput()
					} else {
						workerInputErrCh <- workerInputErr
					}
				case <-ctx.Done():
				}
			}(request.Context())
		}

		close(eventsReady)
		_ = controller.serveExecSessionFrames(request.Context(), connection,
			outgoingFrames, readErrCh, workerInputErrCh)
	}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	dialOptions := new(websocket.DialOptions)
	dialOptions.HTTPClient = server.Client()
	client, _, err := websocket.Dial(ctx, url, dialOptions) //nolint:bodyclose // websocket.Dial owns its response body
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.CloseNow()
	})

	select {
	case <-eventsReady:
	case <-ctx.Done():
		t.Fatal("server did not prepare exec events")
	}

	if ordering == execWorkerErrorFirst {
		select {
		case <-serverClosed:
			t.Fatal("worker input error terminated the exec session before its exit event")
		case <-time.After(100 * time.Millisecond):
		}
	}
	if ordering != execEventsTogether {
		close(releaseSecondEvent)
	}

	for _, expectedType := range []string{string(execstream.FrameTypeStdout), string(execstream.FrameTypeExit)} {
		_, payload, readErr := client.Read(ctx)
		require.NoError(t, readErr)

		var frame struct {
			Type string `json:"type"`
		}
		require.NoError(t, json.Unmarshal(payload, &frame))
		require.Equal(t, expectedType, frame.Type)
	}

	_, _, err = client.Read(ctx)
	var closeErr websocket.CloseError
	require.ErrorAs(t, err, &closeErr)
	require.Equal(t, websocket.StatusNormalClosure, closeErr.Code)

	select {
	case <-serverClosed:
	case <-ctx.Done():
		t.Fatal("server did not close after delivering the exec exit frame")
	}
}

func TestExecSessionStopsWhenClientDisconnects(t *testing.T) {
	controller := new(Controller)
	controller.logger = zap.NewNop().Sugar()
	controller.pingInterval = 30 * time.Second
	serverClosed := make(chan struct{})

	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			t.Errorf("accept exec websocket: %v", err)

			return
		}
		defer connection.CloseNow()
		defer close(serverClosed)

		readErrCh := make(chan error, 1)
		go func(ctx context.Context) {
			_, _, readErr := connection.Read(ctx)
			readErrCh <- readErr
		}(request.Context())

		_ = controller.serveExecSessionFrames(request.Context(), connection,
			make(chan *execstream.Frame), readErrCh, nil)
	}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	dialOptions := new(websocket.DialOptions)
	dialOptions.HTTPClient = server.Client()
	client, _, err := websocket.Dial(ctx, url, dialOptions) //nolint:bodyclose // websocket.Dial owns its response body
	require.NoError(t, err)
	require.NoError(t, client.CloseNow())

	select {
	case <-serverClosed:
	case <-time.After(time.Second):
		t.Fatal("server kept the exec session alive after the client disconnected")
	}
}

func TestExecSessionRejectsUnsupportedClientInput(t *testing.T) {
	const stdinFrame = `{"type":"stdin","data":"eA=="}`

	for _, testCase := range []struct {
		name        string
		payload     string
		closedStdin bool
	}{
		{
			name:        "stdin-without-interactive-session",
			payload:     stdinFrame,
			closedStdin: false,
		},
		{
			name:        "stdin-after-client-closed-input",
			payload:     stdinFrame,
			closedStdin: true,
		},
		{
			name:        "resize-without-tty",
			payload:     `{"type":"resize","terminal":{"rows":24,"cols":80}}`,
			closedStdin: false,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			controller := new(Controller)
			controller.logger = zap.NewNop().Sugar()
			controller.pingInterval = 30 * time.Second
			session := newManualExecSessionForTest(execSessionKey{
				vmName:    "vm",
				sessionID: testCase.name,
			}, nil)
			t.Cleanup(session.close)

			if testCase.closedStdin {
				stdinReader, stdinWriter := io.Pipe()
				t.Cleanup(func() {
					_ = stdinReader.Close()
				})
				session.spec.interactive = true
				session.stdin = stdinWriter
				require.NoError(t, session.writeStdin(nil))
			}

			subscriber, err := session.attach()
			require.NoError(t, err)
			serverClosed := make(chan struct{})

			server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				connection, acceptErr := websocket.Accept(writer, request, nil)
				if acceptErr != nil {
					t.Errorf("accept exec websocket: %v", acceptErr)

					return
				}
				defer connection.CloseNow()
				defer close(serverClosed)
				defer session.detach(subscriber)

				readErrCh := make(chan error, 1)
				workerInputErrCh := make(chan error, 1)
				go func(ctx context.Context) {
					readErrCh <- controller.readExecSessionFrames(ctx, connection,
						session, subscriber, workerInputErrCh)
				}(request.Context())

				_ = controller.serveExecSessionFrames(request.Context(), connection,
					subscriber.frames, readErrCh, workerInputErrCh)
			}))
			t.Cleanup(server.Close)

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			t.Cleanup(cancel)

			url := "ws" + strings.TrimPrefix(server.URL, "http")
			dialOptions := new(websocket.DialOptions)
			dialOptions.HTTPClient = server.Client()
			client, _, err := websocket.Dial(ctx, url, dialOptions) //nolint:bodyclose // websocket.Dial owns its response body
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = client.CloseNow()
			})
			require.NoError(t, client.Write(ctx, websocket.MessageText, []byte(testCase.payload)))

			select {
			case <-serverClosed:
			case <-time.After(time.Second):
				t.Fatal("unsupported client input was silently ignored")
			}
		})
	}
}

func TestReconnectableExecHandlesControlsAfterWorkerInputError(t *testing.T) {
	for _, frameType := range []execstream.FrameType{
		execstream.FrameTypeClose,
		execstream.FrameTypeDetach,
		execstream.FrameTypeAck,
		execstream.FrameTypeHistory,
	} {
		t.Run(string(frameType), func(t *testing.T) {
			testReconnectableExecHandlesControlAfterWorkerInputError(t, frameType)
		})
	}
}

func testReconnectableExecHandlesControlAfterWorkerInputError(
	t *testing.T,
	controlType execstream.FrameType,
) {
	t.Helper()

	controller := new(Controller)
	controller.logger = zap.NewNop().Sugar()
	controller.pingInterval = 30 * time.Second
	session := newManualExecSessionForTest(execSessionKey{vmName: "vm", sessionID: "control-frame-session"}, nil)
	t.Cleanup(session.close)
	exec, ok := session.exec.(*fakeExec)
	require.True(t, ok)
	exec.resize = func(uint32, uint32) error {
		return io.EOF
	}
	session.spec.interactive = true
	session.spec.tty = true

	stdinReader, stdinWriter := io.Pipe()
	t.Cleanup(func() {
		_ = stdinWriter.Close()
	})
	require.NoError(t, stdinReader.CloseWithError(io.EOF))
	session.stdin = stdinWriter

	replayedFrame := new(execstream.Frame)
	replayedFrame.Type = execstream.FrameTypeStdout
	replayedFrame.Data = []byte("replayed")
	session.recordFrame(replayedFrame)

	subscriber, err := session.attach()
	require.NoError(t, err)
	serverClosed := make(chan struct{})

	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, acceptErr := websocket.Accept(writer, request, nil)
		if acceptErr != nil {
			t.Errorf("accept exec websocket: %v", acceptErr)

			return
		}
		defer connection.CloseNow()
		defer close(serverClosed)
		defer session.detach(subscriber)

		readErrCh := make(chan error, 1)
		workerInputErrCh := make(chan error, 1)
		go func(ctx context.Context) {
			readErrCh <- controller.readExecSessionFrames(ctx, connection,
				session, subscriber, workerInputErrCh)
		}(request.Context())

		_ = controller.serveExecSessionFrames(request.Context(), connection,
			subscriber.frames, readErrCh, workerInputErrCh)
	}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	dialOptions := new(websocket.DialOptions)
	dialOptions.HTTPClient = server.Client()
	client, _, err := websocket.Dial(ctx, url, dialOptions) //nolint:bodyclose // websocket.Dial owns its response body
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.CloseNow()
	})

	for _, payload := range []string{
		`{"type":"stdin","data":"eA=="}`,
		`{"type":"resize","terminal":{"rows":24,"cols":80}}`,
		`{"type":"stdin","data":"eQ=="}`,
	} {
		require.NoError(t, client.Write(ctx, websocket.MessageText, []byte(payload)))
	}

	controlFrame := new(execstream.Frame)
	controlFrame.Type = controlType
	if controlType == execstream.FrameTypeAck {
		controlFrame.Watermark = 1
	}
	require.NoError(t, execstream.WriteFrame(ctx, client, controlFrame))

	switch controlType {
	case execstream.FrameTypeClose:
		_, _, readErr := client.Read(ctx)
		require.Equal(t, websocket.StatusNormalClosure, websocket.CloseStatus(readErr))
		require.EqualValues(t, 1, exec.closeCalls.Load())
	case execstream.FrameTypeDetach:
		select {
		case <-serverClosed:
		case <-ctx.Done():
			t.Fatal("detach was ignored after worker input failures")
		}
		require.Zero(t, exec.closeCalls.Load())
	case execstream.FrameTypeAck:
		require.Eventually(t, func() bool {
			session.mu.Lock()
			defer session.mu.Unlock()

			return len(session.replay.frames) == 0
		}, time.Second, 10*time.Millisecond, "replay acknowledgment was ignored")
	case execstream.FrameTypeHistory:
		for _, expectedType := range []execstream.FrameType{
			execstream.FrameTypeStdout,
			execstream.FrameTypeNoMoreHistory,
		} {
			_, payload, readErr := client.Read(ctx)
			require.NoError(t, readErr)

			var frame execstream.Frame
			require.NoError(t, json.Unmarshal(payload, &frame))
			require.Equal(t, expectedType, frame.Type)
		}
	default:
		t.Fatalf("unexpected control frame type: %q", controlType)
	}
}

func TestExecWebSocketClosesNormallyAfterApplicationClose(t *testing.T) {
	controller := new(Controller)
	controller.logger = zap.NewNop().Sugar()
	controller.pingInterval = 30 * time.Second

	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			t.Errorf("accept exec websocket: %v", err)

			return
		}
		defer connection.CloseNow()

		if err := connection.Write(request.Context(), websocket.MessageText,
			[]byte(`{"type":"exit","exit":{"code":0}}`)); err != nil {
			t.Errorf("write exec exit frame: %v", err)

			return
		}

		readErrCh := make(chan error, 1)
		readErrCh <- errExecSessionClosed
		controller.closeExecWebSocket(request.Context(), connection, readErrCh, "Command finished")
	}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	dialOptions := new(websocket.DialOptions)
	dialOptions.HTTPClient = server.Client()
	client, _, err := websocket.Dial(ctx, url, dialOptions) //nolint:bodyclose // websocket.Dial owns its response body
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.CloseNow()
	})

	_, payload, err := client.Read(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `{"type":"exit","exit":{"code":0}}`, string(payload))

	_, _, err = client.Read(ctx)
	require.Equal(t, websocket.StatusNormalClosure, websocket.CloseStatus(err))
}
