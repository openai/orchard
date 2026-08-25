package controller

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cirruslabs/orchard/internal/execstream"
	"github.com/stretchr/testify/require"
)

type fakeExec struct {
	stdin      io.WriteCloser
	run        func(context.Context, string, chan<- *execstream.Frame) error
	resize     func(uint32, uint32) error
	closeCalls atomic.Int32
}

func (exec *fakeExec) Stdin() io.WriteCloser {
	return exec.stdin
}

func (exec *fakeExec) Run(
	ctx context.Context,
	command string,
	outgoingFrames chan<- *execstream.Frame,
) error {
	if exec.run != nil {
		return exec.run(ctx, command, outgoingFrames)
	}

	return nil
}

func (exec *fakeExec) Resize(rows uint32, cols uint32) error {
	if exec.resize != nil {
		return exec.resize(rows, cols)
	}

	return nil
}

func (exec *fakeExec) Close() error {
	exec.closeCalls.Add(1)

	return nil
}

func newManualExecSessionForTest(
	key execSessionKey,
	registry *execSessionRegistry,
) *execSession {
	ctx, cancel := context.WithCancel(context.Background())

	return &execSession{
		key:          key,
		spec:         execSessionSpec{command: "echo test"},
		command:      "echo test",
		exec:         &fakeExec{},
		registry:     registry,
		retentionTTL: time.Minute,
		policy:       reconnectableExecSessionPolicy,
		ctx:          ctx,
		cancel:       cancel,
		subscribers:  map[*execSessionSubscriber]struct{}{},
		done:         make(chan struct{}),
	}
}

func TestExecSessionRegistryGetOrCreateReusesInflightCreation(t *testing.T) {
	registry := newExecSessionRegistry()
	key := execSessionKey{vmName: "vm", sessionID: "session"}

	createStarted := make(chan struct{})
	releaseCreate := make(chan struct{})
	var createCalls atomic.Int32

	create := func() (*execSession, error) {
		createCalls.Add(1)
		close(createStarted)
		<-releaseCreate

		return newManualExecSessionForTest(key, registry), nil
	}

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		_, _, err := registry.getOrCreate(context.Background(), key, create)
		require.NoError(t, err)
	}()

	<-createStarted

	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		_, created, err := registry.getOrCreate(context.Background(), key, create)
		require.NoError(t, err)
		require.False(t, created)
	}()

	close(releaseCreate)

	<-firstDone
	<-secondDone
	require.EqualValues(t, 1, createCalls.Load())
}

func TestExecSessionStartRunsCommandOnlyOnce(t *testing.T) {
	var runCalls atomic.Int32
	runStarted := make(chan struct{})

	session := newExecSession(
		execSessionKey{vmName: "vm", sessionID: "session"},
		"echo test",
		&fakeExec{
			run: func(ctx context.Context, _ string, _ chan<- *execstream.Frame) error {
				runCalls.Add(1)
				close(runStarted)
				<-ctx.Done()

				return ctx.Err()
			},
		},
		nil,
		nil,
		time.Minute,
		reconnectableExecSessionPolicy,
	)
	defer session.close()

	session.start()
	session.start()

	<-runStarted
	require.EqualValues(t, 1, runCalls.Load())
}

func TestExecSessionSpecMatchesOptions(t *testing.T) {
	session := newManualExecSessionForTest(execSessionKey{vmName: "vm", sessionID: "session"}, nil)
	session.spec = execSessionSpec{
		command:     "echo test",
		interactive: true,
		tty:         true,
		rows:        24,
		cols:        80,
		env:         map[string]string{"GREETING": "hello"},
		workdir:     "/tmp",
	}

	require.True(t, session.specMatches(execSessionSpec{}))
	require.True(t, session.specMatches(execSessionSpec{
		command:     "echo test",
		interactive: true,
		tty:         true,
		rows:        24,
		cols:        80,
		env:         map[string]string{"GREETING": "hello"},
		workdir:     "/tmp",
	}))
	require.False(t, session.specMatches(execSessionSpec{
		command:     "echo test",
		interactive: true,
		tty:         true,
		rows:        24,
		cols:        80,
		env:         map[string]string{"GREETING": "goodbye"},
		workdir:     "/tmp",
	}))
}

func TestExecSessionResizeRequiresTTY(t *testing.T) {
	session := newManualExecSessionForTest(execSessionKey{vmName: "vm", sessionID: "session"}, nil)

	err := session.resize(24, 80)
	require.ErrorContains(t, err, "no TTY")
}

func TestExecSessionResizeDelegatesToRunner(t *testing.T) {
	var resizedRows, resizedCols uint32
	session := newManualExecSessionForTest(execSessionKey{vmName: "vm", sessionID: "session"}, nil)
	session.spec.tty = true
	session.exec = &fakeExec{
		resize: func(rows uint32, cols uint32) error {
			resizedRows = rows
			resizedCols = cols

			return nil
		},
	}

	require.NoError(t, session.resize(24, 80))
	require.EqualValues(t, 24, resizedRows)
	require.EqualValues(t, 80, resizedCols)
}

func TestExecSessionHistoryReplayAndAck(t *testing.T) {
	registry := newExecSessionRegistry()
	session := newManualExecSessionForTest(execSessionKey{vmName: "vm", sessionID: "session"}, registry)

	session.recordFrame(&execstream.Frame{Type: execstream.FrameTypeStdout, Data: []byte("out")})
	session.recordFrame(&execstream.Frame{Type: execstream.FrameTypeStderr, Data: []byte("err")})
	session.recordFrame(&execstream.Frame{
		Type: execstream.FrameTypeExit,
		Exit: &execstream.Exit{Code: 7},
	})

	subscriber, err := session.attach()
	require.NoError(t, err)

	session.sendHistory(subscriber, 0)

	require.Equal(t, execstream.FrameTypeStdout, (<-subscriber.frames).Type)
	require.Equal(t, execstream.FrameTypeStderr, (<-subscriber.frames).Type)
	require.Equal(t, execstream.FrameTypeExit, (<-subscriber.frames).Type)
	noMoreHistory := <-subscriber.frames
	require.Equal(t, execstream.FrameTypeNoMoreHistory, noMoreHistory.Type)
	require.EqualValues(t, 3, noMoreHistory.Watermark)

	session.ack(2)
	require.Len(t, session.replay.frames, 1)
	require.EqualValues(t, 3, session.replay.frames[0].frame.Watermark)
}

func TestExecSessionHistoryReplayStreamsPastSubscriberBuffer(t *testing.T) {
	registry := newExecSessionRegistry()
	session := newManualExecSessionForTest(execSessionKey{vmName: "vm", sessionID: "session"}, registry)

	const frameCount = 256
	for i := 0; i < frameCount; i++ {
		session.recordFrame(&execstream.Frame{
			Type: execstream.FrameTypeStdout,
			Data: []byte{byte(i)},
		})
	}

	subscriber, err := session.attach()
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		session.sendHistory(subscriber, 0)
	}()

	for i := 1; i <= frameCount; i++ {
		frame := <-subscriber.frames
		require.Equal(t, execstream.FrameTypeStdout, frame.Type)
		require.EqualValues(t, i, frame.Watermark)
	}

	noMoreHistory := <-subscriber.frames
	require.Equal(t, execstream.FrameTypeNoMoreHistory, noMoreHistory.Type)
	require.EqualValues(t, frameCount, noMoreHistory.Watermark)

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestExecSessionLiveOutputAppliesBackpressure(t *testing.T) {
	session := newManualExecSessionForTest(execSessionKey{vmName: "vm", sessionID: "session"}, nil)
	session.policy = legacyExecSessionPolicy
	t.Cleanup(session.close)

	subscriber, err := session.attach()
	require.NoError(t, err)

	const frameCount = 256
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range frameCount {
			session.recordFrame(&execstream.Frame{
				Type:      execstream.FrameTypeStdout,
				Data:      []byte{byte(i)},
				Terminal:  nil,
				Exit:      nil,
				Error:     "",
				Watermark: 0,
			})
		}
		session.recordFrame(&execstream.Frame{
			Type:      execstream.FrameTypeExit,
			Data:      nil,
			Terminal:  nil,
			Exit:      &execstream.Exit{Code: 0},
			Error:     "",
			Watermark: 0,
		})
	}()

	require.Eventually(t, func() bool {
		return len(subscriber.frames) == cap(subscriber.frames)
	}, time.Second, time.Millisecond)

	for i := range frameCount {
		frame, ok := <-subscriber.frames
		require.True(t, ok, "subscriber closed before output frame %d", i)
		require.Equal(t, execstream.FrameTypeStdout, frame.Type)
		require.Equal(t, []byte{byte(i)}, frame.Data)
	}

	exitFrame, ok := <-subscriber.frames
	require.True(t, ok, "subscriber closed before the exit frame")
	require.Equal(t, execstream.FrameTypeExit, exitFrame.Type)
	require.EqualValues(t, 0, exitFrame.Exit.Code)

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func TestReconnectableExecSessionDropsStalledSubscriber(t *testing.T) {
	session := newManualExecSessionForTest(execSessionKey{vmName: "vm", sessionID: "session"}, nil)
	t.Cleanup(session.close)

	stalledSubscriber, err := session.attach()
	require.NoError(t, err)

	for i := range cap(stalledSubscriber.frames) {
		session.recordFrame(&execstream.Frame{
			Type:      execstream.FrameTypeStdout,
			Data:      []byte{byte(i)},
			Terminal:  nil,
			Exit:      nil,
			Error:     "",
			Watermark: 0,
		})
	}

	healthySubscriber, err := session.attach()
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		session.recordFrame(&execstream.Frame{
			Type:      execstream.FrameTypeStdout,
			Data:      []byte("still running"),
			Terminal:  nil,
			Exit:      nil,
			Error:     "",
			Watermark: 0,
		})
		session.recordFrame(&execstream.Frame{
			Type:      execstream.FrameTypeExit,
			Data:      nil,
			Terminal:  nil,
			Exit:      &execstream.Exit{Code: 0},
			Error:     "",
			Watermark: 0,
		})
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("a stalled reconnectable subscriber blocked live output")
	}

	outputFrame := <-healthySubscriber.frames
	require.Equal(t, execstream.FrameTypeStdout, outputFrame.Type)
	require.Equal(t, []byte("still running"), outputFrame.Data)

	exitFrame := <-healthySubscriber.frames
	require.Equal(t, execstream.FrameTypeExit, exitFrame.Type)
	require.EqualValues(t, 0, exitFrame.Exit.Code)

	select {
	case <-stalledSubscriber.closed:
	default:
		t.Fatal("the stalled reconnectable subscriber was not dropped")
	}

	reconnectedSubscriber, err := session.attach()
	require.NoError(t, err)
	session.sendHistory(reconnectedSubscriber, uint64(cap(stalledSubscriber.frames)))

	require.Equal(t, execstream.FrameTypeStdout, (<-reconnectedSubscriber.frames).Type)
	require.Equal(t, execstream.FrameTypeExit, (<-reconnectedSubscriber.frames).Type)
	require.Equal(t, execstream.FrameTypeNoMoreHistory, (<-reconnectedSubscriber.frames).Type)
}

func TestExecSessionDetachKeepsProcessAlive(t *testing.T) {
	registry := newExecSessionRegistry()
	session := newManualExecSessionForTest(execSessionKey{vmName: "vm", sessionID: "session"}, registry)
	exec := session.exec.(*fakeExec)

	subscriber, err := session.attach()
	require.NoError(t, err)

	session.detach(subscriber)

	require.False(t, session.closed)
	require.EqualValues(t, 0, exec.closeCalls.Load())
}

func TestLegacyExecSessionDetachStopsProcess(t *testing.T) {
	registry := newExecSessionRegistry()
	session := newManualExecSessionForTest(execSessionKey{vmName: "vm", sessionID: "session"}, registry)
	session.policy = legacyExecSessionPolicy
	exec := session.exec.(*fakeExec)

	subscriber, err := session.attach()
	require.NoError(t, err)

	session.detach(subscriber)

	require.True(t, session.closed)
	require.EqualValues(t, 1, exec.closeCalls.Load())
}

func TestLegacyExecSessionDoesNotRetainReplayHistory(t *testing.T) {
	registry := newExecSessionRegistry()
	session := newManualExecSessionForTest(execSessionKey{vmName: "vm", sessionID: "session"}, registry)
	session.policy = legacyExecSessionPolicy

	session.recordFrame(&execstream.Frame{Type: execstream.FrameTypeStdout, Data: []byte("out")})

	require.Empty(t, session.replay.frames)
	require.Zero(t, session.replay.nextWatermark)
}

func TestExecSessionCloseIfUnusedClosesIdleSession(t *testing.T) {
	registry := newExecSessionRegistry()
	key := execSessionKey{vmName: "vm", sessionID: "session"}
	session := newManualExecSessionForTest(key, registry)
	exec := session.exec.(*fakeExec)
	registry.sessions[key] = session

	session.closeIfUnused()

	require.True(t, session.closed)
	require.EqualValues(t, 1, exec.closeCalls.Load())
}

func TestExecSessionCloseIfUnusedKeepsAttachedSession(t *testing.T) {
	registry := newExecSessionRegistry()
	session := newManualExecSessionForTest(execSessionKey{vmName: "vm", sessionID: "session"}, registry)

	_, err := session.attach()
	require.NoError(t, err)

	session.closeIfUnused()

	require.False(t, session.closed)
}

func TestExecSessionCloseStopsProcessAndRemovesRegistryEntry(t *testing.T) {
	registry := newExecSessionRegistry()
	key := execSessionKey{vmName: "vm", sessionID: "session"}
	session := newManualExecSessionForTest(key, registry)
	exec := session.exec.(*fakeExec)
	registry.sessions[key] = session

	session.close()

	require.True(t, session.closed)
	require.EqualValues(t, 1, exec.closeCalls.Load())
	_, ok := registry.get(key)
	require.False(t, ok)
}

func TestExecSessionFinishedEntryExpiresAfterTTL(t *testing.T) {
	registry := newExecSessionRegistry()
	key := execSessionKey{vmName: "vm", sessionID: "session"}
	session := newManualExecSessionForTest(key, registry)
	session.retentionTTL = 10 * time.Millisecond
	registry.sessions[key] = session

	session.markFinished()

	require.Eventually(t, func() bool {
		_, ok := registry.get(key)

		return !ok
	}, time.Second, 10*time.Millisecond)
}

func TestExecSessionFinishKeepsReconnectableSubscriberOpen(t *testing.T) {
	registry := newExecSessionRegistry()
	session := newManualExecSessionForTest(execSessionKey{vmName: "vm", sessionID: "session"}, registry)

	subscriber, err := session.attach()
	require.NoError(t, err)

	session.recordFrame(&execstream.Frame{Type: execstream.FrameTypeStdout, Data: []byte("out")})
	session.recordFrame(&execstream.Frame{
		Type: execstream.FrameTypeExit,
		Exit: &execstream.Exit{Code: 0},
	})
	session.markFinished()

	require.Equal(t, execstream.FrameTypeStdout, (<-subscriber.frames).Type)
	require.Equal(t, execstream.FrameTypeExit, (<-subscriber.frames).Type)

	session.sendHistory(subscriber, 0)

	noMoreHistory, ok := <-subscriber.frames
	require.True(t, ok)
	require.Equal(t, execstream.FrameTypeNoMoreHistory, noMoreHistory.Type)
	require.EqualValues(t, 2, noMoreHistory.Watermark)
}
