package controller

import (
	"context"
	"errors"
	"io"
	"maps"
	"net"
	"sync"
	"time"

	"github.com/cirruslabs/orchard/internal/execstream"
)

const execSessionReplayBufferBytes = 4 * 1024 * 1024

type execSessionPolicy struct {
	closeOnDetach   bool
	retainAfterExit bool
	replayEnabled   bool
}

var (
	legacyExecSessionPolicy = execSessionPolicy{
		closeOnDetach: true,
	}
	reconnectableExecSessionPolicy = execSessionPolicy{
		retainAfterExit: true,
		replayEnabled:   true,
	}
)

type sshExecRunner interface {
	Stdin() io.WriteCloser
	Resize(rows uint32, cols uint32) error
	Run(ctx context.Context, command string, outgoingFrames chan<- *execstream.Frame) error
	Close() error
}

type execSessionSpec struct {
	command     string
	interactive bool
	tty         bool
	rows        uint32
	cols        uint32
	env         map[string]string
	workdir     string
}

func (spec execSessionSpec) clone() execSessionSpec {
	spec.env = maps.Clone(spec.env)

	return spec
}

func (spec execSessionSpec) equal(other execSessionSpec) bool {
	return spec.command == other.command &&
		spec.interactive == other.interactive &&
		spec.tty == other.tty &&
		spec.rows == other.rows &&
		spec.cols == other.cols &&
		spec.workdir == other.workdir &&
		maps.Equal(spec.env, other.env)
}

type execSessionKey struct {
	vmName    string
	sessionID string
}

type execSessionCreation struct {
	done    chan struct{}
	session *execSession
	err     error
}

type execSessionRegistry struct {
	mu       sync.Mutex
	sessions map[execSessionKey]*execSession
	creating map[execSessionKey]*execSessionCreation
}

func newExecSessionRegistry() *execSessionRegistry {
	return &execSessionRegistry{
		sessions: map[execSessionKey]*execSession{},
		creating: map[execSessionKey]*execSessionCreation{},
	}
}

func (registry *execSessionRegistry) get(key execSessionKey) (*execSession, bool) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	session, ok := registry.sessions[key]

	return session, ok
}

func (registry *execSessionRegistry) getOrCreate(
	ctx context.Context,
	key execSessionKey,
	create func() (*execSession, error),
) (*execSession, bool, error) {
	registry.mu.Lock()

	if session, ok := registry.sessions[key]; ok {
		registry.mu.Unlock()

		return session, false, nil
	}

	if creation, ok := registry.creating[key]; ok {
		registry.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-creation.done:
			if creation.err != nil {
				return nil, false, creation.err
			}

			return creation.session, false, nil
		}
	}

	creation := &execSessionCreation{done: make(chan struct{})}
	registry.creating[key] = creation
	registry.mu.Unlock()

	session, err := create()

	registry.mu.Lock()
	delete(registry.creating, key)
	if err == nil {
		registry.sessions[key] = session
	}
	creation.session = session
	creation.err = err
	close(creation.done)
	registry.mu.Unlock()

	return session, true, err
}

func (registry *execSessionRegistry) remove(key execSessionKey, expected *execSession) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	if registry.sessions[key] == expected {
		delete(registry.sessions, key)
	}
}

func (registry *execSessionRegistry) closeAll() {
	registry.mu.Lock()
	sessions := make([]*execSession, 0, len(registry.sessions))
	for _, session := range registry.sessions {
		sessions = append(sessions, session)
	}
	registry.mu.Unlock()

	for _, session := range sessions {
		session.close()
	}
}

type execReplayFrame struct {
	frame *execstream.Frame
	size  int
}

type execReplayBuffer struct {
	frames         []execReplayFrame
	bufferBytes    int
	nextWatermark  uint64
	ackedWatermark uint64
}

func (buffer *execReplayBuffer) append(frame *execstream.Frame) *execstream.Frame {
	frame = cloneExecFrame(frame)
	buffer.nextWatermark++
	frame.Watermark = buffer.nextWatermark

	frameSize := execFrameSize(frame)
	buffer.frames = append(buffer.frames, execReplayFrame{
		frame: frame,
		size:  frameSize,
	})
	buffer.bufferBytes += frameSize
	buffer.trimAcknowledged()
	buffer.trimToLimit()

	return frame
}

func (buffer *execReplayBuffer) ack(watermark uint64) {
	if watermark <= buffer.ackedWatermark {
		return
	}

	buffer.ackedWatermark = watermark
	buffer.trimAcknowledged()
}

func (buffer *execReplayBuffer) replayAfter(
	watermark uint64,
	frames []*execstream.Frame,
) []*execstream.Frame {
	for _, record := range buffer.frames {
		if record.frame.Watermark <= watermark {
			continue
		}

		frames = append(frames, record.frame)
	}

	return frames
}

func (buffer *execReplayBuffer) trimAcknowledged() {
	for len(buffer.frames) > 0 && buffer.frames[0].frame.Watermark <= buffer.ackedWatermark {
		buffer.bufferBytes -= buffer.frames[0].size
		buffer.frames = buffer.frames[1:]
	}
}

func (buffer *execReplayBuffer) trimToLimit() {
	for buffer.bufferBytes > execSessionReplayBufferBytes && len(buffer.frames) > 0 {
		buffer.bufferBytes -= buffer.frames[0].size
		buffer.frames = buffer.frames[1:]
	}
}

type execSessionSubscriber struct {
	frames        chan *execstream.Frame
	closed        chan struct{}
	closeOnce     sync.Once
	sendMu        sync.Mutex
	sentWatermark uint64
}

func newExecSessionSubscriber() *execSessionSubscriber {
	return &execSessionSubscriber{
		frames: make(chan *execstream.Frame, 128),
		closed: make(chan struct{}),
	}
}

func (subscriber *execSessionSubscriber) enqueue(frame *execstream.Frame) bool {
	subscriber.sendMu.Lock()
	defer subscriber.sendMu.Unlock()

	return subscriber.sendLocked(frame)
}

func (subscriber *execSessionSubscriber) sendHistory(frames []*execstream.Frame) bool {
	for _, frame := range frames {
		if !subscriber.sendLocked(frame) {
			return false
		}
	}

	return true
}

func (subscriber *execSessionSubscriber) sendLocked(frame *execstream.Frame) bool {
	if subscriber.alreadySentLocked(frame) {
		return true
	}

	select {
	case <-subscriber.closed:
		return false
	default:
	}

	select {
	case subscriber.frames <- subscriber.markSentLocked(frame):
		return true
	case <-subscriber.closed:
		return false
	}
}

func (subscriber *execSessionSubscriber) alreadySentLocked(frame *execstream.Frame) bool {
	return isReplayOutputFrame(frame) &&
		frame.Watermark != 0 &&
		frame.Watermark <= subscriber.sentWatermark
}

func (subscriber *execSessionSubscriber) markSentLocked(frame *execstream.Frame) *execstream.Frame {
	frame = cloneExecFrame(frame)
	if isReplayOutputFrame(frame) && frame.Watermark > subscriber.sentWatermark {
		subscriber.sentWatermark = frame.Watermark
	}

	return frame
}

func (subscriber *execSessionSubscriber) close() {
	subscriber.closeOnce.Do(func() {
		close(subscriber.closed)
		subscriber.sendMu.Lock()
		close(subscriber.frames)
		subscriber.sendMu.Unlock()
	})
}

type execSession struct {
	key          execSessionKey
	spec         execSessionSpec
	command      string
	exec         sshExecRunner
	transport    net.Conn
	registry     *execSessionRegistry
	retentionTTL time.Duration
	policy       execSessionPolicy

	ctx    context.Context
	cancel context.CancelFunc

	mu          sync.Mutex
	stdin       io.WriteCloser
	stdinClosed bool
	subscribers map[*execSessionSubscriber]struct{}
	replay      execReplayBuffer
	started     bool
	finished    bool
	closed      bool
	expiryTimer *time.Timer

	startOnce sync.Once
	done      chan struct{}
	doneOnce  sync.Once
}

func newExecSession(
	key execSessionKey,
	command string,
	exec sshExecRunner,
	transport net.Conn,
	registry *execSessionRegistry,
	retentionTTL time.Duration,
	policy execSessionPolicy,
) *execSession {
	ctx, cancel := context.WithCancel(context.Background())

	return newExecSessionWithContextAndSpec(
		ctx,
		cancel,
		key,
		execSessionSpec{command: command},
		command,
		exec,
		transport,
		registry,
		retentionTTL,
		policy,
	)
}

func newExecSessionWithContextAndSpec(
	ctx context.Context,
	cancel context.CancelFunc,
	key execSessionKey,
	spec execSessionSpec,
	command string,
	exec sshExecRunner,
	transport net.Conn,
	registry *execSessionRegistry,
	retentionTTL time.Duration,
	policy execSessionPolicy,
) *execSession {
	if ctx == nil || cancel == nil {
		ctx, cancel = context.WithCancel(context.Background())
	}

	session := &execSession{
		key:          key,
		spec:         spec.clone(),
		command:      command,
		exec:         exec,
		transport:    transport,
		registry:     registry,
		retentionTTL: retentionTTL,
		policy:       policy,
		ctx:          ctx,
		cancel:       cancel,
		stdin:        exec.Stdin(),
		subscribers:  map[*execSessionSubscriber]struct{}{},
		done:         make(chan struct{}),
	}

	return session
}

func (session *execSession) specMatches(spec execSessionSpec) bool {
	return spec.command == "" || session.spec.equal(spec)
}

func (session *execSession) start() {
	session.startOnce.Do(func() {
		session.mu.Lock()
		if session.closed {
			session.mu.Unlock()

			return
		}
		session.started = true
		session.mu.Unlock()

		go session.run()
	})
}

func (session *execSession) closeIfUnused() {
	session.mu.Lock()
	unused := !session.started && len(session.subscribers) == 0
	session.mu.Unlock()

	if unused {
		session.close()
	}
}

func (session *execSession) attach() (*execSessionSubscriber, error) {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.closed {
		return nil, errors.New("exec session is closed")
	}

	subscriber := newExecSessionSubscriber()
	session.subscribers[subscriber] = struct{}{}

	return subscriber, nil
}

func (session *execSession) detach(subscriber *execSessionSubscriber) {
	if session.policy.closeOnDetach {
		session.close()

		return
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	session.detachLocked(subscriber)
}

func (session *execSession) detachLocked(subscriber *execSessionSubscriber) {
	if _, ok := session.subscribers[subscriber]; !ok {
		return
	}

	delete(session.subscribers, subscriber)
	subscriber.close()
}

func (session *execSession) writeStdin(data []byte) error {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.stdin == nil || session.stdinClosed {
		return errors.New("this exec session has no stdin enabled or it is already closed")
	}

	if len(data) == 0 {
		if err := session.stdin.Close(); err != nil {
			return err
		}

		session.stdinClosed = true

		return nil
	}

	_, err := session.stdin.Write(data)

	return err
}

func (session *execSession) resize(rows uint32, cols uint32) error {
	session.mu.Lock()
	defer session.mu.Unlock()

	if !session.spec.tty {
		return errors.New("this exec session has no TTY")
	}

	return session.exec.Resize(rows, cols)
}

func (session *execSession) ack(watermark uint64) {
	if !session.policy.replayEnabled {
		return
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	session.replay.ack(watermark)
}

func (session *execSession) sendHistory(
	subscriber *execSessionSubscriber,
	watermark uint64,
) {
	if !session.policy.replayEnabled {
		return
	}

	session.mu.Lock()

	if _, ok := session.subscribers[subscriber]; !ok {
		session.mu.Unlock()

		return
	}

	subscriber.sendMu.Lock()
	frames := session.replay.replayAfter(watermark, nil)
	frames = append(frames, &execstream.Frame{
		Type:      execstream.FrameTypeNoMoreHistory,
		Watermark: session.replay.nextWatermark,
	})
	session.mu.Unlock()

	ok := subscriber.sendHistory(frames)
	subscriber.sendMu.Unlock()

	if !ok {
		session.dropSubscriber(subscriber)
	}
}

func (session *execSession) close() {
	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()

		return
	}

	session.closed = true
	if session.expiryTimer != nil {
		session.expiryTimer.Stop()
		session.expiryTimer = nil
	}

	subscribers := session.takeSubscribersLocked()
	session.mu.Unlock()

	closeSubscribers(subscribers)

	session.cancel()
	_ = session.exec.Close()
	if session.transport != nil {
		_ = session.transport.Close()
	}
	if session.registry != nil {
		session.registry.remove(session.key, session)
	}
}

func (session *execSession) run() {
	outgoingFrames := make(chan *execstream.Frame)
	runErrCh := make(chan error, 1)

	go func() {
		runErrCh <- session.exec.Run(session.ctx, session.command, outgoingFrames)
		close(outgoingFrames)
	}()

	for frame := range outgoingFrames {
		session.recordFrame(frame)
	}

	runErr := <-runErrCh
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		session.recordFrame(&execstream.Frame{
			Type:  execstream.FrameTypeError,
			Error: runErr.Error(),
		})
	}

	session.markFinished()
}

func (session *execSession) recordFrame(frame *execstream.Frame) {
	session.mu.Lock()

	if session.closed {
		session.mu.Unlock()

		return
	}

	if session.policy.replayEnabled {
		frame = session.replay.append(frame)
	} else {
		frame = cloneExecFrame(frame)
	}

	subscribers := make([]*execSessionSubscriber, 0, len(session.subscribers))
	for subscriber := range session.subscribers {
		subscribers = append(subscribers, subscriber)
	}
	session.mu.Unlock()

	for _, subscriber := range subscribers {
		if !subscriber.enqueue(frame) {
			session.dropSubscriber(subscriber)
		}
	}
}

func (session *execSession) markFinished() {
	session.mu.Lock()
	if session.finished {
		session.mu.Unlock()

		return
	}

	session.finished = true
	shouldClose := !session.policy.retainAfterExit
	if !session.closed && session.policy.retainAfterExit {
		session.expiryTimer = time.AfterFunc(session.retentionTTL, session.expire)
	}

	var subscribers []*execSessionSubscriber
	if shouldClose {
		subscribers = session.takeSubscribersLocked()
	}
	session.mu.Unlock()

	closeSubscribers(subscribers)

	session.doneOnce.Do(func() {
		close(session.done)
	})

	if shouldClose {
		session.close()
	}
}

func (session *execSession) expire() {
	session.close()
}

func (session *execSession) takeSubscribersLocked() []*execSessionSubscriber {
	subscribers := make([]*execSessionSubscriber, 0, len(session.subscribers))
	for subscriber := range session.subscribers {
		subscribers = append(subscribers, subscriber)
	}
	session.subscribers = map[*execSessionSubscriber]struct{}{}

	return subscribers
}

func closeSubscribers(subscribers []*execSessionSubscriber) {
	for _, subscriber := range subscribers {
		subscriber.close()
	}
}

func (session *execSession) dropSubscriber(subscriber *execSessionSubscriber) {
	session.mu.Lock()
	defer session.mu.Unlock()

	session.detachLocked(subscriber)
}

func cloneExecFrame(frame *execstream.Frame) *execstream.Frame {
	if frame == nil {
		return nil
	}

	clone := *frame
	if frame.Data != nil {
		clone.Data = append([]byte(nil), frame.Data...)
	}
	if frame.Exit != nil {
		exit := *frame.Exit
		clone.Exit = &exit
	}
	if frame.Terminal != nil {
		terminal := *frame.Terminal
		clone.Terminal = &terminal
	}

	return &clone
}

func execFrameSize(frame *execstream.Frame) int {
	if frame == nil {
		return 0
	}

	return len(frame.Data) + len(frame.Error) + 16
}

func isReplayOutputFrame(frame *execstream.Frame) bool {
	if frame == nil {
		return false
	}

	switch frame.Type {
	case execstream.FrameTypeStdout,
		execstream.FrameTypeStderr,
		execstream.FrameTypeExit,
		execstream.FrameTypeError:
		return true
	default:
		return false
	}
}
