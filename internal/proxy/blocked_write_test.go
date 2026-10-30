package proxy_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cirruslabs/orchard/internal/proxy"
	"github.com/stretchr/testify/require"
)

// These peers model a connection without write-side shutdown support. One copy
// finishes only once the reverse copy is blocked inside the destination's Write.
type blockedWritePeer struct {
	started  chan struct{}
	closed   chan struct{}
	readErr  error
	closeErr error
	writeErr error
	once     sync.Once
}

func (peer *blockedWritePeer) Read([]byte) (int, error) {
	<-peer.started
	return 0, peer.readErr
}

func (peer *blockedWritePeer) Write([]byte) (int, error) {
	close(peer.started)
	<-peer.closed
	if peer.writeErr != nil {
		return 0, peer.writeErr
	}
	return 0, net.ErrClosed
}

func (peer *blockedWritePeer) Close() error {
	peer.once.Do(func() { close(peer.closed) })
	return peer.closeErr
}

type sendingPeer struct {
	*bytes.Reader

	closed chan struct{}
	once   sync.Once
}

func (*sendingPeer) Write(payload []byte) (int, error) { return len(payload), nil }
func (peer *sendingPeer) Close() error {
	peer.once.Do(func() { close(peer.closed) })
	return nil
}

func TestConnectionsUnblocksDestinationWrites(t *testing.T) {
	failure := errors.New("synthetic source failure")
	for _, firstError := range []error{io.EOF, failure} {
		for _, reverse := range []bool{false, true} {
			name := firstError.Error()
			if reverse {
				name += "/reversed"
			}
			t.Run(name, func(t *testing.T) {
				source := &sendingPeer{Reader: bytes.NewReader([]byte("synthetic payload")), closed: make(chan struct{})}
				destination := &blockedWritePeer{
					started: make(chan struct{}), closed: make(chan struct{}), readErr: firstError,
				}
				result := make(chan error, 1)
				go func() {
					if reverse {
						result <- proxy.Connections(destination, source)
					} else {
						result <- proxy.Connections(source, destination)
					}
				}()
				// Also releases both goroutines when checking unchanged production code.
				t.Cleanup(func() { _ = source.Close(); _ = destination.Close() })
				select {
				case err := <-result:
					if firstError == io.EOF {
						require.NoError(t, err)
					} else {
						require.ErrorIs(t, err, failure)
					}
				case <-time.After(time.Second):
					t.Fatal("a blocked destination write was not interrupted after the other copy finished")
				}
				select {
				case <-destination.closed:
				default:
					t.Fatal("destination remained open")
				}
				select {
				case <-source.closed:
				default:
					t.Fatal("source remained open")
				}
			})
		}
	}
}

func TestConnectionsIgnoresEOFOnClose(t *testing.T) {
	// SSH channels may acknowledge a normal peer-close with io.EOF from
	// Close().  The EOF is an expected cleanup result, not a proxy failure.
	for _, reverse := range []bool{false, true} {
		for _, closeErr := range []error{io.EOF, fmt.Errorf("SSH channel closed: %w", io.EOF)} {
			name := closeErr.Error()
			if reverse {
				name += "/reversed"
			}
			t.Run(name, func(t *testing.T) {
				source := &sendingPeer{Reader: bytes.NewReader([]byte("payload")), closed: make(chan struct{})}
				destination := &blockedWritePeer{
					started: make(chan struct{}), closed: make(chan struct{}), readErr: io.EOF,
					closeErr: closeErr, writeErr: io.EOF,
				}
				t.Cleanup(func() { _ = source.Close(); _ = destination.Close() })
				result := make(chan error, 1)
				go func() {
					if reverse {
						result <- proxy.Connections(destination, source)
					} else {
						result <- proxy.Connections(source, destination)
					}
				}()
				select {
				case err := <-result:
					require.NoError(t, err)
				case <-time.After(time.Second):
					t.Fatal("proxy failed to complete after normal SSH EOF")
				}
			})
		}
	}
}

func TestConnectionsPreservesRealFailureAlongsideCloseEOF(t *testing.T) {
	failure := errors.New("synthetic source failure")
	source := &sendingPeer{Reader: bytes.NewReader([]byte("payload")), closed: make(chan struct{})}
	destination := &blockedWritePeer{
		started: make(chan struct{}), closed: make(chan struct{}),
		readErr: failure, closeErr: io.EOF, writeErr: io.EOF,
	}
	t.Cleanup(func() { _ = source.Close(); _ = destination.Close() })
	result := make(chan error, 1)
	go func() { result <- proxy.Connections(source, destination) }()
	select {
	case err := <-result:
		require.ErrorIs(t, err, failure)
	case <-time.After(time.Second):
		t.Fatal("proxy failed to return underlying I/O error")
	}
}

func TestConnectionsPreservesUnexpectedCloseError(t *testing.T) {
	closeFailure := errors.New("close failed")
	source := &sendingPeer{Reader: bytes.NewReader([]byte("payload")), closed: make(chan struct{})}
	destination := &blockedWritePeer{
		started: make(chan struct{}), closed: make(chan struct{}),
		readErr: io.EOF, closeErr: closeFailure,
	}
	t.Cleanup(func() { _ = source.Close(); _ = destination.Close() })
	result := make(chan error, 1)
	go func() { result <- proxy.Connections(source, destination) }()
	select {
	case err := <-result:
		require.ErrorIs(t, err, closeFailure)
	case <-time.After(time.Second):
		t.Fatal("proxy did not finish after close error")
	}
}
