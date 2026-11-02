package endpoint_test

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cirruslabs/orchard/internal/dialer"
	"github.com/cirruslabs/orchard/internal/worker/endpoint"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
)

func TestTargetDialHonorsWaitingCallerDeadline(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var resolutions, connections atomic.Int32
	dialFailure := errors.New("synthetic dial result")
	binder := endpoint.NewVMTargetBinder(func(context.Context) (string, error) {
		resolutions.Add(1)
		close(entered)
		<-release
		return "192.0.2.1", nil
	}, dialer.DialFunc(func(context.Context, string, string) (net.Conn, error) {
		connections.Add(1)
		return nil, dialFailure
	}))
	dial, err := binder(v1.ConnectionTarget{VM: &v1.ConnectionTargetVM{Port: 1234}}, v1.EndpointProtocolTCP)
	require.NoError(t, err)
	leader := make(chan error, 1)
	go func() { _, err := dial(t.Context()); leader <- err }()
	<-entered
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	waiter := make(chan error, 1)
	go func() { _, err := dial(ctx); waiter <- err }()
	defer func() {
		close(release)
		require.ErrorIs(t, <-leader, dialFailure)
	}()
	select {
	case err := <-waiter:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("a cancelled waiter remained blocked on another caller's VM lookup")
	}
	require.EqualValues(t, 1, resolutions.Load())
	require.Zero(t, connections.Load(), "cancelled waiters must not reach the network dialer")
}

func TestTargetDialRejectsAnAlreadyCancelledContext(t *testing.T) {
	var resolutions atomic.Int32
	binder := endpoint.NewVMTargetBinder(func(context.Context) (string, error) {
		resolutions.Add(1)
		return "192.0.2.1", nil
	}, dialer.DialFunc(func(context.Context, string, string) (net.Conn, error) { return nil, net.ErrClosed }))
	dial, err := binder(v1.ConnectionTarget{VM: &v1.ConnectionTargetVM{Port: 1234}}, v1.EndpointProtocolTCP)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = dial(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, resolutions.Load())
}

func TestTargetDialDoesNotCacheFinishedLookups(t *testing.T) {
	var resolutions atomic.Int32
	failure := errors.New("synthetic resolution failure")
	binder := endpoint.NewVMTargetBinder(func(context.Context) (string, error) {
		resolutions.Add(1)
		return "", failure
	}, nil)
	dial, err := binder(v1.ConnectionTarget{VM: &v1.ConnectionTargetVM{Port: 1234}}, v1.EndpointProtocolTCP)
	require.NoError(t, err)
	for range 2 {
		_, err = dial(t.Context())
		require.ErrorIs(t, err, failure)
	}
	require.EqualValues(t, 2, resolutions.Load())
}
