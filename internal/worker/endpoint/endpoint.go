package endpoint

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"

	"github.com/cirruslabs/orchard/internal/proxy"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"go.uber.org/zap"
)

// Bound file descriptor usage by a single endpoint.
const maxEndpointListenerConnections = 128

// Dial connects to a target after an endpoint accepts a connection.
type Dial func(context.Context) (net.Conn, error)

//nolint:containedctx // the listener and accepted connections share an owned cancellation lifetime
type endpoint struct {
	port            uint16
	spec            v1.EndpointSpec
	listener        net.Listener
	dial            Dial
	logger          *zap.SugaredLogger
	failure         atomic.Pointer[error]
	connectionSlots chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
}

func newEndpoint(
	spec v1.EndpointSpec,
	bindTarget BindTarget,
	logger *zap.SugaredLogger,
) (*endpoint, error) {
	// Prepare the target dialer before opening the worker listener
	dial, err := bindTarget(spec.Target)
	if err != nil {
		return nil, err
	}

	// Claim a worker port from the requested range
	listener, port, err := listen(spec.WorkerPortRange)
	if err != nil {
		return nil, err
	}

	// Give the listener and all accepted connections one shared lifetime
	ctx, cancel := context.WithCancel(context.Background())
	result := &endpoint{
		port:            port,
		spec:            spec,
		listener:        listener,
		dial:            dial,
		logger:          logger,
		ctx:             ctx,
		cancel:          cancel,
		connectionSlots: make(chan struct{}, maxEndpointListenerConnections),
	}

	// Begin accepting connections only after construction is complete
	go result.accept()

	return result, nil
}

//nolint:forcetypeassert,gosec,noctx // owned TCP listeners intentionally bind all interfaces and return TCPAddr
func listen(portRange *v1.PortRange) (net.Listener, uint16, error) {
	// Let the operating system select a port when no range was requested
	if portRange == nil {
		listener, err := net.Listen("tcp", ":0")
		if err != nil {
			return nil, 0, fmt.Errorf("failed to bind a TCP listener: %w", err)
		}

		return listener, uint16(listener.Addr().(*net.TCPAddr).Port), nil
	}

	var lastErr error

	// Try every requested port in ascending order until one is available
	for candidate := int(portRange.Min); candidate <= int(portRange.Max); candidate++ {
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", candidate))
		if err != nil {
			lastErr = err

			continue
		}

		return listener, uint16(candidate), nil
	}

	return nil, 0, fmt.Errorf(
		"failed to bind a TCP listener in worker port range %d-%d: %w",
		portRange.Min,
		portRange.Max,
		lastErr,
	)
}

func (ep *endpoint) running() bool {
	return ep.ctx.Err() == nil
}

func (ep *endpoint) status() v1.EndpointStatus {
	if failure := ep.failure.Load(); failure != nil {
		return v1.EndpointStatus{
			Name:    ep.spec.Name,
			State:   v1.EndpointStateError,
			Message: (*failure).Error(),
		}
	}

	return v1.EndpointStatus{
		Name:       ep.spec.Name,
		WorkerPort: ep.port,
		State:      v1.EndpointStateListening,
	}
}

func (ep *endpoint) fail(err error) {
	// Preserve the first fatal listener error and make failure idempotent
	if ep.failure.CompareAndSwap(nil, &err) {
		ep.logger.Warnf("endpoint %q failed: %v", ep.spec.Name, err)
		ep.close()
	}
}

func (ep *endpoint) accept() {
	// Accept connections until the endpoint stops or the listener fails
	for {
		select {
		case ep.connectionSlots <- struct{}{}:
			// Successfully obtained a connection slot, proceed
		case <-ep.ctx.Done():
			return
		}

		connection, err := ep.listener.Accept()
		if err != nil {
			// Return connection slot back
			<-ep.connectionSlots

			// Listener closure is expected during normal endpoint shutdown
			if ep.running() {
				ep.fail(fmt.Errorf("failed to accept connections: %w", err))
			}

			return
		}

		// Forward each accepted connection independently
		go ep.forward(connection)
	}
}

func (ep *endpoint) forward(connection net.Conn) {
	// Return connection slot back once done
	defer func() { <-ep.connectionSlots }()

	// Close the client connection when forwarding ends or the endpoint stops
	defer connection.Close()
	stopClosingConnection := context.AfterFunc(ep.ctx, func() {
		_ = connection.Close()
	})
	defer stopClosingConnection()

	// Resolve and connect to the endpoint target lazily for this connection
	targetConnection, err := ep.dial(ep.ctx)
	if err != nil {
		if ep.running() {
			ep.logger.Debugf("failed to connect endpoint %q to its target: %v", ep.spec.Name, err)
		}

		return
	}

	// Close the target connection when forwarding ends or the endpoint stops
	defer targetConnection.Close()
	stopClosingTarget := context.AfterFunc(ep.ctx, func() {
		_ = targetConnection.Close()
	})
	defer stopClosingTarget()

	// Relay traffic in both directions until either side finishes
	if err := proxy.Connections(connection, targetConnection); err != nil && ep.running() {
		ep.logger.Debugf("endpoint %q TCP relay failed: %v", ep.spec.Name, err)
	}
}

func (ep *endpoint) close() {
	ep.cancel()
	_ = ep.listener.Close()
}
