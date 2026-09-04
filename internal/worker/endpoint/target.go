package endpoint

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/cirruslabs/orchard/internal/dialer"
	"github.com/cirruslabs/orchard/internal/udpconn"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"golang.org/x/sync/singleflight"
)

// BindTarget validates a declarative target and returns a lazy Dial.
// It runs synchronously during reconciliation and must not perform I/O.
type BindTarget func(v1.ConnectionTarget, v1.EndpointProtocol) (Dial, error)

//nolint:err113,forcetypeassert,perfsprint // preserve validation errors; the coalesced IP resolver returns a string
func NewVMTargetBinder(
	resolveIP func(context.Context) (string, error),
	networkDialer dialer.Dialer,
) BindTarget {
	// Use the standard network dialer unless the caller supplied one
	if networkDialer == nil {
		networkDialer = &net.Dialer{}
	}

	return func(target v1.ConnectionTarget, protocol v1.EndpointProtocol) (Dial, error) {
		// Validate the target synchronously during endpoint reconciliation
		if err := target.Validate(); err != nil {
			return nil, fmt.Errorf("invalid connection target: %w", err)
		}
		if target.VM == nil {
			return nil, fmt.Errorf("unsupported connection target")
		}

		// Capture the target port by value for the lifetime of this endpoint
		targetPort := target.VM.Port

		// Coalesce concurrent IP lookups for this endpoint without caching results
		var resolveGroup singleflight.Group

		return func(ctx context.Context) (net.Conn, error) {
			// Resolve the VM address lazily because it can change across VM runs
			resolved, err, _ := resolveGroup.Do("vm-ip", func() (any, error) {
				return resolveIP(ctx)
			})
			if err != nil {
				return nil, fmt.Errorf("failed to get VM's IP: %w", err)
			}

			// Reject an empty host before address construction can reinterpret it
			host := resolved.(string)
			if host == "" {
				return nil, fmt.Errorf("failed to get VM's IP: empty address")
			}

			address := net.JoinHostPort(host, strconv.Itoa(int(targetPort)))

			// Connect to the resolved VM address using the caller's cancellation context
			connection, err := networkDialer.DialContext(ctx, string(protocol), address)
			if err != nil {
				return nil, fmt.Errorf("failed to connect to the VM: %w", err)
			}

			if socket, ok := connection.(*net.UDPConn); ok {
				if err := udpconn.TuneSocket(socket); err != nil {
					_ = connection.Close()

					return nil, fmt.Errorf("failed to tune UDP socket: %w", err)
				}
			}

			return connection, nil
		}, nil
	}
}
