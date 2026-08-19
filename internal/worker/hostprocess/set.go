//nolint:contextcheck,err113 // Host processes have independent lifetimes; preserve the original errors.
package hostprocess

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/cirruslabs/orchard/internal/worker/ondiskname"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
)

// Set owns the host processes associated with one VM.
type Set struct {
	workerName string
	vmName     string
	onDiskName ondiskname.OnDiskName
	processes  map[string]*Process
	started    bool
	mtx        sync.Mutex
}

func NewSet(
	workerName string,
	vmName string,
	onDiskName ondiskname.OnDiskName,
) *Set {
	return &Set{
		workerName: workerName,
		vmName:     vmName,
		onDiskName: onDiskName,
		processes:  make(map[string]*Process),
	}
}

func (set *Set) Start(ctx context.Context, specs []v1.HostProcess) error {
	set.mtx.Lock()
	defer set.mtx.Unlock()

	set.stopLocked()

	return set.startLocked(ctx, specs)
}

func (set *Set) Replace(ctx context.Context, specs []v1.HostProcess) error {
	set.mtx.Lock()
	defer set.mtx.Unlock()

	set.stopLocked()

	return set.startLocked(ctx, specs)
}

func (set *Set) Dial(ctx context.Context, name string) (net.Conn, error) {
	process := set.Lookup(name)

	if process == nil {
		return nil, fmt.Errorf("host process %q is not running", name)
	}

	connection, err := process.Dial(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to host process %q: %w", name, err)
	}

	return connection, nil
}

func (set *Set) Lookup(name string) *Process {
	set.mtx.Lock()
	defer set.mtx.Unlock()

	return set.processes[name]
}

func (set *Set) Ready() bool {
	set.mtx.Lock()
	defer set.mtx.Unlock()

	if !set.started {
		return false
	}

	for _, process := range set.processes {
		select {
		case <-process.done:
			return false
		default:
		}
	}

	return true
}

func (set *Set) Stop() {
	set.mtx.Lock()
	defer set.mtx.Unlock()

	set.stopLocked()
}

func (set *Set) startLocked(ctx context.Context, specs []v1.HostProcess) error {
	set.started = false

	if len(specs) == 0 {
		set.started = true

		return nil
	}

	controlSocket, err := set.onDiskName.ControlSocketPath()
	if err != nil {
		return err
	}

	// Start and register every process
	//
	// If any process fails to start, stop the entire set
	// rather than leaving it partially running.
	for _, spec := range specs {
		process, err := NewProcess(
			spec,
			set.workerName,
			set.vmName,
			controlSocket,
		)
		if err != nil {
			set.stopLocked()

			return fmt.Errorf("failed to start host process %q: %w", spec.Name, err)
		}

		set.processes[spec.Name] = process
	}

	// A host process is ready once it accepts connections on its socket
	for _, spec := range specs {
		process := set.processes[spec.Name]

		connection, err := process.Dial(ctx)
		if err != nil {
			set.stopLocked()

			return fmt.Errorf("failed to connect to host process %q: %w", spec.Name, err)
		}

		_ = connection.Close()
	}

	set.started = true

	return nil
}

func (set *Set) stopLocked() {
	set.started = false

	for name, process := range set.processes {
		delete(set.processes, name)

		process.Close()
	}
}
