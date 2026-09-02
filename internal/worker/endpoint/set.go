package endpoint

import (
	"slices"
	"sync"

	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"go.uber.org/zap"
)

// Set owns the endpoint listeners associated with one VM.
type Set struct {
	logger      *zap.SugaredLogger
	endpoints   map[string]*endpoint
	lastDesired []v1.EndpointSpec
	started     bool
	mtx         sync.Mutex
}

func NewSet(logger *zap.SugaredLogger) *Set {
	return &Set{
		logger:    logger,
		endpoints: make(map[string]*endpoint),
	}
}

func (set *Set) Start() {
	set.mtx.Lock()
	defer set.mtx.Unlock()

	set.stopLocked()
	set.started = true
}

func (set *Set) Reconcile(
	desired []v1.EndpointSpec,
	bindTarget BindTarget,
) []v1.EndpointStatus {
	set.mtx.Lock()
	defer set.mtx.Unlock()

	// Ignore reconciliation until the owning VM starts the endpoint set
	if !set.started {
		return nil
	}

	// Recreate the entire endpoint set on any change to avoid conflicts
	// between current and desired worker-port assignments
	if !v1.SemanticallyEqual(set.lastDesired, desired) {
		set.stopLocked()
		set.lastDesired = slices.Clone(desired)
	}

	if len(desired) == 0 {
		return nil
	}

	// Reuse healthy unchanged endpoints and recreate every other endpoint
	statuses := make([]v1.EndpointStatus, 0, len(desired))

	for _, spec := range desired {
		name := spec.Name

		if current := set.endpoints[name]; current != nil {
			status := current.status()

			if status.State == v1.EndpointStateListening {
				statuses = append(statuses, status)

				continue
			}

			delete(set.endpoints, name)
			current.close()
		}

		current, err := newEndpoint(spec, bindTarget, set.logger)
		if err != nil {
			statuses = append(statuses, v1.EndpointStatus{
				Name:    name,
				State:   v1.EndpointStateError,
				Message: err.Error(),
			})

			continue
		}

		set.endpoints[name] = current
		statuses = append(statuses, current.status())
	}

	return statuses
}

func (set *Set) Stop() {
	set.mtx.Lock()
	defer set.mtx.Unlock()

	set.started = false
	set.stopLocked()
}

func (set *Set) stopLocked() {
	for _, current := range set.endpoints {
		current.close()
	}
	clear(set.endpoints)
}
