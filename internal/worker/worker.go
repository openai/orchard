package worker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"time"

	goruntime "runtime"

	"github.com/cirruslabs/orchard/internal/dialer"
	"github.com/cirruslabs/orchard/internal/opentelemetry"
	"github.com/cirruslabs/orchard/internal/worker/dhcpleasetime"
	"github.com/cirruslabs/orchard/internal/worker/endpoint"
	"github.com/cirruslabs/orchard/internal/worker/ondiskname"
	"github.com/cirruslabs/orchard/internal/worker/platform"
	"github.com/cirruslabs/orchard/internal/worker/runtime"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager/tart"
	"github.com/cirruslabs/orchard/pkg/client"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/cirruslabs/orchard/rpc"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/dustin/go-humanize"
	"github.com/hashicorp/go-multierror"
	goversion "github.com/hashicorp/go-version"
	"github.com/samber/lo"
	"github.com/samber/mo"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"
)

const (
	pollInterval                 = 5 * time.Second
	workerResourceUpdateInterval = 15 * time.Second
	recoveredVMProtectionPeriod  = 30 * time.Second
	rpcWatchReconnectInterval    = 100 * time.Millisecond
	rpcWatchReconnectMaxInterval = 5 * time.Second
	rpcWatchReconnectMultiplier  = 2
	rpcWatchHealthyInterval      = time.Second
	onDiskVMSyncTimeout          = 30 * time.Second

	tartVersionSoftnetPolicyUpdates = "2.34.0"
)

var (
	ErrPollFailed           = errors.New("failed to poll controller")
	errRPCWatchDisconnected = errors.New("RPC watch disconnected")
)

type Worker struct {
	name          string
	nameSuffix    string
	syncRequested chan bool
	vmm           *vmmanager.VMManager
	client        *client.Client
	pollTicker    *time.Ticker
	recoveredVMs  map[ondiskname.OnDiskName]time.Time
	resources     v1.Resources
	labels        v1.Labels

	defaultCPU    uint64
	defaultMemory uint64

	runtime runtime.Runtime

	softnetPolicyUpdates mo.Option[bool]

	vmPullTimeHistogram metric.Float64Histogram

	dialer dialer.Dialer

	logger *zap.SugaredLogger
}

func New(client *client.Client, opts ...Option) (*Worker, error) {
	worker := &Worker{
		client:        client,
		pollTicker:    time.NewTicker(pollInterval),
		recoveredVMs:  make(map[ondiskname.OnDiskName]time.Time),
		vmm:           vmmanager.New(),
		syncRequested: make(chan bool, 1),
	}

	// Apply options
	for _, opt := range opts {
		opt(worker)
	}

	// Apply defaults
	if worker.name == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, err
		}

		worker.name = hostname
	}

	if worker.nameSuffix != "" {
		worker.name += worker.nameSuffix
	}

	if worker.runtime == nil {
		if goruntime.GOOS == "linux" {
			worker.runtime = runtime.NewVetu()
		} else {
			worker.runtime = runtime.NewTart()
		}
	}

	defaultResources := v1.Resources{}

	if worker.runtime.ID() == v1.RuntimeTart {
		defaultResources[v1.ResourceTartVMs] = 2
	}

	// Determine the number of the host's logical CPU cores
	numLogicalCPUs, err := cpu.Counts(true)
	if err != nil {
		worker.logger.Warnf("cannot determine the number of host's logical CPU cores, "+
			"%s resource will not be available: %v", v1.ResourceLogicalCores, err)
	} else {
		defaultResources[v1.ResourceLogicalCores] = uint64(numLogicalCPUs)
	}

	// Determine the size of the host's memory
	virtualMemoryStat, err := mem.VirtualMemory()
	if err != nil {
		worker.logger.Warnf("cannot determine the size of the host's memory, "+
			"%s resource will not be available: %v", v1.ResourceMemoryMiB, err)
	} else {
		defaultResources[v1.ResourceMemoryMiB] = virtualMemoryStat.Total / humanize.MiByte
	}

	worker.resources = defaultResources.Merged(worker.resources)

	// Worker, VMs and images-related metrics
	worker.vmPullTimeHistogram, err = opentelemetry.DefaultMeter.Float64Histogram(
		"org.cirruslabs.orchard.worker.vm.pull_time",
	)
	if err != nil {
		return nil, err
	}

	if worker.logger == nil {
		worker.logger = zap.NewNop().Sugar()
	}

	if worker.softnetPolicyUpdates.IsAbsent() &&
		worker.runtime.ID() == v1.RuntimeTart && !worker.runtime.Synthetic() {
		tartVersion, err := tart.Version(context.Background(), worker.logger)
		if err != nil {
			worker.logger.Warnf("failed to check whether Tart supports Softnet policy updates: %v", err)
		} else {
			minimumVersion := goversion.Must(goversion.NewSemver(tartVersionSoftnetPolicyUpdates))
			worker.softnetPolicyUpdates = mo.Some(tartVersion.GreaterThanOrEqual(minimumVersion))
		}
	}

	return worker, nil
}

func (worker *Worker) Run(ctx context.Context) error {
	if worker.runtime.ID() == v1.RuntimeTart {
		if err := dhcpleasetime.Check(); err != nil {
			worker.logger.Warnf("%v", err)
		}
	}

	var reconnectBackoff rpcWatchReconnectBackoff
	reconnectBackoff.reset()

	for {
		if err := worker.runNewSession(ctx, reconnectBackoff.reset); err != nil {
			if errors.Is(err, errRPCWatchDisconnected) {
				select {
				case <-time.After(reconnectBackoff.next()):
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			return err
		}

		select {
		case <-worker.pollTicker.C:
			// continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type rpcWatchReconnectBackoff struct {
	nextInterval time.Duration
}

func (backoff *rpcWatchReconnectBackoff) next() time.Duration {
	interval := backoff.nextInterval
	if interval <= 0 {
		interval = rpcWatchReconnectInterval
	}

	if interval >= rpcWatchReconnectMaxInterval/rpcWatchReconnectMultiplier {
		backoff.nextInterval = rpcWatchReconnectMaxInterval
	} else {
		backoff.nextInterval = interval * rpcWatchReconnectMultiplier
	}

	return min(interval, rpcWatchReconnectMaxInterval)
}

func (backoff *rpcWatchReconnectBackoff) reset() {
	backoff.nextInterval = rpcWatchReconnectInterval
}

func (worker *Worker) Close() error {
	var result error
	for _, vm := range worker.vmm.List() {
		<-vm.Stop()
	}
	for _, vm := range worker.vmm.List() {
		err := vm.Delete()
		if err != nil {
			result = multierror.Append(result, err)
		}
	}
	return result
}

func (worker *Worker) runNewSession(ctx context.Context, onWatchHealthy func()) error {
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Check the runtime before advertising this worker, but do not touch local
	// VMs until registration confirms that this worker belongs to this machine.
	vmInfos, err := worker.listOnDiskVMs(subCtx)
	if err != nil {
		worker.logger.Errorf("failed to list on-disk VMs: %v", err)

		return nil
	}

	if err := worker.registerWorker(subCtx); err != nil {
		worker.logger.Warnf("failed to register worker: %v", err)

		return nil
	}

	info, err := worker.client.Controller().Info(ctx)
	if err != nil {
		worker.logger.Warnf("failed to retrieve controller info: %v", err)

		return nil
	}

	group, sessionCtx := errgroup.WithContext(subCtx)
	worker.superviseRPCWatch(sessionCtx, ctx, group, info, onWatchHealthy)

	// Sync on-disk VMs
	if err := worker.syncOnDiskVMsWithInventory(sessionCtx, vmInfos); err != nil {
		cancel()
		watchErr := group.Wait()

		worker.logger.Errorf("failed to sync on-disk VMs: %v", err)

		if errors.Is(watchErr, errRPCWatchDisconnected) {
			return watchErr
		}

		return nil
	}

	recoveredVMs := worker.trackRecoveredVMs(time.Now())

	// Backward compatibility with for older Orchard Controllers
	updateFuncInner := worker.client.VMs().UpdateState

	if !info.Capabilities.Has(v1.ControllerCapabilityVMStateEndpoint) {
		updateFuncInner = worker.client.VMs().Update
	}

	// Ignore HTTP 404, because the VM might no longer exist while we're still processing it
	updateFunc := func(ctx context.Context, vm v1.VM) error {
		_, err = updateFuncInner(ctx, vm)
		var apiError *client.APIError
		if errors.As(err, &apiError) && apiError.StatusCode == http.StatusNotFound {
			return nil
		}

		return err
	}

	group.Go(func() error {
		for {
			if err := worker.updateWorker(sessionCtx); err != nil {
				return fmt.Errorf("failed to update worker resource: %w", err)
			}

			select {
			case <-sessionCtx.Done():
				return sessionCtx.Err()
			case <-time.After(workerResourceUpdateInterval):
				// Proceed
			}
		}
	})

	group.Go(func() error {
		for {
			if err := worker.syncVMs(sessionCtx, updateFunc, recoveredVMs); err != nil {
				return fmt.Errorf("failed to sync VMs: %w", err)
			}

			select {
			case <-sessionCtx.Done():
				return sessionCtx.Err()
			case <-worker.syncRequested:
			case <-worker.pollTicker.C:
				// Proceed
			}
		}
	})

	if err := group.Wait(); err != nil {
		worker.logger.Errorf("%v", err)

		if errors.Is(err, errRPCWatchDisconnected) {
			return err
		}
	}

	return nil
}

func (worker *Worker) superviseRPCWatch(
	sessionCtx context.Context,
	operationCtx context.Context,
	group *errgroup.Group,
	info v1.ControllerInfo,
	onWatchHealthy func(),
) {
	watchRPC := worker.watchRPC
	rpcVersion := "v1"

	if info.Capabilities.Has(v1.ControllerCapabilityRPCV2) {
		worker.logger.Infof("using WebSocket-based v2 RPC")
		watchRPC = worker.watchRPCV2
		rpcVersion = "v2"
	} else {
		worker.logger.Infof("using gRPC-based v1 RPC")
	}

	watchEstablished := make(chan struct{})

	group.Go(func() error {
		if err := watchRPC(sessionCtx, operationCtx, func() { close(watchEstablished) }); err != nil {
			if sessionCtx.Err() != nil {
				return sessionCtx.Err()
			}

			return fmt.Errorf("%w: failed to watch RPC %s: %w", errRPCWatchDisconnected, rpcVersion, err)
		}

		return fmt.Errorf("%w: RPC %s watch closed unexpectedly", errRPCWatchDisconnected, rpcVersion)
	})

	group.Go(func() error {
		return monitorRPCWatchHealth(sessionCtx, watchEstablished, rpcWatchHealthyInterval, onWatchHealthy)
	})
}

func monitorRPCWatchHealth(
	ctx context.Context,
	established <-chan struct{},
	healthyAfter time.Duration,
	onHealthy func(),
) error {
	select {
	case <-established:
	case <-ctx.Done():
		return ctx.Err()
	}

	healthTimer := time.NewTimer(healthyAfter)
	defer healthTimer.Stop()

	select {
	case <-healthTimer.C:
		onHealthy()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (worker *Worker) trackRecoveredVMs(now time.Time) map[ondiskname.OnDiskName]time.Time {
	if worker.recoveredVMs == nil {
		worker.recoveredVMs = make(map[ondiskname.OnDiskName]time.Time)
	}

	for onDiskName := range worker.recoveredVMs {
		if !worker.vmm.Exists(onDiskName) {
			delete(worker.recoveredVMs, onDiskName)
		}
	}

	for _, vm := range worker.vmm.List() {
		status := vm.Status()
		if status != v1.VMStatusPending && status != v1.VMStatusRunning {
			continue
		}

		onDiskName := vm.OnDiskName()
		if _, alreadyTracked := worker.recoveredVMs[onDiskName]; !alreadyTracked {
			worker.recoveredVMs[onDiskName] = now.Add(recoveredVMProtectionPeriod)
		}
	}

	return worker.recoveredVMs
}

func (worker *Worker) registerWorker(ctx context.Context) error {
	platformUUID, err := platform.MachineID()
	if err != nil {
		return err
	}

	_, err = worker.client.Workers().Create(ctx, v1.Worker{
		Meta: v1.Meta{
			Name: worker.name,
		},
		Arch:          v1.Architecture(goruntime.GOARCH),
		Runtime:       worker.runtime.ID(),
		Resources:     worker.resources,
		Labels:        worker.labels,
		LastSeen:      time.Now(),
		MachineID:     platformUUID,
		DefaultCPU:    worker.defaultCPU,
		DefaultMemory: worker.defaultMemory,
		Capabilities: lo.Ternary(worker.runtime.ID() == v1.RuntimeTart || worker.runtime.Synthetic(),
			v1.WorkerCapabilities{v1.WorkerCapabilityVMEndpoints}, nil),
	})
	if err != nil {
		return err
	}

	worker.logger.Infof("registered worker %s", worker.name)

	return nil
}

func (worker *Worker) updateWorker(ctx context.Context) error {
	workerResource, err := worker.client.Workers().Get(ctx, worker.name)
	if err != nil {
		return fmt.Errorf("%w: failed to retrieve worker from the API: %v", ErrPollFailed, err)
	}

	worker.logger.Debugf("got worker from the API")

	workerResource.LastSeen = time.Now()

	if _, err := worker.client.Workers().Update(ctx, *workerResource); err != nil {
		return fmt.Errorf("%w: failed to update worker in the API: %v", ErrPollFailed, err)
	}

	worker.logger.Debugf("updated worker in the API")

	return nil
}

//nolint:gocognit // VM lifecycle branches are clearest in a single reconciliation loop.
func (worker *Worker) syncVMs(
	ctx context.Context,
	updateVM func(context.Context, v1.VM) error,
	recoveredVMs map[ondiskname.OnDiskName]time.Time,
) error {
	allKeys := mapset.NewSet[ondiskname.OnDiskName]()

	remoteVMs, err := worker.client.VMs().FindForWorker(ctx, worker.name)
	if err != nil {
		return err
	}
	remoteVMsIndex := map[ondiskname.OnDiskName]*v1.VM{}
	for _, remoteVM := range remoteVMs {
		onDiskName := ondiskname.NewFromResource(remoteVM)
		allKeys.Add(onDiskName)
		// Can't take an address of a loop variable
		remoteVMCopy := remoteVM
		remoteVMsIndex[onDiskName] = &remoteVMCopy
	}

	localVMsIndex := map[ondiskname.OnDiskName]vmmanager.VM{}
	for _, vm := range worker.vmm.List() {
		onDiskName := vm.OnDiskName()
		allKeys.Add(onDiskName)
		localVMsIndex[onDiskName] = vm
	}

	worker.logger.Infof("syncing %d local VMs against %d remote VMs...",
		len(localVMsIndex), len(remoteVMsIndex))

	var pairs []lo.Tuple3[ondiskname.OnDiskName, *v1.VM, vmmanager.VM]

	for onDiskName := range allKeys.Iter() {
		vmResource := remoteVMsIndex[onDiskName]
		vm := localVMsIndex[onDiskName]

		pairs = append(pairs, lo.T3(onDiskName, vmResource, vm))
	}

	// It's important to process the remote VMs in failed state
	// and local VMs that ceased to exist remotely first, otherwise
	// we risk violating the scheduler resource assumptions
	sortNonExistentAndFailedFirst(pairs)

	hasUnaccountedRecoveredVM := false

	for _, tuple := range pairs {
		onDiskName, vmResource, vm := lo.Unpack3(tuple)

		remoteState := mo.None[v1.VMStatus]()
		if vmResource != nil {
			remoteState = mo.Some(vmResource.Status)
		}

		localState := mo.None[v1.VMStatus]()
		var localConditions []v1.Condition
		if vm != nil {
			localState = mo.Some(vm.Status())
			localConditions = vm.Conditions()
		}

		if shouldPreserveRecoveredVM(recoveredVMs, onDiskName, vmResource, localState, time.Now()) {
			hasUnaccountedRecoveredVM = true
			worker.logger.Warnf("preserving active VM %s missing from controller during worker session recovery",
				onDiskName)

			continue
		}

		action := transitions[remoteState][localState]

		worker.logger.Debugf("processing VM: %s, remote state: %s, local state: %s, "+
			"local conditions: [%s], action: %v", onDiskName, optionToString(remoteState),
			optionToString(localState), v1.ConditionsHumanize(localConditions), action)

		switch action {
		case ActionCreate:
			// Remote VM was created, but not the local VM
			if hasUnaccountedRecoveredVM {
				worker.logger.Warnf("deferring VM %s while recovered VMs are missing from controller inventory",
					onDiskName)

				continue
			}

			worker.createVM(onDiskName, *vmResource)
		case ActionMonitorPending:
			if vmResource.StatusMessage != vm.StatusMessage() {
				vmResource.StatusMessage = vm.StatusMessage()

				if err := updateVM(ctx, *vmResource); err != nil {
					return err
				}
			}
		case ActionReportRunning:
			// Remote VM was created, and the local VM too,
			// check if the local VM had already started
			// and update the remote VM as accordingly

			// Image FQN feature, see https://github.com/cirruslabs/orchard/issues/164
			if imageFQN := vm.ImageFQN(); imageFQN != nil {
				vmResource.ImageFQN = *imageFQN
			}

			// Mark the remote VM as started
			vmResource.Status = v1.VMStatusRunning
			vmResource.StatusMessage = vm.StatusMessage()

			if err := updateVM(ctx, *vmResource); err != nil {
				return err
			}
		case ActionMonitorRunning:
			currentVMResource := vm.Resource()

			if worker.softnetPolicyUpdates.OrElse(false) &&
				currentVMResource.SoftnetEnabled() && vmResource.SoftnetEnabled() &&
				currentVMResource.SoftnetPolicyChanged(vmResource.VMSpec) {
				if err := vm.UpdateSoftnetPolicy(ctx,
					vmResource.NetSoftnetAllow, vmResource.NetSoftnetBlock); err != nil {
					worker.logger.Warnf("failed to update Softnet policy in-place, "+
						"falling back to restart: %v", err)
				} else {
					currentVMResource.NetSoftnetAllow = vmResource.NetSoftnetAllow
					currentVMResource.NetSoftnetBlock = vmResource.NetSoftnetBlock

					// Advance the generation only if no other spec changes remain
					if v1.SemanticallyEqual(currentVMResource.VMSpec, vmResource.VMSpec) {
						currentVMResource = *vmResource
					}

					vm.SetResource(currentVMResource)
				}
			}

			if err := worker.monitorRunningVM(ctx, vmResource, vm, updateVM); err != nil {
				return err
			}
		case ActionStop:
			// VM has failed on the remote side, stop it locally to prevent incorrect
			// worker's resources calculation in the Controller's scheduler
			if err := waitForVMStop(ctx, vm); err != nil {
				return fmt.Errorf("failed to stop VM: %w", err)
			}
		case ActionFail, ActionLostTrack, ActionImpossible:
			// VM has failed on the local side, stop it before reporting as failed to prevent incorrect
			// worker's resources calculation in the Controller's scheduler
			if vm != nil {
				if err := waitForVMStop(ctx, vm); err != nil {
					return fmt.Errorf("failed to stop VM: %w", err)
				}
			}

			var statusMessage string

			switch action {
			case ActionFail:
				statusMessage = vm.Err().Error()
			case ActionLostTrack:
				statusMessage = "Worker lost track of VM"
			case ActionImpossible:
				statusMessage = "Encountered an impossible transition"
			}

			vmResource.Status = v1.VMStatusFailed
			vmResource.StatusMessage = statusMessage
			vmResource.ObservedEndpoints = nil
			if err := updateVM(ctx, *vmResource); err != nil {
				return err
			}
		case ActionDelete:
			// Remote VM was deleted, delete local VM
			//
			// Note: this check needs to run for each VM
			// before we attempt to create any VMs below.
			if err := worker.deleteVM(vm); err != nil {
				return err
			}
		}
	}

	return nil
}

func (worker *Worker) monitorRunningVM(
	ctx context.Context,
	vmResource *v1.VM,
	vm vmmanager.VM,
	updateVM func(context.Context, v1.VM) error,
) error {
	currentVMResource := vm.Resource()

	// Tracks whether any specification changes were applied without restarting the VM
	appliedInPlace := false

	// Endpoint specification changes do not require restarting the VM;
	// reconciliation happens separately below
	endpointsChanged := !v1.SemanticallyEqual(
		currentVMResource.Endpoints,
		vmResource.Endpoints,
	)

	if vmResource.PowerState == v1.PowerStateRunning &&
		!v1.ConditionIsTrue(vm.Conditions(), v1.ConditionTypeStopping) &&
		!v1.ConditionIsTrue(vm.Conditions(), v1.ConditionTypeSuspending) &&
		v1.ConditionIsTrue(vm.Conditions(), v1.ConditionTypeRunning) &&
		endpointsChanged {
		currentVMResource.Endpoints = vmResource.Endpoints
		appliedInPlace = true
	}

	// Advance the generation only if no other specification changes remain
	if appliedInPlace {
		if v1.SemanticallyEqual(currentVMResource.VMSpec, vmResource.VMSpec) {
			currentVMResource = *vmResource
		}

		vm.SetResource(currentVMResource)
	}

	worker.reconcileRunningVM(vmResource, vm) //nolint:contextcheck // Event streams outlive sync sessions.

	var updateNeeded bool

	if (worker.runtime.ID() == v1.RuntimeTart || worker.runtime.Synthetic()) &&
		worker.reconcileEndpoints(vmResource, vm) {
		updateNeeded = true
	}

	if vmResource.StatusMessage != vm.StatusMessage() {
		vmResource.StatusMessage = vm.StatusMessage()

		updateNeeded = true
	}

	if vmResource.ObservedGeneration != vm.Resource().ObservedGeneration {
		vmResource.ObservedGeneration = vm.Resource().ObservedGeneration

		updateNeeded = true
	}

	// Propagate VM's conditions to the Orchard Controller
	for _, condition := range vm.Conditions() {
		if v1.ConditionsSet(&vmResource.Conditions, condition) {
			updateNeeded = true
		}
	}

	if updateNeeded {
		return updateVM(ctx, *vmResource)
	}

	return nil
}

func (worker *Worker) reconcileEndpoints(
	vmResource *v1.VM,
	vm vmmanager.VM,
) bool {
	// Expose endpoints only when the requested VM generation is running
	endpointsShouldRun := vmResource.PowerState == v1.PowerStateRunning &&
		v1.ConditionIsTrue(vm.Conditions(), v1.ConditionTypeRunning) &&
		vmResource.Generation == vm.Resource().Generation

	desired := vmResource.Endpoints
	if !endpointsShouldRun {
		desired = nil
	}

	observed := vm.EndpointSet().Reconcile(
		desired,
		endpoint.NewVMTargetBinder(vm.IP, worker.dialer),
	)

	if slices.Equal(vmResource.ObservedEndpoints, observed) {
		return false
	}

	vmResource.ObservedEndpoints = observed

	return true
}

func (worker *Worker) reconcileRunningVM(vmResource *v1.VM, vm vmmanager.VM) {
	if vmResource.Generation == vm.Resource().Generation {
		return
	}

	stoppingOrSuspending := v1.ConditionIsTrue(vm.Conditions(), v1.ConditionTypeStopping) ||
		v1.ConditionIsTrue(vm.Conditions(), v1.ConditionTypeSuspending)
	if stoppingOrSuspending {
		return
	}

	if v1.ConditionIsTrue(vm.Conditions(), v1.ConditionTypeRunning) {
		// VM is running, suspend or stop it first.
		shouldStop := vmResource.PowerState == v1.PowerStateStopped || !vm.Resource().Suspendable

		if shouldStop {
			vm.Stop()
			return
		} else {
			vm.Suspend()
		}
	}

	if v1.ConditionIsFalse(vm.Conditions(), v1.ConditionTypeRunning) {
		// VM stopped, update its specification.
		vm.SetResource(*vmResource)

		if vmResource.PowerState == v1.PowerStateRunning {
			// Start the VM.
			eventStreamer := worker.client.VMs().StreamEvents(vmResource.Name)
			vm.Start(eventStreamer)
		}
	}
}

func shouldPreserveRecoveredVM(
	recoveredVMs map[ondiskname.OnDiskName]time.Time,
	onDiskName ondiskname.OnDiskName,
	remoteVM *v1.VM,
	localState mo.Option[v1.VMStatus],
	now time.Time,
) bool {
	deadline, recovered := recoveredVMs[onDiskName]
	if !recovered {
		return false
	}

	active := localState == mo.Some(v1.VMStatusPending) || localState == mo.Some(v1.VMStatusRunning)
	if remoteVM == nil && active && now.Before(deadline) {
		return true
	}

	// Once the controller recognizes a recovered VM, or the bounded recovery
	// window expires, user-requested deletion follows the normal lifecycle.
	delete(recoveredVMs, onDiskName)

	return false
}

func (worker *Worker) listOnDiskVMs(ctx context.Context) ([]vmmanager.VMInfo, error) {
	if worker.runtime.Synthetic() {
		// There's no on-disk VMs when using synthetic VMs
		return nil, nil
	}

	worker.logger.Infof("listing on-disk VMs...")

	runtimeCtx, cancelRuntime := context.WithTimeout(ctx, onDiskVMSyncTimeout)
	defer cancelRuntime()

	vmInfos, err := worker.runtime.ListVMs(runtimeCtx, worker.logger)
	if err != nil {
		if errors.Is(runtimeCtx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("timed out listing on-disk VMs: %w", context.DeadlineExceeded)
		}

		return nil, err
	}

	return vmInfos, nil
}

func (worker *Worker) syncOnDiskVMs(ctx context.Context) error {
	vmInfos, err := worker.listOnDiskVMs(ctx)
	if err != nil {
		return err
	}

	return worker.syncOnDiskVMsWithInventory(ctx, vmInfos)
}

//nolint:nestif,gocognit // complexity is tolerable for now
func (worker *Worker) syncOnDiskVMsWithInventory(ctx context.Context, vmInfos []vmmanager.VMInfo) error {
	if worker.runtime.Synthetic() {
		return nil
	}

	remoteVMs, err := worker.client.VMs().FindForWorker(ctx, worker.name)
	if err != nil {
		return err
	}
	remoteVMsIndex := map[ondiskname.OnDiskName]v1.VM{}
	for _, remoteVM := range remoteVMs {
		remoteVMsIndex[ondiskname.NewFromResource(remoteVM)] = remoteVM
	}

	worker.logger.Infof("syncing on-disk VMs...")

	for _, vmInfo := range vmInfos {
		onDiskName, err := ondiskname.Parse(vmInfo.Name)
		if err != nil {
			if errors.Is(err, ondiskname.ErrNotManagedByOrchard) {
				continue
			}

			return err
		}

		// VMs that exist in the Worker's VM manager will be handled in the syncVMs()
		if worker.vmm.Exists(onDiskName) {
			continue
		}

		remoteVM, ok := remoteVMsIndex[onDiskName]
		if !ok {
			// On-disk VM doesn't exist on the controller nor in the Worker's VM manager,
			// stop it (if applicable) and delete it
			if vmInfo.Running {
				_, _, err := worker.runtime.Cmd(ctx, worker.logger, "stop", vmInfo.Name)
				if err != nil {
					worker.logger.Warnf("failed to stop")
				}
			}

			_, _, err := worker.runtime.Cmd(ctx, worker.logger, "delete", vmInfo.Name)
			if err != nil {
				return err
			}
		} else if remoteVM.Status != v1.VMStatusPending {
			// On-disk VM exists on the controller and was acted upon,
			// but we've lost track of it, so shut it down (if applicable)
			// and report the error (if not failed yet)
			if vmInfo.Running {
				_, _, err := worker.runtime.Cmd(ctx, worker.logger, "stop", vmInfo.Name)
				if err != nil {
					worker.logger.Warnf("failed to stop")
				}
			}
		}
	}

	return nil
}

func waitForVMStop(ctx context.Context, vm vmmanager.VM) error {
	select {
	case err := <-vm.Stop():
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (worker *Worker) deleteVM(vm vmmanager.VM) error {
	<-vm.Stop()

	if err := vm.Delete(); err != nil {
		return err
	}

	worker.vmm.Delete(vm.OnDiskName())

	return nil
}

func (worker *Worker) createVM(odn ondiskname.OnDiskName, vmResource v1.VM) {
	eventStreamer := worker.client.VMs().StreamEvents(vmResource.Name)

	vm := worker.runtime.NewVM(vmResource, eventStreamer, worker.vmPullTimeHistogram,
		worker.dialer, worker.softnetPolicyUpdates.OrElse(false), worker.logger)

	worker.vmm.Put(odn, vm)
}

func (worker *Worker) grpcMetadata() metadata.MD {
	return metadata.Join(
		worker.client.GPRCMetadata(),
		metadata.Pairs(rpc.MetadataWorkerNameKey, worker.name),
	)
}

func (worker *Worker) requestVMSyncing() {
	select {
	case worker.syncRequested <- true:
		worker.logger.Debugf("Successfully requested syncing")
	default:
		worker.logger.Debugf("There's already a syncing request in the queue, skipping")
	}
}

func sortNonExistentAndFailedFirst(input []lo.Tuple3[ondiskname.OnDiskName, *v1.VM, vmmanager.VM]) {
	slices.SortStableFunc(input, func(left, right lo.Tuple3[ondiskname.OnDiskName, *v1.VM, vmmanager.VM]) int {
		_, leftVM, _ := lo.Unpack3(left)
		_, rightVM, _ := lo.Unpack3(right)

		leftNonExistent := leftVM == nil
		rightNonExistent := rightVM == nil

		switch {
		case leftNonExistent && rightNonExistent:
			return 0
		case leftNonExistent:
			return -1
		case rightNonExistent:
			return 1
		}

		leftFailed := leftVM != nil && leftVM.Status == v1.VMStatusFailed
		rightFailed := rightVM != nil && rightVM.Status == v1.VMStatusFailed

		switch {
		case leftFailed && rightFailed:
			return 0
		case leftFailed:
			return -1
		case rightFailed:
			return 1
		default:
			return 0
		}
	})
}
