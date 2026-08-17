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

	"github.com/avast/retry-go/v4"
	"github.com/cirruslabs/orchard/internal/dialer"
	"github.com/cirruslabs/orchard/internal/opentelemetry"
	"github.com/cirruslabs/orchard/internal/worker/dhcpleasetime"
	"github.com/cirruslabs/orchard/internal/worker/ondiskname"
	"github.com/cirruslabs/orchard/internal/worker/platform"
	"github.com/cirruslabs/orchard/internal/worker/runtime"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager"
	"github.com/cirruslabs/orchard/pkg/client"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/cirruslabs/orchard/rpc"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/dustin/go-humanize"
	"github.com/hashicorp/go-multierror"
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
)

var ErrPollFailed = errors.New("failed to poll controller")

type Worker struct {
	name          string
	nameSuffix    string
	syncRequested chan bool
	vmm           *vmmanager.VMManager
	client        *client.Client
	pollTicker    *time.Ticker
	resources     v1.Resources
	labels        v1.Labels

	defaultCPU    uint64
	defaultMemory uint64

	runtime runtime.Runtime

	vmPullTimeHistogram metric.Float64Histogram

	dialer dialer.Dialer

	logger *zap.SugaredLogger
}

func New(client *client.Client, opts ...Option) (*Worker, error) {
	worker := &Worker{
		client:        client,
		pollTicker:    time.NewTicker(pollInterval),
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

	return worker, nil
}

func (worker *Worker) Run(ctx context.Context) error {
	if worker.runtime.ID() == v1.RuntimeTart {
		if err := dhcpleasetime.Check(); err != nil {
			worker.logger.Warnf("%v", err)
		}
	}

	for {
		if err := worker.runNewSession(ctx); err != nil {
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

func (worker *Worker) runNewSession(ctx context.Context) error {
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := worker.registerWorker(subCtx); err != nil {
		worker.logger.Warnf("failed to register worker: %v", err)

		return nil
	}

	info, err := worker.client.Controller().Info(ctx)
	if err != nil {
		worker.logger.Warnf("failed to retrieve controller info: %v", err)

		return nil
	}

	if info.Capabilities.Has(v1.ControllerCapabilityRPCV2) {
		worker.logger.Infof("using WebSocket-based v2 RPC")

		go func() {
			_ = retry.Do(func() error {
				return worker.watchRPCV2(subCtx)
			}, retry.OnRetry(func(n uint, err error) {
				worker.logger.Warnf("failed to watch RPC v2: %v", err)
			}), retry.Context(subCtx), retry.Attempts(0))
		}()
	} else {
		worker.logger.Infof("using gRPC-based v1 RPC")

		go func() {
			_ = retry.Do(func() error {
				return worker.watchRPC(subCtx)
			}, retry.OnRetry(func(n uint, err error) {
				worker.logger.Warnf("failed to watch RPC v1: %v", err)
			}), retry.Context(subCtx), retry.Attempts(0))
		}()
	}

	// Sync on-disk VMs
	if err := worker.syncOnDiskVMs(ctx); err != nil {
		worker.logger.Errorf("failed to sync on-disk VMs: %v", err)

		return nil
	}

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

	group, ctx := errgroup.WithContext(subCtx)

	group.Go(func() error {
		for {
			if err := worker.updateWorker(ctx); err != nil {
				return fmt.Errorf("failed to update worker resource: %w", err)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(workerResourceUpdateInterval):
				// Proceed
			}
		}
	})

	group.Go(func() error {
		for {
			if err := worker.syncVMs(ctx, updateFunc); err != nil {
				return fmt.Errorf("failed to sync VMs: %w", err)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-worker.syncRequested:
			case <-worker.pollTicker.C:
				// Proceed
			}
		}
	})

	if err := group.Wait(); err != nil {
		worker.logger.Errorf("%v", err)
	}

	return nil
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

//nolint:nestif,gocognit // nested "if" and cognitive complexity is tolerable for now
func (worker *Worker) syncVMs(ctx context.Context, updateVM func(context.Context, v1.VM) error) error {
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

		action := transitions[remoteState][localState]

		worker.logger.Debugf("processing VM: %s, remote state: %s, local state: %s, "+
			"local conditions: [%s], action: %v", onDiskName, optionToString(remoteState),
			optionToString(localState), v1.ConditionsHumanize(localConditions), action)

		switch action {
		case ActionCreate:
			// Remote VM was created, but not the local VM
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
			if vmResource.Generation != vm.Resource().Generation {
				// VM specification changed, reboot the VM for the changes to take effect
				stoppingOrSuspending := v1.ConditionIsTrue(vm.Conditions(), v1.ConditionTypeStopping) ||
					v1.ConditionIsTrue(vm.Conditions(), v1.ConditionTypeSuspending)

				if v1.ConditionIsTrue(vm.Conditions(), v1.ConditionTypeRunning) && !stoppingOrSuspending {
					// VM is running, suspend or stop it first
					shouldStop := vmResource.PowerState == v1.PowerStateStopped || !vm.Resource().Suspendable

					if shouldStop {
						vm.Stop()
					} else {
						vm.Suspend()
					}
				}

				if v1.ConditionIsFalse(vm.Conditions(), v1.ConditionTypeRunning) && !stoppingOrSuspending {
					// VM stopped, update its specification
					vm.SetResource(*vmResource)

					if vmResource.PowerState == v1.PowerStateRunning {
						// Start the VM
						eventStreamer := worker.client.VMs().StreamEvents(vmResource.Name)
						vm.Start(eventStreamer)
					}
				}
			}

			var updateNeeded bool

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
				if err := updateVM(ctx, *vmResource); err != nil {
					return err
				}
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

//nolint:nestif,gocognit // complexity is tolerable for now
func (worker *Worker) syncOnDiskVMs(ctx context.Context) error {
	if worker.runtime.Synthetic() {
		// There's no on-disk VMs when using synthetic VMs
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

	vmInfos, err := worker.runtime.ListVMs(ctx, worker.logger)
	if err != nil {
		return err
	}

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

	vm := worker.runtime.NewVM(vmResource, eventStreamer, worker.vmPullTimeHistogram, worker.dialer, worker.logger)

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
