package scheduler

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"sort"
	"time"

	"github.com/cirruslabs/orchard/internal/controller/lifecycle"
	"github.com/cirruslabs/orchard/internal/controller/notifier"
	storepkg "github.com/cirruslabs/orchard/internal/controller/store"
	"github.com/cirruslabs/orchard/internal/opentelemetry"
	"github.com/cirruslabs/orchard/internal/worker/ondiskname"
	"github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/cirruslabs/orchard/rpc"
	mapset "github.com/deckarep/golang-set/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

const (
	schedulerInterval = 5 * time.Second

	schedulerVMRestartDelay = 15 * time.Second
)

var (
	ErrVMSchedulingSkipped     = errors.New("scheduling skipped for VM")
	ErrWorkerSchedulingSkipped = errors.New("scheduling skipped for worker")
)

type Scheduler struct {
	store                storepkg.Store
	notifier             *notifier.Notifier
	workerOfflineTimeout time.Duration
	logger               *zap.SugaredLogger
	schedulingRequested  chan bool

	schedulingTimeHistogram metric.Float64Histogram
	workerStatusGauge       metric.Int64ObservableGauge
	vmStatusGauge           metric.Int64ObservableGauge
}

func NewScheduler(
	store storepkg.Store,
	notifier *notifier.Notifier,
	workerOfflineTimeout time.Duration,
	logger *zap.SugaredLogger,
) (*Scheduler, error) {
	scheduler := &Scheduler{
		store:                store,
		notifier:             notifier,
		workerOfflineTimeout: workerOfflineTimeout,
		logger:               logger,
		schedulingRequested:  make(chan bool, 1),
	}

	// Metrics
	var err error

	scheduler.workerStatusGauge, err = opentelemetry.DefaultMeter.Int64ObservableGauge(
		"org.cirruslabs.orchard.controller.scheduler.worker_status",
		metric.WithInt64Callback(scheduler.observeWorkerStatus),
	)
	if err != nil {
		return nil, err
	}

	scheduler.vmStatusGauge, err = opentelemetry.DefaultMeter.Int64ObservableGauge(
		"org.cirruslabs.orchard.controller.scheduler.vm_status",
		metric.WithInt64Callback(scheduler.observeVMStatus),
	)
	if err != nil {
		return nil, err
	}

	scheduler.schedulingTimeHistogram, err = opentelemetry.DefaultMeter.
		Float64Histogram("org.cirruslabs.orchard.controller.scheduling_time")
	if err != nil {
		return nil, err
	}

	return scheduler, nil
}

func (scheduler *Scheduler) Run() {
	for {
		// wait either the scheduling interval or a request to schedule
		select {
		case <-scheduler.schedulingRequested:
		case <-time.After(schedulerInterval):
		}

		healthCheckingLoopIterationStart := time.Now()
		numVMsHealth, err := scheduler.healthCheckingLoopIteration()
		healthCheckingLoopIterationEnd := time.Now()
		if err != nil {
			scheduler.logger.Errorf("Failed to health-check VMs: %v", err)
		}

		schedulingLoopIterationStart := time.Now()
		numWorkersScheduling, numVMsScheduling, err := scheduler.schedulingLoopIteration()
		schedulingLoopIterationEnd := time.Now()

		scheduler.logger.Debugf("Health checking loop iteration for %d VMs took %v, "+
			"scheduling loop iteration for %d workers and %d VMs took %v",
			numVMsHealth, healthCheckingLoopIterationEnd.Sub(healthCheckingLoopIterationStart),
			numWorkersScheduling, numVMsScheduling,
			schedulingLoopIterationEnd.Sub(schedulingLoopIterationStart))

		if err != nil {
			scheduler.logger.Errorf("Failed to schedule VMs: %v", err)
		}
	}
}

func (scheduler *Scheduler) observeWorkerStatus(_ context.Context, observer metric.Int64Observer) error {
	return scheduler.store.View(func(txn storepkg.Transaction) error {
		workers, err := txn.ListWorkers()
		if err != nil {
			return err
		}

		for _, worker := range workers {
			var online, offline int64

			if worker.Offline(scheduler.workerOfflineTimeout) {
				offline = 1
			} else {
				online = 1
			}

			observer.Observe(online, metric.WithAttributes(attribute.String("worker", worker.Name),
				attribute.String("status", "online")))
			observer.Observe(offline, metric.WithAttributes(attribute.String("worker", worker.Name),
				attribute.String("status", "offline")))
		}

		return nil
	})
}

func (scheduler *Scheduler) observeVMStatus(_ context.Context, observer metric.Int64Observer) error {
	return scheduler.store.View(func(txn storepkg.Transaction) error {
		vms, err := txn.ListVMs()
		if err != nil {
			return err
		}

		vmStatusCounts := map[v1.VMStatus]int64{}
		for _, vm := range vms {
			vmStatusCounts[vm.Status]++
		}

		for status, count := range vmStatusCounts {
			observer.Observe(count, metric.WithAttributes(attribute.String("status", status.String())))
		}

		return nil
	})
}

func (scheduler *Scheduler) RequestScheduling() {
	select {
	case scheduler.schedulingRequested <- true:
		scheduler.logger.Debugf("Successfully requested scheduling")
	default:
		scheduler.logger.Debugf("There's already a scheduling request in the queue, skipping")
	}
}

//nolint:gocognit,gocyclo // this logic could be seen as even more complex if split into multiple functions
func (scheduler *Scheduler) schedulingLoopIteration() (int, int, error) {
	affectedWorkers := mapset.NewSet[string]()

	// Scheduler consistency model is based on the following:
	//
	// EXCLUSIVENESS:
	//
	// Only one scheduler might operate in a cluster at any given time.
	//
	// Currently, this is achieved automatically since we run in-process
	// BadgerDB that runs in the Orchard Controller, and Orchard Controller
	// in turn runs a single scheduler.
	//
	// In the future, we might support etcd, and in that case leader
	// election can be implemented to ensure this property, thanks to
	// etcd leases[1].
	//
	// [1]: https://medium.com/@ahadrana/understanding-etcd3-8784c4f61755
	//
	// OVERESTIMATION OF USED RESOURCES:
	//
	// Scheduler acts opportunistically on a lagging view of resource
	// usage in the cluster.
	//
	// This means that we won't assign more than what a worker can handle,
	// but we might skip the worker from the consideration, even through
	// in reality it can already handle the VM we're scheduling, and assign
	// the VM to a next worker, thus slightly violating the scheduler
	// profile at times.
	//
	// LAGGING SCHEDULER PROFILE:
	//
	// In case the scheduler profile is changed amidst the scheduling loop
	// iteration, we'll act on a previously set scheduler profile.
	//
	// It feels that this is totally fine, assuming that (1) the scheduler
	// profile is not something that's changed frequently and that (2) at
	// the same time when the scheduler profile is changed, a user won't
	// schedule a bunch of VMs in the hope that they'll be serviced using
	// that new scheduling profile.

	var vms []v1.VM
	var workers []v1.Worker
	var schedulerProfile v1.SchedulerProfile

	if err := scheduler.store.View(func(txn storepkg.Transaction) error {
		var err error

		vms, err = txn.ListVMs()
		if err != nil {
			return err
		}

		workers, err = txn.ListWorkers()
		if err != nil {
			return err
		}

		clusterSettings, err := txn.GetClusterSettings()
		if err != nil {
			return err
		}
		schedulerProfile = clusterSettings.SchedulerProfile

		return nil
	}); err != nil {
		return 0, 0, err
	}

	unscheduledVMs, workerInfos := ProcessVMs(vms)

NextVM:
	for _, unscheduledVM := range unscheduledVMs {
		// Order workers depending on the scheduler profile and
		// our updated lagging resource usage for each worker
		switch schedulerProfile {
		case v1.SchedulerProfileDistributeLoad:
			slices.SortFunc(workers, func(a, b v1.Worker) int {
				// Sort by the number of running VMs, ascending order
				return cmp.Compare(workerInfos[a.Name].NumRunningVMs,
					workerInfos[b.Name].NumRunningVMs)
			})
		case v1.SchedulerProfileOptimizeUtilization:
			fallthrough
		default:
			slices.SortFunc(workers, func(a, b v1.Worker) int {
				// Sort by the number of running VMs, descending order
				return cmp.Compare(workerInfos[b.Name].NumRunningVMs,
					workerInfos[a.Name].NumRunningVMs)
			})
		}

		// Iterate through sorted workers and find a worker that can run this VM
	NextWorker:
		for _, worker := range workers {
			resourcesUsed := workerInfos.Get(worker.Name).ResourcesUsed
			resourcesRemaining := worker.Resources.Subtracted(resourcesUsed)

			if worker.Offline(scheduler.workerOfflineTimeout) ||
				worker.SchedulingPaused ||
				!compatibleArchAndRuntime(unscheduledVM, worker) ||
				!resourcesRemaining.CanFit(unscheduledVM.Resources) ||
				!worker.Labels.Contains(unscheduledVM.Labels) {
				continue NextWorker
			}

			// Don't schedule VMs with endpoints on workers that don't support them
			if len(unscheduledVM.Endpoints) != 0 &&
				!worker.Capabilities.Has(v1.WorkerCapabilityVMEndpoints) {
				continue NextWorker
			}

			err := scheduler.store.Update(func(txn storepkg.Transaction) error {
				currentUnscheduledVM, err := txn.GetVM(unscheduledVM.Name)
				if err != nil {
					if errors.Is(err, storepkg.ErrNotFound) {
						// The unscheduled VM ceased to exist,
						// so nothing to schedule
						return ErrVMSchedulingSkipped
					}

					return err
				}

				if currentUnscheduledVM.UID != unscheduledVM.UID ||
					currentUnscheduledVM.Generation != unscheduledVM.Generation {
					// The unscheduled VM had changed, so we'll re-evaluate a new
					// version of it in the next scheduling loop iteration
					return ErrVMSchedulingSkipped
				}

				if unscheduledVM.IsScheduled() {
					// Unscheduled VM is not unscheduled anymore,
					// so there's nothing to do
					return ErrVMSchedulingSkipped
				}

				if unscheduledVM.TerminalState() {
					// We don't support re-scheduling of VMs in terminal state at the moment
					return ErrVMSchedulingSkipped
				}

				if unscheduledVM.PowerState.TerminalState() {
					// We don't support re-scheduling of stopped/suspended VMs at the moment
					return ErrVMSchedulingSkipped
				}

				currentWorker, err := txn.GetWorker(worker.Name)
				if err != nil {
					if errors.Is(err, storepkg.ErrNotFound) {
						// The worker that we were planning to schedule
						// this VM on has ceased to exist, so move on
						return ErrWorkerSchedulingSkipped
					}

					return err
				}

				if currentWorker.Offline(scheduler.workerOfflineTimeout) ||
					currentWorker.SchedulingPaused {
					return ErrWorkerSchedulingSkipped
				}

				if !compatibleArchAndRuntime(unscheduledVM, *currentWorker) {
					return ErrWorkerSchedulingSkipped
				}

				// Don't schedule VMs with endpoints on workers that don't support them
				if len(unscheduledVM.Endpoints) != 0 &&
					!currentWorker.Capabilities.Has(v1.WorkerCapabilityVMEndpoints) {
					return ErrWorkerSchedulingSkipped
				}

				if currentWorker.MachineID != worker.MachineID ||
					!currentWorker.Resources.Equal(worker.Resources) {
					// Worker has changed
					return ErrWorkerSchedulingSkipped
				}

				unscheduledVM.Worker = worker.Name
				unscheduledVM.ScheduledAt = time.Now()
				v1.ConditionsSet(&unscheduledVM.Conditions, v1.Condition{
					Type:  v1.ConditionTypeScheduled,
					State: v1.ConditionStateTrue,
				})

				// Fill out the actual CPU allocation
				if unscheduledVM.CPU == 0 {
					// Provide defaults for VMs with implicit CPU specification
					if worker.DefaultCPU != 0 {
						unscheduledVM.AssignedCPU = worker.DefaultCPU
					} else {
						unscheduledVM.AssignedCPU = 4
					}
				} else {
					unscheduledVM.AssignedCPU = unscheduledVM.CPU
				}

				// Fill out the actual memory allocation
				if unscheduledVM.Memory == 0 {
					// Provide defaults for VMs with implicit memory specification
					if worker.DefaultMemory != 0 {
						unscheduledVM.AssignedMemory = worker.DefaultMemory
					} else {
						unscheduledVM.AssignedMemory = 8192
					}
				} else {
					unscheduledVM.AssignedMemory = unscheduledVM.Memory
				}

				if err := txn.SetVM(unscheduledVM); err != nil {
					return err
				}

				return nil
			})
			if err != nil {
				if errors.Is(err, ErrVMSchedulingSkipped) {
					continue NextVM
				}

				if errors.Is(err, ErrWorkerSchedulingSkipped) {
					continue NextWorker
				}

				return 0, 0, err
			}

			// Update lagging resource usage
			workerInfos.AddVM(worker.Name, unscheduledVM.Resources)

			// Ping the worker afterward for faster VM execution
			affectedWorkers.Add(worker.Name)

			// Update metrics
			scheduler.schedulingTimeHistogram.Record(context.Background(),
				time.Since(unscheduledVM.CreatedAt).Seconds())

			break
		}
	}

	for affectedWorker := range affectedWorkers.Iter() {
		// It's fine to not treat the error as fatal here,
		// since the worker will sync the VMs on the next
		// scheduling iteration
		notifyContext, notifyContextCancel := context.WithTimeout(context.Background(), time.Second)
		if err := scheduler.notifier.Notify(notifyContext, affectedWorker, &rpc.WatchInstruction{
			Action: &rpc.WatchInstruction_SyncVmsAction{},
		}); err != nil {
			scheduler.logger.Errorf("Failed to reactively sync VMs on worker %s: %v", affectedWorker, err)
		}
		notifyContextCancel()
	}

	return len(workers), len(vms), nil
}

func ProcessVMs(vms []v1.VM) ([]v1.VM, WorkerInfos) {
	var unscheduledVMs []v1.VM
	workerToResources := make(WorkerInfos)

	for _, vm := range vms {
		if vm.IsScheduled() {
			workerToResources.AddVM(vm.Worker, vm.Resources)
		} else {
			unscheduledVMs = append(unscheduledVMs, vm)
		}
	}

	// Sort unscheduled VMs by the date of creation
	sort.Slice(unscheduledVMs, func(i, j int) bool {
		return unscheduledVMs[i].CreatedAt.Before(unscheduledVMs[j].CreatedAt)
	})

	return unscheduledVMs, workerToResources
}

func compatibleArchAndRuntime(vm v1.VM, worker v1.Worker) bool {
	return vm.Arch == worker.Arch && vm.Runtime == worker.Runtime
}

func (scheduler *Scheduler) healthCheckingLoopIteration() (int, error) {
	// Stats for the caller
	var numVMs int

	// Get a lagging view of VMs
	var vms []v1.VM

	if err := scheduler.store.View(func(txn storepkg.Transaction) error {
		var err error

		vms, err = txn.ListVMs()
		if err != nil {
			return err
		}
		numVMs = len(vms)

		return nil
	}); err != nil {
		return 0, err
	}

	// Process each VM in a lagging list of VMs in an individual
	// transaction, re-checking that the VM still exists
	// and it is still scheduled
	for _, vm := range vms {
		if !vm.IsScheduled() {
			// Not a scheduled VM
			//
			// We'll re-check this below, but this allows us
			// to avoid wasting cycles opening a transaction
			// for nothing.
			continue
		}

		if err := scheduler.store.Update(func(txn storepkg.Transaction) error {
			currentVM, err := txn.GetVM(vm.Name)
			if err != nil {
				if errors.Is(err, storepkg.ErrNotFound) {
					// VM ceased to exist, nothing to do
					return nil
				}

				return err
			}

			if !vm.IsScheduled() {
				// Not a scheduled VM, nothing to do
				return nil
			}

			return scheduler.healthCheckVM(txn, *currentVM)
		}); err != nil {
			return 0, err
		}
	}

	return numVMs, nil
}

func (scheduler *Scheduler) healthCheckVM(txn storepkg.Transaction, vm v1.VM) error {
	logger := scheduler.logger.With("vm_name", vm.Name, "vm_uid", vm.UID, "vm_restart_count", vm.RestartCount)

	// Preserve the current endpoint observations for comparison below and reset them,
	// so every VM transition persisted below discards the stale endpoint observations
	savedObservedEndpoints := vm.ObservedEndpoints
	vm.ObservedEndpoints = nil

	// Schedule a VM restart if the restart policy mandates it
	needsRestart := vm.RestartPolicy == v1.RestartPolicyOnFailure &&
		vm.Status == v1.VMStatusFailed &&
		time.Since(vm.RestartedAt) > schedulerVMRestartDelay

	if needsRestart {
		logger.Debugf("restarting VM")

		lifecycle.Report(&vm, "VM restarted", scheduler.logger)

		vm.Status = v1.VMStatusPending
		vm.StatusMessage = ""
		vm.Worker = ""
		vm.AssignedCPU = 0
		vm.AssignedMemory = 0
		vm.RestartedAt = time.Now()
		vm.RestartCount++
		vm.ScheduledAt = time.Time{}
		vm.StartedAt = time.Time{}
		vm.PowerState = v1.PowerStateRunning
		vm.LocalName = ondiskname.New(vm.Name, vm.UID, vm.RestartCount).String()
		//nolint:staticcheck // yes, this is deprecated, but we still maintain it for backward compatibility
		vm.TartName = vm.LocalName
		vm.Conditions = []v1.Condition{
			{
				Type:  v1.ConditionTypeScheduled,
				State: v1.ConditionStateFalse,
			},
		}

		return txn.SetVM(vm)
	}

	worker, err := txn.GetWorker(vm.Worker)
	if err != nil {
		if errors.Is(err, storepkg.ErrNotFound) {
			vm.Status = v1.VMStatusFailed
			vm.StatusMessage = "VM is assigned to a worker that " +
				"doesn't exist anymore"

			return txn.SetVM(vm)
		}

		return err
	}

	if worker.Offline(scheduler.workerOfflineTimeout) && !vm.TerminalState() {
		vm.Status = v1.VMStatusFailed
		vm.StatusMessage = "VM is assigned to a worker that " +
			"lost connection with the controller"

		return txn.SetVM(vm)
	}

	if vm.PowerState.TerminalState() && v1.ConditionIsFalse(vm.Conditions, v1.ConditionTypeRunning) {
		// VM has entered a terminal power state and stopped running,
		// de-schedule it to free up resources
		v1.ConditionsSet(&vm.Conditions, v1.Condition{
			Type:  v1.ConditionTypeScheduled,
			State: v1.ConditionStateFalse,
		})

		return txn.SetVM(vm)
	}

	if vm.TerminalState() {
		// VM has entered a terminal state,
		// de-schedule it to free up resources
		v1.ConditionsSet(&vm.Conditions, v1.Condition{
			Type:  v1.ConditionTypeScheduled,
			State: v1.ConditionStateFalse,
		})

		// Also correct the conditions for the worker
		v1.ConditionsSet(&vm.Conditions, v1.Condition{
			Type:  v1.ConditionTypeRunning,
			State: v1.ConditionStateFalse,
		})

		return txn.SetVM(vm)
	}

	// Report unsupported endpoints to clients without failing the VM
	if !worker.Capabilities.Has(v1.WorkerCapabilityVMEndpoints) {
		vm.ObservedEndpoints = make([]v1.EndpointStatus, 0, len(vm.Endpoints))

		for _, endpoint := range vm.Endpoints {
			vm.ObservedEndpoints = append(vm.ObservedEndpoints, v1.EndpointStatus{
				Name:     endpoint.Name,
				Protocol: endpoint.Protocol,
				State:    v1.EndpointStateError,
				Message:  "worker doesn't support VM endpoints",
			})
		}

		if slices.Equal(savedObservedEndpoints, vm.ObservedEndpoints) {
			return nil
		}

		return txn.SetVM(vm)
	}

	return nil
}
