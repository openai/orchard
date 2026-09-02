package vetu

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cirruslabs/orchard/internal/dialer"
	"github.com/cirruslabs/orchard/internal/worker/ondiskname"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager/base"
	"github.com/cirruslabs/orchard/pkg/client"
	"github.com/cirruslabs/orchard/pkg/resource/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type VM struct {
	onDiskName ondiskname.OnDiskName
	resource   v1.VM
	logger     *zap.SugaredLogger

	// Image FQN feature, see https://github.com/cirruslabs/orchard/issues/164
	imageFQN atomic.Pointer[string]

	ctx    context.Context
	cancel context.CancelFunc

	wg *sync.WaitGroup

	stopMtx  sync.Mutex
	stopDone chan error

	dialer dialer.Dialer

	*base.VM
}

func NewVM(
	vmResource v1.VM,
	eventStreamer *client.EventStreamer,
	vmPullTimeHistogram metric.Float64Histogram,
	dialer dialer.Dialer,
	logger *zap.SugaredLogger,
) *VM {
	vmContext, vmContextCancel := context.WithCancel(context.Background())

	vm := &VM{
		onDiskName: ondiskname.NewFromResource(vmResource),
		resource:   vmResource,
		logger: logger.With(
			"vm_uid", vmResource.UID,
			"vm_name", vmResource.Name,
			"vm_restart_count", vmResource.RestartCount,
		),

		ctx:    vmContext,
		cancel: vmContextCancel,

		wg: &sync.WaitGroup{},

		dialer: dialer,

		VM: base.NewVM(logger),
	}

	vm.wg.Add(1)

	go func() {
		defer vm.wg.Done()

		if vmResource.ImagePullPolicy == v1.ImagePullPolicyAlways {
			vm.SetStatusMessage("pulling VM image...")

			pullStartedAt := time.Now()

			_, _, err := Vetu(vm.ctx, vm.logger, "pull", vm.resource.Image)
			if err != nil {
				select {
				case <-vm.ctx.Done():
					// Do not return an error because it's the user's intent to cancel this VM operation
				default:
					vm.SetErr(fmt.Errorf("failed to pull the VM: %w", err))
				}

				return
			}

			vmPullTimeHistogram.Record(vm.ctx, time.Since(pullStartedAt).Seconds(), metric.WithAttributes(
				attribute.String("worker", vm.resource.Worker),
				attribute.String("image", vm.resource.Image),
			))
		}

		if err := vm.cloneAndConfigure(vm.ctx); err != nil {
			select {
			case <-vm.ctx.Done():
				// Do not return an error because it's the user's intent to cancel this VM operation
			default:
				vm.SetErr(fmt.Errorf("failed to clone the VM: %w", err))
			}

			return
		}

		// Backward compatibility with v1.VM specification's "Status" field
		vm.SetStarted(true)

		vm.ConditionsSet().Add(v1.ConditionTypeRunning)

		vm.run(vm.ctx, eventStreamer)
	}()

	return vm
}

func (vm *VM) Resource() v1.VM {
	return vm.resource
}

func (vm *VM) SetResource(vmResource v1.VM) {
	vm.resource = vmResource
	vm.resource.ObservedGeneration = vmResource.Generation
}

func (vm *VM) UpdateSoftnetPolicy(context.Context, []string, []string) error {
	return errors.ErrUnsupported
}

func (vm *VM) OnDiskName() ondiskname.OnDiskName {
	return vm.onDiskName
}

func (vm *VM) ImageFQN() *string {
	return vm.imageFQN.Load()
}

func (vm *VM) id() string {
	return vm.onDiskName.String()
}

func (vm *VM) cloneAndConfigure(ctx context.Context) error {
	vm.SetStatusMessage("cloning VM...")

	_, _, err := Vetu(ctx, vm.logger, "clone", vm.resource.Image, vm.id())
	if err != nil {
		return err
	}

	vm.ConditionsSet().Remove(v1.ConditionTypeCloning)

	// Image FQN feature, see https://github.com/cirruslabs/orchard/issues/164
	fqnRaw, _, err := Vetu(ctx, vm.logger, "fqn", vm.resource.Image)
	if err == nil {
		fqn := strings.TrimSpace(fqnRaw)
		vm.imageFQN.Store(&fqn)
	}

	// Set memory
	vm.SetStatusMessage("configuring VM...")

	memory := vm.resource.AssignedMemory

	if memory == 0 {
		memory = vm.resource.Memory
	}

	if memory != 0 {
		_, _, err = Vetu(ctx, vm.logger, "set", "--memory",
			strconv.FormatUint(memory, 10), vm.id())
		if err != nil {
			return err
		}
	}

	// Set CPU
	cpu := vm.resource.AssignedCPU

	if cpu == 0 {
		cpu = vm.resource.CPU
	}

	if cpu != 0 {
		_, _, err = Vetu(ctx, vm.logger, "set", "--cpu",
			strconv.FormatUint(cpu, 10), vm.id())
		if err != nil {
			return err
		}
	}

	if diskSize := vm.resource.DiskSize; diskSize != 0 {
		_, _, err = Vetu(ctx, vm.logger, "set", "--disk-size",
			strconv.FormatUint(diskSize, 10), vm.id())
		if err != nil {
			return err
		}
	}

	return nil
}

func (vm *VM) run(ctx context.Context, eventStreamer *client.EventStreamer) {
	// Stop owns Stopping until both its command and this goroutine finish.
	defer vm.ConditionsSet().RemoveAll(v1.ConditionTypeRunning, v1.ConditionTypeSuspending)

	// Launch the startup script goroutine as close as possible
	// to the VM startup (below) to avoid "vetu ip" timing out
	if vm.resource.StartupScript != nil {
		vm.SetStatusMessage("VM started, running startup script...")

		go vm.RunScript(vm.ctx, vm.resource.Username, vm.resource.Password, vm.resource.StartupScript,
			eventStreamer, vm.dialer, vm.IP)
	} else {
		vm.SetStatusMessage("VM started")
	}

	var runArgs = []string{"run"}

	runArgs = append(runArgs, vm.id())
	_, _, err := Vetu(ctx, vm.logger, runArgs...)
	if err != nil {
		select {
		case <-vm.ctx.Done():
			// Do not return an error because it's the user's intent to cancel this VM
		default:
			vm.SetErr(fmt.Errorf("%w: %v", base.ErrVMFailed, err))
		}

		return
	}

	select {
	case <-vm.ctx.Done():
		// Do not return an error because it's the user's intent to cancel this VM
	default:
		if !vm.ConditionsSet().ContainsAny(v1.ConditionTypeSuspending, v1.ConditionTypeStopping) {
			vm.SetErr(fmt.Errorf("%w: VM exited unexpectedly", base.ErrVMFailed))
		}
	}
}

func (vm *VM) IP(ctx context.Context) (string, error) {
	args := []string{"ip", "--wait", "60"}

	args = append(args, vm.id())

	stdout, _, err := Vetu(ctx, vm.logger, args...)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(stdout), nil
}

func (vm *VM) Suspend() <-chan error {
	errCh := make(chan error, 1)

	errCh <- fmt.Errorf("suspending Vetu VMs is not supported at the moment")

	return errCh
}

func (vm *VM) Stop() <-chan error {
	vm.stopMtx.Lock()
	defer vm.stopMtx.Unlock()
	if vm.stopDone != nil {
		return vm.stopDone
	}

	done := make(chan error)
	vm.stopDone = done
	ctx, cancel, wg := vm.ctx, vm.cancel, vm.wg
	vm.SetStatusMessage("Stopping VM")
	vm.ConditionsSet().Add(v1.ConditionTypeStopping)

	go func() {
		if ctx.Err() == nil {
			// Try to gracefully terminate the VM.
			_, _, _ = Vetu(context.WithoutCancel(ctx), zap.NewNop().Sugar(), "stop", "--timeout", "5", vm.id())
		}

		// Cancellation requests shutdown; it does not establish completion.
		cancel()
		wg.Wait()

		vm.stopMtx.Lock()
		vm.ConditionsSet().Remove(v1.ConditionTypeStopping)
		// Closing broadcasts successful completion to every Stop caller.
		close(done)
		vm.stopMtx.Unlock()
	}()

	return done
}

func (vm *VM) Start(eventStreamer *client.EventStreamer) {
	// The worker defers Start while Stopping is true.
	vm.stopMtx.Lock()
	defer vm.stopMtx.Unlock()
	vm.stopDone = nil

	vm.SetStatusMessage("Starting VM")
	vm.ConditionsSet().Add(v1.ConditionTypeRunning)

	vm.cancel()

	vm.ctx, vm.cancel = context.WithCancel(context.Background())
	vm.wg.Add(1)

	go func() {
		defer vm.wg.Done()

		vm.run(vm.ctx, eventStreamer)
	}()
}

func (vm *VM) Delete() error {
	// Cancel all currently running Vetu invocations
	// (e.g. "vetu clone", "vetu run", etc.)
	vm.cancel()

	if vm.ConditionsSet().Contains(v1.ConditionTypeCloning) {
		// Not cloned yet, nothing to delete
		return nil
	}

	_, _, err := Vetu(context.Background(), vm.logger, "delete", vm.id())
	if err != nil {
		return fmt.Errorf("%w: failed to delete VM: %v", base.ErrVMFailed, err)
	}

	return nil
}
