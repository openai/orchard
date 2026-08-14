package synthetic

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/cirruslabs/orchard/internal/worker/ondiskname"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager/base"
	"github.com/cirruslabs/orchard/pkg/client"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type VM struct {
	onDiskName ondiskname.OnDiskName
	resource   v1.VM
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	logger     *zap.SugaredLogger

	*base.VM
}

func NewVM(
	vmResource v1.VM,
	eventStreamer *client.EventStreamer,
	vmPullTimeHistogram metric.Float64Histogram,
	logger *zap.SugaredLogger,
) *VM {
	ctx, cancel := context.WithCancel(context.Background())

	logger = logger.With(
		"vm_uid", vmResource.UID,
		"vm_name", vmResource.Name,
		"vm_restart_count", vmResource.RestartCount,
	)

	onDiskName := ondiskname.NewFromResource(vmResource)
	vm := &VM{
		onDiskName: onDiskName,
		resource:   vmResource,
		ctx:        ctx,
		cancel:     cancel,
		logger:     logger,

		VM: base.NewVM(vmResource, onDiskName, logger),
	}

	vm.wg.Add(1)

	go func() {
		defer vm.wg.Done()

		if vmResource.ImagePullPolicy == v1.ImagePullPolicyAlways {
			vm.SetStatusMessage("pulling VM image...")

			pullStartedAt := time.Now()

			// Pull
			time.Sleep(randomDelay())

			vmPullTimeHistogram.Record(vm.ctx, time.Since(pullStartedAt).Seconds(), metric.WithAttributes(
				attribute.String("worker", vm.resource.Worker),
				attribute.String("image", vm.resource.Image),
			))
		}

		// Clone and configure
		time.Sleep(randomDelay())

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
	return nil
}

func (vm *VM) OnDiskName() ondiskname.OnDiskName {
	return vm.onDiskName
}

func (vm *VM) ImageFQN() *string {
	return nil
}

func (vm *VM) Start(eventStreamer *client.EventStreamer) {
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

func (vm *VM) Suspend() <-chan error {
	vm.HostProcessSet().Stop()

	vm.EndpointSet().Stop()
	errChan := make(chan error, 1)

	errChan <- nil

	return errChan
}

func (vm *VM) IP(ctx context.Context) (string, error) {
	time.Sleep(randomDelay())

	return "127.0.0.1", nil
}

func (vm *VM) Stop() <-chan error {
	vm.HostProcessSet().Stop()

	vm.EndpointSet().Stop()
	errChan := make(chan error, 1)

	errChan <- nil

	return errChan
}

func (vm *VM) Delete() error {
	vm.cancel()

	if vm.ConditionsSet().Contains(v1.ConditionTypeCloning) {
		// Not cloned yet, nothing to delete
		return nil
	}

	time.Sleep(randomDelay())

	return nil
}

func (vm *VM) run(ctx context.Context, eventStreamer *client.EventStreamer) {
	defer vm.ConditionsSet().RemoveAll(v1.ConditionTypeRunning, v1.ConditionTypeSuspending, v1.ConditionTypeStopping)

	if err := vm.HostProcessSet().Start(ctx, vm.resource.HostProcesses); err != nil {
		vm.SetErr(fmt.Errorf("failed to start host processes: %w", err))

		return
	}
	defer vm.HostProcessSet().Stop()

	vm.EndpointSet().Start()
	defer vm.EndpointSet().Stop()

	// Launch the startup script goroutine as close as possible
	// to the VM startup (below) to avoid "tart ip" timing out
	if vm.resource.StartupScript != nil {
		vm.SetStatusMessage("VM started, running startup script...")

		go vm.runScript(vm.resource.StartupScript, eventStreamer)
	} else {
		vm.SetStatusMessage("VM started")
	}

	<-ctx.Done()
}

func (vm *VM) runScript(script *v1.VMScript, eventStreamer *client.EventStreamer) {
	if eventStreamer != nil {
		defer func() {
			if err := eventStreamer.Close(); err != nil {
				vm.logger.Errorf("errored during streaming events for startup script: %v", err)
			}
		}()
	}

	consumeLine := func(line string) {
		if eventStreamer == nil {
			return
		}

		eventStreamer.Stream(v1.Event{
			Kind:      v1.EventKindLogLine,
			Timestamp: time.Now().Unix(),
			Payload:   line,
		})
	}

	for line := range strings.Lines(script.ScriptContent) {
		consumeLine(line)

		randomDelay()
	}
}

func randomDelay() time.Duration {
	const jitterBaseDelay = 2500 * time.Millisecond

	return time.Duration(rand.Float64() * float64(jitterBaseDelay))
}
