package worker

import (
	"context"
	"fmt"
	"net"

	"github.com/cirruslabs/orchard/internal/proxy"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/samber/lo"
)

func (worker *Worker) watchRPCV2(ctx context.Context, operationCtx context.Context, onEstablished func()) error {
	watchInstructionCh, watchErrCh, err := worker.client.RPC().Watch(ctx, worker.name)
	if err != nil {
		return err
	}
	onEstablished()

	for {
		select {
		case watchInstruction := <-watchInstructionCh:
			if portForwardAction := watchInstruction.PortForwardAction; portForwardAction != nil {
				go worker.handlePortForwardV2(operationCtx, portForwardAction)
			} else if syncVMsAction := watchInstruction.SyncVMsAction; syncVMsAction != nil {
				worker.requestVMSyncing()
			} else if resolveIPAction := watchInstruction.ResolveIPAction; resolveIPAction != nil {
				go worker.handleGetIPV2(operationCtx, resolveIPAction)
			}
		case watchErr := <-watchErrCh:
			return watchErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (worker *Worker) handlePortForwardV2(ctx context.Context, portForward *v1.PortForwardAction) {
	var errorMessage string

	worker.logger.Debugf("received port-forwarding request to VM UID %s, port %d",
		portForward.VMUID, portForward.Port)

	// Establish a connection with the VM
	vmConn, err := worker.handlePortForwardV2Inner(ctx, portForward)
	if err != nil {
		errorMessage = fmt.Sprintf("port-forwarding failed: %v", err)

		worker.logger.Warn(errorMessage)
	}

	// Respond
	netConn, err := worker.client.RPC().RespondPortForward(ctx, portForward.Session, errorMessage)
	if err != nil {
		worker.logger.Warnf("port forwarding failed: failed to call API: %v", err)

		return
	}
	defer func() {
		// Ensure that we always close the accepted WebSocket connection,
		// otherwise resource leak is possible[1]
		//
		// [1]: https://github.com/coder/websocket/issues/445#issuecomment-2053792044
		_ = netConn.Close()
	}()

	// Proxy bytes if the connection was established without errors
	if errorMessage == "" {
		_ = proxy.Connections(vmConn, netConn)
	}
}

//nolint:err113,perfsprint // Preserve the original host-process forwarding errors.
func (worker *Worker) handlePortForwardV2Inner(
	ctx context.Context,
	portForward *v1.PortForwardAction,
) (net.Conn, error) {
	if target := portForward.Target; target != nil {
		// Sanity check
		if portForward.VMUID != "" || portForward.Port != 0 {
			return nil, fmt.Errorf("target and legacy fields are mutually exclusive")
		}

		// Retrieve the typed host process target, it's the only possible target right now
		if target.HostProcess == nil || target.HostProcess.VMUID == "" || target.HostProcess.Name == "" {
			return nil, fmt.Errorf("invalid or unsupported target")
		}

		// Dial host process
		return worker.dialHostProcess(ctx, target.HostProcess.VMUID, target.HostProcess.Name)
	}

	var host string
	var err error

	if portForward.VMUID == "" {
		// Port-forwarding request to a worker
		host = "localhost"
	} else {
		// Port-forwarding request to a VM, find that VM
		vm, ok := lo.Find(worker.vmm.List(), func(item vmmanager.VM) bool {
			return item.Resource().UID == portForward.VMUID
		})
		if !ok {
			return nil, fmt.Errorf("failed to get VM with UID %q", portForward.VMUID)
		}

		// Obtain VM's IP address
		host, err = vm.IP(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get VM's IP: %v", err)
		}
	}

	// Connect to the VM's port

	var vmConn net.Conn

	if worker.dialer != nil {
		vmConn, err = worker.dialer.DialContext(ctx, "tcp",
			fmt.Sprintf("%s:%d", host, portForward.Port))
	} else {
		dialer := net.Dialer{}

		vmConn, err = dialer.DialContext(ctx, "tcp",
			fmt.Sprintf("%s:%d", host, portForward.Port))
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to the VM: %v", err)
	}

	return vmConn, nil
}

func (worker *Worker) handleGetIPV2(ctx context.Context, resolveIP *v1.ResolveIPAction) {
	var errorMessage string

	worker.logger.Debugf("received IP resolution request to VM UID %s", resolveIP.VMUID)

	// Retrieve the VM's IP
	ip, err := worker.handleGetIPV2Inner(ctx, resolveIP)
	if err != nil {
		errorMessage = fmt.Sprintf("failed to resolve VM's IP: %v", err)

		worker.logger.Warn(errorMessage)
	}

	// Report results
	if err := worker.client.RPC().RespondIP(ctx, resolveIP.Session, ip, errorMessage); err != nil {
		worker.logger.Warnf("failed to resolve IP for the VM with UID %q: "+
			"failed to call back to the controller: %v", resolveIP.VMUID, err)

		return
	}
}

func (worker *Worker) handleGetIPV2Inner(
	ctx context.Context,
	resolveIP *v1.ResolveIPAction,
) (string, error) {
	// Find the desired VM
	vm, ok := lo.Find(worker.vmm.List(), func(item vmmanager.VM) bool {
		return item.Resource().UID == resolveIP.VMUID
	})
	if !ok {
		return "", fmt.Errorf("VM %q not found", resolveIP.VMUID)
	}

	// Obtain VM's IP address
	ip, err := vm.IP(ctx)
	if err != nil {
		return "", fmt.Errorf("\"tart ip\" failed for VM %q: %v", resolveIP.VMUID, err)
	}

	return ip, nil
}

//nolint:err113,ireturn // Preserve the original VM lookup helper required by host processes.
func (worker *Worker) findVMByUID(uid string) (vmmanager.VM, error) {
	vm, ok := lo.Find(worker.vmm.List(), func(item vmmanager.VM) bool {
		return item.Resource().UID == uid
	})
	if !ok {
		return nil, fmt.Errorf("VM with UID %q not found", uid)
	}

	if !vm.Started() {
		return nil, fmt.Errorf("VM with UID %q is not running", uid)
	}

	return vm, nil
}

func (worker *Worker) dialHostProcess(ctx context.Context, vmUID string, name string) (net.Conn, error) {
	vm, err := worker.findVMByUID(vmUID)
	if err != nil {
		return nil, err
	}

	return vm.HostProcessSet().Dial(ctx, name)
}
