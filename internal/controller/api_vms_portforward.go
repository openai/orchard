package controller

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/avast/retry-go/v5"
	storepkg "github.com/cirruslabs/orchard/internal/controller/store"
	"github.com/cirruslabs/orchard/internal/netconncancel"
	"github.com/cirruslabs/orchard/internal/proxy"
	"github.com/cirruslabs/orchard/internal/responder"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/cirruslabs/orchard/rpc"
	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errPortForwardRequest = errors.New("failed to request port forwarding")

func (controller *Controller) portForwardVM(ctx *gin.Context) responder.Responder {
	if responder := controller.authorizeAny(ctx, v1.ServiceAccountRoleComputeWrite,
		v1.ServiceAccountRoleComputeConnect); responder != nil {
		return responder
	}

	// Retrieve and parse path and query parameters
	name := ctx.Param("name")
	portRaw := ctx.Query("port")
	hostProcess := ctx.Query("hostProcess")

	// Host process connections require an additional role
	var port uint64
	var err error

	if hostProcess != "" {
		if responder := controller.authorizeAny(ctx, v1.ServiceAccountRoleHostProcessWrite,
			v1.ServiceAccountRoleHostProcessConnect); responder != nil {
			return responder
		}

		// Host process forwarding cannot also target a VM port
		if portRaw != "" {
			return responder.Code(http.StatusBadRequest)
		}
	} else {
		// VM port forwarding requires a valid non-zero TCP port
		port, err = strconv.ParseUint(portRaw, 10, 16)
		if err != nil || port < 1 || port > 65535 {
			return responder.Code(http.StatusBadRequest)
		}
	}

	waitRaw := ctx.DefaultQuery("wait", "10")
	wait, err := strconv.ParseUint(waitRaw, 10, 16)
	if err != nil {
		return responder.Code(http.StatusBadRequest)
	}

	// Look-up the VM
	waitContext, waitContextCancel := context.WithTimeout(ctx, time.Duration(wait)*time.Second)
	defer waitContextCancel()

	vm, responderImpl := controller.waitForVM(waitContext, name)
	if responderImpl != nil {
		return responderImpl
	}

	// Verify that the requested host process is declared on the VM.
	if hostProcess != "" && !lo.SomeBy(vm.HostProcesses, func(process v1.HostProcess) bool {
		return process.Name == hostProcess
	}) {
		return responder.JSON(http.StatusNotFound,
			NewErrorResponse("host process %q is not declared on VM %q", hostProcess, vm.Name))
	}

	// Commence port forwarding
	return controller.portForward(ctx, waitContext, vm.Worker, vm.UID, uint32(port), hostProcess)
}

func (controller *Controller) portForward(
	ctx *gin.Context,
	notifyContext context.Context,
	workerName string,
	vmUID string,
	port uint32,
	hostProcess string,
) responder.Responder {
	// Request and wait for a connection with a worker
	rendezvousConn, err := retry.NewWithData[net.Conn](
		retry.Context(notifyContext),
		retry.DelayType(retry.FixedDelay),
		retry.Delay(time.Second),
		retry.Attempts(0),
		retry.LastErrorOnly(true),
	).Do(func() (net.Conn, error) {
		return controller.portForwardConnection(ctx, notifyContext, workerName, vmUID, port, hostProcess)
	})
	if err != nil {
		if errors.Is(err, errPortForwardRequest) {
			controller.logger.Warnf("failed to request port forwarding from the worker %s: %v",
				workerName, err)

			return responder.Code(http.StatusServiceUnavailable)
		}

		return responder.Error(err)
	}
	defer func() {
		_ = rendezvousConn.Close()
	}()

	// Worker will asynchronously start port forwarding, so we wait
	wsConn, err := websocket.Accept(ctx.Writer, ctx.Request, &websocket.AcceptOptions{
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		return responder.Error(err)
	}
	defer func() {
		// Ensure that we always close the accepted WebSocket connection,
		// otherwise resource leak is possible[1]
		//
		// [1]: https://github.com/coder/websocket/issues/445#issuecomment-2053792044
		_ = wsConn.CloseNow()
	}()

	expectedMsgType := websocket.MessageBinary

	// Backwards compatibility with older Orchard clients
	// using "golang.org/x/net/websocket" package
	if ctx.Request.Header.Get("User-Agent") == "" {
		expectedMsgType = websocket.MessageText
	}

	proxyConnectionsErrCh := make(chan error, 1)
	wsConnAsNetConn := websocket.NetConn(ctx, wsConn, expectedMsgType)

	go func() {
		proxyConnectionsErrCh <- proxy.Connections(wsConnAsNetConn, rendezvousConn)
	}()

	for {
		select {
		case err := <-proxyConnectionsErrCh:
			if err != nil {
				var websocketCloseError websocket.CloseError

				// Normal closure from the user
				if errors.As(err, &websocketCloseError) && websocketCloseError.Code == websocket.StatusNormalClosure {
					return responder.Empty()
				}

				if errors.Is(err, context.Canceled) {
					return responder.Empty()
				}

				if status, ok := status.FromError(err); ok && status.Code() == codes.Canceled {
					return responder.Empty()
				}

				controller.logger.Warnf("port forwarding: failed to proxy connections: %v", err)
			}

			return responder.Empty()
		case <-time.After(controller.pingInterval):
			pingCtx, pingCtxCancel := context.WithTimeout(ctx, 5*time.Second)

			if err := wsConn.Ping(pingCtx); err != nil {
				controller.logger.Warnf("port forwarding: failed to ping the client, "+
					"connection might time out: %v", err)
			}

			pingCtxCancel()
		}
	}
}

func (controller *Controller) waitForVM(ctx context.Context, name string) (*v1.VM, responder.Responder) {
	var vm *v1.VM
	var err error

	for {
		if lookupResponder := controller.storeView(func(txn storepkg.Transaction) responder.Responder {
			vm, err = txn.GetVM(name)
			if err != nil {
				return responder.Error(err)
			}

			return nil
		}); lookupResponder != nil {
			return nil, lookupResponder
		}

		if vm.TerminalState() {
			return nil, responder.JSON(http.StatusExpectationFailed,
				NewErrorResponse("VM is in a terminal state '%s'", vm.Status))
		}
		if vm.Status == v1.VMStatusRunning {
			// VM is running, proceed
			return vm, nil
		}
		select {
		case <-ctx.Done():
			return nil, responder.JSON(http.StatusRequestTimeout,
				NewErrorResponse("VM is not running on '%s' worker", vm.Worker))
		case <-time.After(1 * time.Second):
			// try again
			continue
		}
	}
}

func (controller *Controller) portForwardConnection(
	ctx context.Context,
	waitContext context.Context,
	workerName string,
	vmUID string,
	port uint32,
	hostProcess string,
) (net.Conn, error) {
	// Create a rendezvous connection point
	rendezvousCtx, rendezvousCtxCancel := context.WithCancel(ctx)

	session := uuid.New().String()
	boomerangConnCh, boomerangConnCancel := controller.connRendezvous.Request(rendezvousCtx, session)

	cancel := func() {
		boomerangConnCancel()
		rendezvousCtxCancel()
	}

	// Send request to a worker to initiate a port forwarding connection back to us
	portForwardAction := &rpc.WatchInstruction_PortForward{
		Session: session,
	}
	if hostProcess != "" {
		portForwardAction.Target = &rpc.WatchInstruction_PortForward_Target{
			Value: &rpc.WatchInstruction_PortForward_Target_HostProcess_{
				HostProcess: &rpc.WatchInstruction_PortForward_Target_HostProcess{
					VmUid: vmUID,
					Name:  hostProcess,
				},
			},
		}
	} else {
		portForwardAction.VmUid = vmUID
		portForwardAction.Port = port
	}
	err := controller.workerNotifier.Notify(waitContext, workerName, &rpc.WatchInstruction{
		Action: &rpc.WatchInstruction_PortForwardAction{
			PortForwardAction: portForwardAction,
		},
	})
	if err != nil {
		cancel()

		return nil, fmt.Errorf("%w: failed to request port forwarding from the worker %s: %v",
			errPortForwardRequest, workerName, err)
	}

	// Wait for the worker to respond
	select {
	case rendezvousResponse := <-boomerangConnCh:
		if rendezvousResponse.ErrorMessage != "" {
			cancel()

			return nil, fmt.Errorf("failed to establish port forwarding session on the worker: %s",
				rendezvousResponse.ErrorMessage)
		}

		return netconncancel.New(rendezvousResponse.Result, cancel), nil
	case <-waitContext.Done():
		cancel()

		return nil, waitContext.Err()
	}
}
