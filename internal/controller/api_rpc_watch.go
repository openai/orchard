package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cirruslabs/orchard/internal/responder"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/cirruslabs/orchard/rpc"
	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
)

//nolint:protogetter // Preserve the original host-process wire conversion.
func (controller *Controller) rpcWatch(ctx *gin.Context) responder.Responder {
	if responder := controller.authorize(ctx, v1.ServiceAccountRoleComputeRead); responder != nil {
		return responder
	}

	// Retrieve and parse path and query parameters
	workerName := ctx.Query("workerName")

	if workerName == "" {
		return responder.Error(errors.New("worker name cannot be empty"))
	}

	// Register with the worker notifier to forward requests from other
	// parts of the Orchard Controller destined to this specific worker
	workerCh, cancel := controller.workerNotifier.Register(ctx, workerName)
	defer cancel()

	// Perform WebSocket protocol upgrade
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

	// Ensure that pongs will be processed by reading
	// from the connection in the background
	//
	// Otherwise the wsConn.Ping() will wait forever.
	closeReadCtx := wsConn.CloseRead(ctx)

	for {
		select {
		case msg := <-workerCh:
			var watchInstruction v1.WatchInstruction

			// Perform Protocol Buffers to v1 API data structure conversion
			switch typedAction := msg.Action.(type) {
			case *rpc.WatchInstruction_PortForwardAction:
				watchInstruction.PortForwardAction = &v1.PortForwardAction{
					Session: typedAction.PortForwardAction.Session,
				}

				if target := typedAction.PortForwardAction.GetTarget(); target != nil {
					watchInstruction.PortForwardAction.Target = &v1.PortForwardTarget{}

					if hostProcess := target.GetHostProcess(); hostProcess != nil {
						watchInstruction.PortForwardAction.Target.HostProcess = &v1.PortForwardTargetHostProcess{
							VMUID: hostProcess.VmUid,
							Name:  hostProcess.Name,
						}
					}
				} else {
					watchInstruction.PortForwardAction.VMUID = typedAction.PortForwardAction.VmUid
					watchInstruction.PortForwardAction.Port = uint16(typedAction.PortForwardAction.Port)
				}
			case *rpc.WatchInstruction_SyncVmsAction:
				watchInstruction.SyncVMsAction = &v1.SyncVMsAction{}
			case *rpc.WatchInstruction_ResolveIpAction:
				watchInstruction.ResolveIPAction = &v1.ResolveIPAction{
					Session: typedAction.ResolveIpAction.Session,
					VMUID:   typedAction.ResolveIpAction.VmUid,
				}
			default:
				continue
			}

			watchInstructionJSONBytes, err := json.Marshal(&watchInstruction)
			if err != nil {
				return controller.wsError(wsConn, websocket.StatusInternalError, "watch RPC",
					"failure to marshal the watch instruction", err)
			}

			if err := wsConn.Write(ctx, websocket.MessageBinary, watchInstructionJSONBytes); err != nil {
				return controller.wsError(wsConn, websocket.StatusInternalError, "watch RPC",
					"failure to write the watch instruction", err)
			}
		case <-time.After(controller.pingInterval):
			pingCtx, pingCtxCancel := context.WithTimeout(ctx, 5*time.Second)

			if err := wsConn.Ping(pingCtx); err != nil {
				controller.logger.Warnf("watch RPC: failed to ping the worker %s, "+
					"connection might time out: %v", workerName, err)
			}

			pingCtxCancel()
		case <-closeReadCtx.Done():
			// Connection shouldn't be normally closed by the worker
			return controller.wsErrorNoClose("watch RPC",
				fmt.Sprintf("worker %s unexpectedly disconnected", workerName), closeReadCtx.Err())
		case <-ctx.Done():
			// Connection shouldn't be normally closed by the worker
			return controller.wsErrorNoClose("watch RPC",
				fmt.Sprintf("worker %s unexpectedly disconnected", workerName), ctx.Err())
		}
	}
}
