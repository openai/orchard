package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/cirruslabs/orchard/internal/proxy"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager"
	"github.com/cirruslabs/orchard/rpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"

	"net"

	//nolint:staticcheck // https://github.com/mitchellh/go-grpc-net-conn/pull/1
	"github.com/golang/protobuf/proto"
	grpc_net_conn "github.com/mitchellh/go-grpc-net-conn"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/samber/lo"
)

func (worker *Worker) watchRPC(ctx context.Context, operationCtx context.Context, onEstablished func()) error {
	worker.logger.Infof("connecting to %s over gRPC", worker.client.GRPCTarget())

	conn, err := grpc.NewClient(worker.client.GRPCTarget(),
		grpc.WithTransportCredentials(worker.client.GRPCTransportCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: 30 * time.Second,
		}),
	)
	if err != nil {
		return err
	}

	worker.logger.Infof("gRPC connection established, starting gRPC stream with the controller")

	client := rpc.NewControllerClient(conn)

	ctxWithMetadata := metadata.NewOutgoingContext(ctx, worker.grpcMetadata())
	operationCtxWithMetadata := metadata.NewOutgoingContext(operationCtx, worker.grpcMetadata())

	stream, err := client.Watch(ctxWithMetadata, &emptypb.Empty{})
	if err != nil {
		return err
	}
	onEstablished()

	worker.logger.Infof("running gRPC stream with the controller")

	for {
		watchFromController, err := stream.Recv()
		if err != nil {
			return err
		}

		switch action := watchFromController.Action.(type) {
		case *rpc.WatchInstruction_PortForwardAction:
			go worker.handlePortForward(operationCtxWithMetadata, client, action.PortForwardAction)
		case *rpc.WatchInstruction_SyncVmsAction:
			worker.requestVMSyncing()
		case *rpc.WatchInstruction_ResolveIpAction:
			go worker.handleGetIP(operationCtxWithMetadata, client, action.ResolveIpAction)
		}
	}
}

func (worker *Worker) handlePortForward(
	ctx context.Context,
	client rpc.ControllerClient,
	portForwardAction *rpc.WatchInstruction_PortForward,
) {
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	grpcMetadata := metadata.Join(
		worker.grpcMetadata(),
		metadata.Pairs(rpc.MetadataWorkerPortForwardingSessionKey, portForwardAction.Session),
	)
	ctxWithMetadata := metadata.NewOutgoingContext(subCtx, grpcMetadata)
	stream, err := client.PortForward(ctxWithMetadata)
	if err != nil {
		worker.logger.Warnf("port forwarding failed: failed to call PortForward() RPC method: %v", err)

		return
	}

	var host string

	if portForwardAction.VmUid == "" {
		// Port-forwarding request to a worker
		host = "localhost"
	} else {
		// Port-forwarding request to a VM, find that VM
		vm, ok := lo.Find(worker.vmm.List(), func(item vmmanager.VM) bool {
			return item.Resource().UID == portForwardAction.VmUid
		})
		if !ok {
			worker.logger.Warnf("port forwarding failed: failed to get the VM: %v", err)

			return
		}

		// Obtain VM's IP address
		host, err = vm.IP(ctx)
		if err != nil {
			worker.logger.Warnf("port forwarding failed: failed to get VM's IP: %v", err)

			return
		}
	}

	// Connect to the VM's port
	var vmConn net.Conn

	if worker.dialer != nil {
		vmConn, err = worker.dialer.DialContext(ctx, "tcp",
			fmt.Sprintf("%s:%d", host, portForwardAction.Port))
	} else {
		dialer := net.Dialer{}

		vmConn, err = dialer.DialContext(ctx, "tcp",
			fmt.Sprintf("%s:%d", host, portForwardAction.Port))
	}
	if err != nil {
		worker.logger.Warnf("port forwarding failed: failed to connect to the VM: %v", err)

		return
	}

	// Proxy bytes
	grpcConn := &grpc_net_conn.Conn{
		Stream:   stream,
		Request:  &rpc.PortForwardData{},
		Response: &rpc.PortForwardData{},
		Encode: grpc_net_conn.SimpleEncoder(func(message proto.Message) *[]byte {
			return &message.(*rpc.PortForwardData).Data
		}),
		Decode: grpc_net_conn.SimpleDecoder(func(message proto.Message) *[]byte {
			return &message.(*rpc.PortForwardData).Data
		}),
	}

	_ = proxy.Connections(vmConn, grpcConn)
}

func (worker *Worker) handleGetIP(
	ctx context.Context,
	client rpc.ControllerClient,
	resolveIP *rpc.WatchInstruction_ResolveIP,
) {
	grpcMetadata := metadata.Join(
		worker.grpcMetadata(),
		metadata.Pairs(rpc.MetadataWorkerPortForwardingSessionKey, resolveIP.Session),
	)
	ctxWithMetadata := metadata.NewOutgoingContext(ctx, grpcMetadata)

	// Find the desired VM
	vm, ok := lo.Find(worker.vmm.List(), func(item vmmanager.VM) bool {
		return item.Resource().UID == resolveIP.VmUid
	})
	if !ok {
		worker.logger.Warnf("failed to resolve IP for the VM with UID %q: VM not found",
			resolveIP.VmUid)

		return
	}

	// Obtain VM's IP address
	ip, err := vm.IP(ctx)
	if err != nil {
		worker.logger.Warnf("failed to resolve IP for the VM with UID %q: \"tart ip\" failed: %v",
			resolveIP.VmUid, err)

		return
	}

	_, err = client.ResolveIP(ctxWithMetadata, &rpc.ResolveIPResult{
		Session: resolveIP.Session,
		Ip:      ip,
	})
	if err != nil {
		worker.logger.Warnf("failed to resolve IP for the VM with UID %q: "+
			"failed to call back to the controller: %v", resolveIP.VmUid, err)

		return
	}
}
