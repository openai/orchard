package sshserver

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/cirruslabs/orchard/internal/controller/notifier"
	"github.com/cirruslabs/orchard/internal/controller/rendezvous"
	storepkg "github.com/cirruslabs/orchard/internal/controller/store"
	"github.com/cirruslabs/orchard/internal/proxy"
	"github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/cirruslabs/orchard/rpc"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
)

const (
	// "ssh -J" uses channels of type "direct-tcpip", which are documented
	// in the RFC 4254 (7.2. TCP/IP Forwarding Channels)[1].
	//
	// [1]: https://datatracker.ietf.org/doc/html/rfc4254#section-7.2
	channelTypeDirectTCPIP = "direct-tcpip"
)

type SSHServer struct {
	listener       net.Listener
	serverConfig   *ssh.ServerConfig
	store          storepkg.Store
	connRendezvous *rendezvous.Rendezvous[rendezvous.ResultWithErrorMessage[net.Conn]]
	workerNotifier *notifier.Notifier
	logger         *zap.SugaredLogger
}

func NewSSHServer(
	address string,
	signer ssh.Signer,
	store storepkg.Store,
	connRendezvous *rendezvous.Rendezvous[rendezvous.ResultWithErrorMessage[net.Conn]],
	workerNotifier *notifier.Notifier,
	noClientAuth bool,
	logger *zap.SugaredLogger,
) (*SSHServer, error) {
	server := &SSHServer{
		store:          store,
		connRendezvous: connRendezvous,
		workerNotifier: workerNotifier,
		logger:         logger,
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	server.listener = listener

	server.serverConfig = &ssh.ServerConfig{
		NoClientAuth:     noClientAuth,
		PasswordCallback: server.passwordCallback,
	}
	server.serverConfig.AddHostKey(signer)

	return server, nil
}

// Close stops accepting SSH connections.
func (server *SSHServer) Close() error {
	return server.listener.Close()
}

func (server *SSHServer) Run() {
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			server.logger.Warnf("failed to accept connection: %v", err)

			continue
		}

		go server.handleConnection(conn)
	}
}

func (server *SSHServer) Address() string {
	return strings.ReplaceAll(server.listener.Addr().String(), "[::]", "127.0.0.1")
}

func (server *SSHServer) passwordCallback(connMetadata ssh.ConnMetadata, password []byte) (*ssh.Permissions, error) {
	if err := server.store.View(func(txn storepkg.Transaction) error {
		// Authenticate
		server.logger.Debugf("authenticating user %q using the password authentication",
			connMetadata.User())

		serviceAccount, err := txn.GetServiceAccount(connMetadata.User())
		if err != nil {
			if errors.Is(err, storepkg.ErrNotFound) {
				return fmt.Errorf("authentication failed, non-existent user %q",
					connMetadata.User())
			}

			server.logger.Errorf("failed to retrieve service account %q: %v",
				connMetadata.User(), err)

			return fmt.Errorf("authentication failed due to an internal error")
		}

		if subtle.ConstantTimeCompare([]byte(serviceAccount.Token), password) != 1 {
			return fmt.Errorf("authentication failed for user %q: invalid password",
				connMetadata.User())
		}

		// Authorize
		authorized := lo.Contains(serviceAccount.Roles, v1.ServiceAccountRoleComputeWrite) ||
			lo.Contains(serviceAccount.Roles, v1.ServiceAccountRoleComputeConnect)

		if !authorized {
			return fmt.Errorf("authorization failed for user %q because it lacks %q or %q roles",
				connMetadata.User(), v1.ServiceAccountRoleComputeWrite, v1.ServiceAccountRoleComputeConnect)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return &ssh.Permissions{}, nil
}

func (server *SSHServer) handleConnection(conn net.Conn) {
	sshConn, newChannelCh, requestCh, err := ssh.NewServerConn(conn, server.serverConfig)
	if err != nil {
		server.logger.Warnf("failed to instantiate the SSH server instance to handle "+
			"the incoming connection from %s: %v", conn.RemoteAddr().String(), err)

		return
	}
	defer func() {
		_ = sshConn.Close()
	}()

	server.logger.Debugf("accepted SSH connection for user %q connecting from %q",
		sshConn.User(), sshConn.RemoteAddr().String())

	connCtx, connCtxCancel := context.WithCancel(context.Background())
	defer connCtxCancel()

	for {
		select {
		case newChannel, ok := <-newChannelCh:
			if !ok {
				return
			}

			switch newChannel.ChannelType() {
			case channelTypeDirectTCPIP:
				server.logger.Debugf("handling a new direct TCP/IP channel for user %q connecting from %q",
					sshConn.User(), sshConn.RemoteAddr().String())

				go server.handleDirectTCPIP(connCtx, newChannel)
			default:
				message := fmt.Sprintf("unsupported channel type requested: %q", newChannel.ChannelType())

				server.logger.Debugf(message)

				if err := newChannel.Reject(ssh.UnknownChannelType, message); err != nil {
					server.logger.Warnf("failed to reject new channel of unsupported type %q: %v",
						newChannel.ChannelType(), err)

					return
				}
			}
		case request, ok := <-requestCh:
			if !ok {
				return
			}

			server.logger.Debugf("refusing to service new request of type %q with payload of %d bytes",
				request.Type, len(request.Payload))

			if err := request.Reply(false, nil); err != nil {
				server.logger.Warnf("failed to reply to a new request of type %q and payload of %d bytes: %v",
					request.Type, len(request.Payload), err)

				return
			}
		}
	}
}

func (server *SSHServer) handleDirectTCPIP(ctx context.Context, newChannel ssh.NewChannel) {
	// Unmarshal the payload to determine to which VM the user wants to connect to
	//
	// This direct TCP/IP channel's payload is documented
	// in the RFC 4254 (7.2. TCP/IP Forwarding Channels)[1].
	//
	// [1]: https://datatracker.ietf.org/doc/html/rfc4254#section-7.2
	payload := struct {
		HostToConnect       string
		PortToConnect       uint32
		OriginatorIPAddress string
		OriginatorPort      uint32
	}{}

	if err := ssh.Unmarshal(newChannel.ExtraData(), &payload); err != nil {
		message := fmt.Sprintf("failed to unmarshal payload: %v", err)

		server.logger.Warn(message)

		if err := newChannel.Reject(ssh.ConnectionFailed, message); err != nil {
			server.logger.Warnf("failed to reject the new channel: %v", err)
		}

		return
	}

	server.logger.Debugf("proxying connection to %s:%d", payload.HostToConnect, payload.PortToConnect)

	// Retrieve the VM object
	var vm *v1.VM
	var err error

	err = server.store.View(func(txn storepkg.Transaction) error {
		vm, err = txn.GetVM(payload.HostToConnect)

		return err
	})
	if err != nil {
		if err := newChannel.Reject(ssh.ConnectionFailed, "failed to find VM"); err != nil {
			server.logger.Warnf("failed to reject the new channel due to non-existent VM %q: %v",
				payload.HostToConnect, err)
		}

		return
	}

	// The user wants to connect to an existing VM, request and wait
	// for a connection with the worker before accepting the channel
	session := uuid.New().String()
	boomerangConnCh, cancel := server.connRendezvous.Request(ctx, session)
	defer cancel()

	notifyContext, notifyContextCancel := context.WithTimeout(ctx, time.Second)
	defer notifyContextCancel()
	err = server.workerNotifier.Notify(notifyContext, vm.Worker, &rpc.WatchInstruction{
		Action: &rpc.WatchInstruction_PortForwardAction{
			PortForwardAction: &rpc.WatchInstruction_PortForward{
				Session: session,
				VmUid:   vm.UID,
				Port:    payload.PortToConnect,
			},
		},
	})
	if err != nil {
		server.logger.Warnf("failed to request port-forwarding from the worker %s: %v",
			vm.Worker, err)

		return
	}

	// Wait for the connection from worker and commence port forwarding
	select {
	case rendezvousResponse := <-boomerangConnCh:
		if rendezvousResponse.ErrorMessage != "" {
			message := fmt.Sprintf("failed to establish port forwarding session on the worker: %s",
				rendezvousResponse.ErrorMessage)

			if err := newChannel.Reject(ssh.ConnectionFailed, message); err != nil {
				server.logger.Warnf("failed to reject new channel due to "+
					"failure establishing port forwarding session on the worker: %v", err)
			}

			return
		}

		// Now that we have the connection from worker we can accept the channel
		acceptedChannel, acceptedChannelRequests, err := newChannel.Accept()
		if err != nil {
			server.logger.Warnf("failed to accept the new channel: %v", err)

			return
		}

		// Handle new requests on the accepted channel by refusing them
		go func() {
			req, ok := <-acceptedChannelRequests
			if !ok {
				return
			}

			if err := req.Reply(false, nil); err != nil {
				server.logger.Warnf("failed to reply to the new channel request: %v", err)

				return
			}
		}()

		// Commence port forwarding
		if err := proxy.Connections(acceptedChannel, rendezvousResponse.Result); err != nil {
			server.logger.Warnf("failed to port-forward: %v", err)
		}
	case <-ctx.Done():
		return
	}
}
