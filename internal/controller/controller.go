package controller

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cirruslabs/orchard/internal/controller/notifier"
	"github.com/cirruslabs/orchard/internal/controller/rendezvous"
	"github.com/cirruslabs/orchard/internal/controller/scheduler"
	"github.com/cirruslabs/orchard/internal/controller/sshserver"
	storepkg "github.com/cirruslabs/orchard/internal/controller/store"
	"github.com/cirruslabs/orchard/internal/controller/store/badger"
	"github.com/cirruslabs/orchard/internal/netconstants"
	"github.com/cirruslabs/orchard/internal/opentelemetry"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/cirruslabs/orchard/rpc"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	ErrInitFailed      = errors.New("controller initialization failed")
	ErrAdminTaskFailed = errors.New("controller administrative task failed")
)

type Controller struct {
	dataDir                            *DataDir
	listenAddr                         string
	apiPrefix                          string
	tlsConfig                          *tls.Config
	listener                           net.Listener
	httpServer                         *http.Server
	insecureAuthDisabled               bool
	insecureAllowHostDirs              bool
	scheduler                          *scheduler.Scheduler
	store                              storepkg.Store
	logger                             *zap.SugaredLogger
	grpcServer                         *grpc.Server
	workerNotifier                     *notifier.Notifier
	connRendezvous                     *rendezvous.Rendezvous[rendezvous.ResultWithErrorMessage[net.Conn]]
	ipRendezvous                       *rendezvous.Rendezvous[rendezvous.ResultWithErrorMessage[string]]
	enableSwaggerDocs                  bool
	workerOfflineTimeout               time.Duration
	execSessionRetentionTTL            time.Duration
	execSSHConnectionKeepaliveInterval time.Duration
	experimentalRPCV2                  bool
	disableDBCompression               bool
	pingInterval                       time.Duration
	synthetic                          bool

	sshListenAddr   string
	sshSigner       ssh.Signer
	sshNoClientAuth bool
	sshServer       *sshserver.SSHServer
	execSessions    *execSessionRegistry
	execSSHClients  *execSSHClientPool

	single singleflight.Group

	rpc.UnimplementedControllerServer
}

func New(opts ...Option) (*Controller, error) {
	controller := &Controller{
		connRendezvous:                     rendezvous.New[rendezvous.ResultWithErrorMessage[net.Conn]](),
		ipRendezvous:                       rendezvous.New[rendezvous.ResultWithErrorMessage[string]](),
		workerOfflineTimeout:               3 * time.Minute,
		execSessionRetentionTTL:            10 * time.Minute,
		execSSHConnectionKeepaliveInterval: 30 * time.Second,
		pingInterval:                       30 * time.Second,
		execSessions:                       newExecSessionRegistry(),
		single:                             singleflight.Group{},
	}

	// Apply options
	for _, opt := range opts {
		opt(controller)
	}

	// Apply defaults
	if controller.dataDir == nil {
		return nil, fmt.Errorf("%w: please specify the data directory path with WithDataDir()",
			ErrInitFailed)
	}
	if controller.listenAddr == "" {
		controller.listenAddr = fmt.Sprintf(":%d", netconstants.DefaultControllerPort)
	}
	if controller.logger == nil {
		controller.logger = zap.NewNop().Sugar()
	}
	controller.execSSHClients = newExecSSHClientPool(
		controller.execSSHConnectionKeepaliveInterval,
		controller.logger.With("component", "exec-ssh"),
	)

	// Instantiate the database
	store, err := badger.NewBadgerStore(controller.dataDir.DBPath(), controller.disableDBCompression,
		controller.logger)
	if err != nil {
		return nil, err
	}
	controller.store = store

	// Instantiate the worker notifier
	controller.workerNotifier = notifier.NewNotifier(controller.logger.With("component", "rpc"))

	// Instantiate the scheduler
	controller.scheduler, err = scheduler.NewScheduler(store, controller.workerNotifier,
		controller.workerOfflineTimeout, controller.logger)
	if err != nil {
		return nil, err
	}

	// Instantiate the SSH server (if configured)
	if controller.sshListenAddr != "" && controller.sshSigner != nil {
		controller.sshServer, err = sshserver.NewSSHServer(controller.sshListenAddr, controller.sshSigner,
			store, controller.connRendezvous, controller.workerNotifier, controller.sshNoClientAuth, controller.logger)
		if err != nil {
			return nil, err
		}
	}

	// Instantiate the controller
	listener, err := net.Listen("tcp", controller.listenAddr)
	if err != nil {
		return nil, err
	}
	if controller.tlsConfig != nil {
		controller.listener = tls.NewListener(listener, controller.tlsConfig)
	} else {
		controller.listener = listener
	}

	apiServer := controller.initAPI()

	controller.grpcServer = grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 30 * time.Second,
		}),
	)
	rpc.RegisterControllerServer(controller.grpcServer, controller)

	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Content-Type") == "application/grpc" {
			controller.grpcServer.ServeHTTP(writer, request)
		} else {
			apiServer.ServeHTTP(writer, request)
		}
	})

	controller.httpServer = &http.Server{
		Handler:           h2c.NewHandler(handler, &http2.Server{}),
		ReadHeaderTimeout: 60 * time.Second,
	}

	// Ensure cluster settings object is present
	if err := controller.store.Update(func(txn storepkg.Transaction) error {
		_, err := txn.GetClusterSettings()
		if errors.Is(err, storepkg.ErrNotFound) {
			return txn.SetClusterSettings(v1.ClusterSettings{})
		}

		return err
	}); err != nil {
		return nil, fmt.Errorf("%w: failed to ensure cluster settings object is present: %v",
			ErrInitFailed, err)
	}

	// Migrate VMs that were created before platform fields were introduced
	if err := controller.vmsEnsurePlatformDefaults(); err != nil {
		return nil, fmt.Errorf("%w: failed to migrate VM platform defaults: %v", ErrInitFailed, err)
	}

	// Migrate workers that were created before platform fields were introduced
	if err := controller.workersEnsurePlatformDefaults(); err != nil {
		return nil, fmt.Errorf("%w: failed to migrate VM platform defaults: %v", ErrInitFailed, err)
	}

	// Metrics
	if err := controller.initializeMetrics(); err != nil {
		return nil, err
	}

	// When no "--insecure-allow-host-dirs" is present,
	// fail the VMs that have "hostDirs" set
	if !controller.insecureAllowHostDirs {
		if err := controller.failVMsWithHostDirs(); err != nil {
			return nil, err
		}
	}

	return controller, nil
}

func (controller *Controller) vmsEnsurePlatformDefaults() error {
	return controller.store.Update(func(txn storepkg.Transaction) error {
		vms, err := txn.ListVMs()
		if err != nil {
			return err
		}

		for _, vm := range vms {
			updated := false

			if vm.OS == "" {
				vm.OS = v1.OSDarwin
				updated = true
			}
			if vm.Arch == "" {
				vm.Arch = v1.ArchitectureARM64
				updated = true
			}
			if vm.Runtime == "" {
				vm.Runtime = v1.RuntimeTart
				updated = true
			}

			if !updated {
				continue
			}

			if err := txn.SetVM(vm); err != nil {
				return err
			}
		}

		return nil
	})
}

func (controller *Controller) workersEnsurePlatformDefaults() error {
	return controller.store.Update(func(txn storepkg.Transaction) error {
		workers, err := txn.ListWorkers()
		if err != nil {
			return err
		}

		for _, worker := range workers {
			updated := false

			if worker.Arch == "" {
				worker.Arch = v1.ArchitectureARM64
				updated = true
			}
			if worker.Runtime == "" {
				worker.Runtime = v1.RuntimeTart
				updated = true
			}

			if !updated {
				continue
			}

			if err := txn.SetWorker(worker); err != nil {
				return err
			}
		}

		return nil
	})
}

func (controller *Controller) ServiceAccounts() ([]v1.ServiceAccount, error) {
	var serviceAccounts []v1.ServiceAccount
	var err error

	if err := controller.store.View(func(txn storepkg.Transaction) error {
		serviceAccounts, err = txn.ListServiceAccounts()

		return err
	}); err != nil {
		return nil, fmt.Errorf("failed to retrieve a list of service accounts: %w", err)
	}

	return serviceAccounts, nil
}

func (controller *Controller) EnsureServiceAccount(serviceAccount *v1.ServiceAccount) error {
	if serviceAccount.Name == "" {
		return fmt.Errorf("%w: attempted to create a service account with an empty name",
			ErrAdminTaskFailed)
	}

	if serviceAccount.Token == "" {
		return fmt.Errorf("%w: attempted to create a service account with an empty token",
			ErrAdminTaskFailed)
	}

	serviceAccount.CreatedAt = time.Now()

	return controller.store.Update(func(txn storepkg.Transaction) error {
		return txn.SetServiceAccount(serviceAccount)
	})
}

func (controller *Controller) DeleteServiceAccount(name string) error {
	return controller.store.Update(func(txn storepkg.Transaction) error {
		return txn.DeleteServiceAccount(name)
	})
}

func (controller *Controller) failVMsWithHostDirs() error {
	return controller.store.Update(func(txn storepkg.Transaction) error {
		vms, err := txn.ListVMs()
		if err != nil {
			return err
		}

		for _, vm := range vms {
			if vm.TerminalState() || len(vm.HostDirs) == 0 {
				continue
			}

			vm.Status = v1.VMStatusFailed
			vm.StatusMessage = "host directories are used, but host directory sharing is disabled"
			vm.RestartPolicy = v1.RestartPolicyNever

			if err := txn.SetVM(vm); err != nil {
				return err
			}
		}

		return nil
	})
}

func (controller *Controller) Run(ctx context.Context) error {
	// Run the scheduler so that each VM will eventually
	// be assigned to a specific Worker
	go controller.scheduler.Run()

	// Run the SSH server (if configured)
	if controller.sshServer != nil {
		go controller.sshServer.Run()
	}

	// A helper function to shut down the HTTP server on context cancellation
	go func() {
		<-ctx.Done()

		controller.execSessions.closeAll()
		controller.execSSHClients.closeAll()

		if err := controller.httpServer.Shutdown(ctx); err != nil {
			controller.logger.Errorf("failed to cleanly shutdown the HTTP server: %v", err)
		}
	}()

	if err := controller.httpServer.Serve(controller.listener); err != nil {
		return err
	}

	return nil
}

func (controller *Controller) Address() string {
	hostPort := strings.ReplaceAll(controller.listener.Addr().String(), "[::]", "127.0.0.1")

	url := url.URL{
		Scheme: "http",
		Host:   hostPort,
		Path:   controller.apiPrefix,
	}

	if controller.tlsConfig != nil {
		url.Scheme = "https"
	}

	return url.String()
}

func (controller *Controller) SSHAddress() (string, bool) {
	if controller.sshServer == nil {
		return "", false
	}

	return controller.sshServer.Address(), true
}

//nolint:gocognit // looks OK for now
func (controller *Controller) initializeMetrics() error {
	_, err := opentelemetry.DefaultMeter.Int64ObservableGauge("org.cirruslabs.orchard.controller.vm_status",
		metric.WithInt64Callback(func(ctx context.Context, observer metric.Int64Observer) error {
			return controller.store.View(func(txn storepkg.Transaction) error {
				vms, err := txn.ListVMs()
				if err != nil {
					return err
				}

				type Key struct {
					Worker string
					Status v1.VMStatus
				}

				groups := lo.CountValuesBy(vms, func(vm v1.VM) Key {
					return Key{
						Worker: vm.Worker,
						Status: vm.Status,
					}
				})

				for key, count := range groups {
					observer.Observe(int64(count), metric.WithAttributes(
						attribute.String("worker", key.Worker),
						attribute.String("status", key.Status.String()),
					))
				}

				return nil
			})
		}),
	)
	if err != nil {
		return err
	}

	_, err = opentelemetry.DefaultMeter.Int64ObservableGauge("org.cirruslabs.orchard.controller.worker_status",
		metric.WithInt64Callback(func(ctx context.Context, observer metric.Int64Observer) error {
			return controller.store.View(func(txn storepkg.Transaction) error {
				workers, err := txn.ListWorkers()
				if err != nil {
					return err
				}

				groups := lo.CountValuesBy(workers, func(worker v1.Worker) string {
					if worker.Offline(controller.workerOfflineTimeout) {
						return "offline"
					}

					return "online"
				})

				for status, count := range groups {
					observer.Observe(int64(count), metric.WithAttributes(
						attribute.String("status", status),
					))
				}

				return nil
			})
		}),
	)
	if err != nil {
		return err
	}

	_, err = opentelemetry.DefaultMeter.Int64ObservableGauge("org.cirruslabs.orchard.controller.worker_resource",
		metric.WithInt64Callback(func(ctx context.Context, observer metric.Int64Observer) error {
			return controller.store.View(func(txn storepkg.Transaction) error {
				workers, err := txn.ListWorkers()
				if err != nil {
					return err
				}

				vms, err := txn.ListVMs()
				if err != nil {
					return err
				}

				_, workerInfos := scheduler.ProcessVMs(vms)

				for _, worker := range workers {
					resourcesUsed := workerInfos.Get(worker.Name).ResourcesUsed

					for key, value := range resourcesUsed {
						observer.Observe(int64(value), metric.WithAttributes(
							attribute.String("worker", worker.Name),
							attribute.String("resource", key),
							attribute.String("type", "used"),
						))
					}

					resourcesAvailable := worker.Resources.Subtracted(resourcesUsed)

					for key, value := range resourcesAvailable {
						observer.Observe(int64(value), metric.WithAttributes(
							attribute.String("worker", worker.Name),
							attribute.String("resource", key),
							attribute.String("type", "available"),
						))
					}
				}

				return nil
			})
		}),
	)
	if err != nil {
		return err
	}

	return nil
}
