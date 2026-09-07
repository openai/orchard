package controller

import (
	"crypto/tls"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	configpkg "github.com/cirruslabs/orchard/internal/config"
	"github.com/cirruslabs/orchard/internal/controller"
	"github.com/cirruslabs/orchard/internal/netconstants"
	"github.com/cirruslabs/orchard/internal/orchardhome"
	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var ErrRunFailed = errors.New("failed to run controller")

var address string
var apiPrefix string
var addressSSH string
var addressPprof string
var debug bool
var noTLS bool
var sshNoClientAuth bool
var insecureAllowHostDirs bool
var experimentalRPCV2 bool
var noExperimentalRPCV2 bool
var experimentalPingInterval time.Duration
var experimentalDisableDBCompression bool
var experimentalStoreBackend string
var experimentalEtcdEndpoints string
var experimentalEtcdKeyPrefix string
var workerOfflineTimeout time.Duration
var execSessionRetentionTTL time.Duration
var execSSHConnectionKeepaliveInterval time.Duration
var synthetic bool

func newRunCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run the controller",
		RunE:  runController,
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = strconv.FormatInt(netconstants.DefaultControllerPort, 10)
	}

	cmd.Flags().StringVarP(&address, "listen", "l", fmt.Sprintf(":%s", port),
		"address to listen on")
	cmd.Flags().StringVar(&apiPrefix, "api-prefix", "",
		"prefix to prepend to all Orchard Controller API endpoints; useful when exposing Orchard Controller "+
			"behind an HTTP proxy together with other services")
	cmd.Flags().StringVar(&addressSSH, "listen-ssh", "",
		"address for the built-in SSH server to listen on (e.g. \":6122\")")
	cmd.Flags().StringVar(&addressPprof, "listen-pprof", "",
		"start pprof HTTP server on localhost:6060 for diagnostic purposes (e.g. \"localhost:6060\")")
	cmd.Flags().BoolVar(&debug, "debug", false, "enable debug logging")
	cmd.Flags().StringVar(&controllerCertPath, "controller-cert", "",
		"use the controller certificate from the specified path instead of the auto-generated one"+
			" (requires --controller-key)")
	cmd.Flags().StringVar(&controllerKeyPath, "controller-key", "",
		"use the controller certificate key from the specified path instead of the auto-generated one"+
			" (requires --controller-cert)")
	cmd.Flags().StringVar(&sshHostKeyPath, "ssh-host-key", "",
		"use the SSH private host key from the specified path instead of the auto-generated one")
	cmd.Flags().BoolVar(&noTLS, "insecure-no-tls", false,
		"disable TLS, making all connections to the controller unencrypted")
	cmd.Flags().BoolVar(&sshNoClientAuth, "insecure-ssh-no-client-auth", false,
		"allow SSH clients to connect to the controller's SSH server without authentication, "+
			"thus only authenticating on the target worker/VM's SSH server")
	cmd.Flags().BoolVar(&insecureAllowHostDirs, "insecure-allow-host-dirs", false,
		"allow unsafe path-based local host directory sharing")
	cmd.Flags().BoolVar(&experimentalRPCV2, "experimental-rpc-v2", false,
		"enable experimental RPC v2 (https://github.com/cirruslabs/orchard/issues/235)")
	_ = cmd.Flags().MarkHidden("experimental-rpc-v2")
	cmd.Flags().BoolVar(&noExperimentalRPCV2, "no-experimental-rpc-v2", false,
		"disable experimental RPC v2 (https://github.com/cirruslabs/orchard/issues/235)")
	cmd.Flags().DurationVar(&experimentalPingInterval, "experimental-ping-interval", 0,
		"interval between WebSocket PING's sent by the controller to workers and clients, "+
			"useful when facing intermediate load balancers/proxies that have timeouts "+
			"smaller than the controller's default 30 second interval")
	cmd.Flags().BoolVar(&experimentalDisableDBCompression, "experimental-disable-db-compression", false,
		"disable database compression, which might reduce RAM usage in some scenarios")
	cmd.Flags().StringVar(&experimentalStoreBackend, "experimental-store", string(controller.StoreBackendBadger),
		"store backend to use (badger or etcd)")
	cmd.Flags().StringVar(&experimentalEtcdEndpoints, "experimental-etcd-endpoints", "localhost:2379",
		"comma-separated etcd endpoints used when --experimental-store=etcd")
	cmd.Flags().StringVar(&experimentalEtcdKeyPrefix, "experimental-etcd-key-prefix", "/orchard",
		"etcd key prefix used when --experimental-store=etcd")
	cmd.Flags().DurationVar(&workerOfflineTimeout, "worker-offline-timeout", 3*time.Minute,
		"duration (e.g. 60s or 5m30s) after which a worker is considered offline for the purposes "+
			"of scheduling (no new VMs will be scheduled on such worker and already assigned VMs will be "+
			"marked as failed)")
	cmd.Flags().DurationVar(&execSessionRetentionTTL, "exec-session-retention-ttl", 10*time.Minute,
		"duration to retain reconnectable exec session history after the command exits")
	cmd.Flags().DurationVar(&execSSHConnectionKeepaliveInterval, "exec-ssh-connection-keepalive-interval", 30*time.Second,
		"interval between SSH keepalive requests sent by the controller for shared exec connections")

	// Hidden flags
	cmd.Flags().BoolVar(&synthetic, "synthetic", false, "")
	cmd.Flags().MarkHidden("synthetic")

	return cmd
}

func runController(cmd *cobra.Command, args []string) (err error) {
	// Avoid fancy output that doesn't go through the logger
	gin.SetMode(gin.ReleaseMode)

	// Initialize the logger
	zapConfig := zap.NewProductionConfig()
	if debug {
		zapConfig.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	}
	logger, err := zapConfig.Build()
	if err != nil {
		return err
	}
	defer func() {
		if syncErr := logger.Sync(); syncErr != nil && err == nil {
			err = syncErr
		}
	}()

	if addressPprof != "" {
		go func() {
			if err := http.ListenAndServe(addressPprof, nil); err != nil {
				logger.Sugar().Errorf("pprof server failed: %v", err)
			}
		}()
	}

	// Redirect standard's library package-global logger
	// to our zap logger at debug level
	if _, err := zap.RedirectStdLogAt(logger, zap.DebugLevel); err != nil {
		return err
	}

	// Instantiate a data directory and ensure it's initialized
	if dataDirPath == "" {
		orchardHome, err := orchardhome.Path()
		if err != nil {
			return err
		}

		dataDirPath = filepath.Join(orchardHome, "controller")
	}

	dataDir, err := controller.NewDataDir(dataDirPath)
	if err != nil {
		return err
	}

	controllerOpts := []controller.Option{
		controller.WithListenAddr(address),
		controller.WithDataDir(dataDir),
		controller.WithWorkerOfflineTimeout(workerOfflineTimeout),
		controller.WithExecSessionRetentionTTL(execSessionRetentionTTL),
		controller.WithExecSSHConnectionKeepaliveInterval(execSSHConnectionKeepaliveInterval),
		controller.WithLogger(logger),
	}

	if apiPrefix != "" {
		controllerOpts = append(controllerOpts, controller.WithAPIPrefix(apiPrefix))
	}

	if synthetic {
		controllerOpts = append(controllerOpts, controller.WithSynthetic())
	}

	if insecureAllowHostDirs {
		controllerOpts = append(controllerOpts, controller.WithInsecureAllowHostDirs())
	}

	var controllerCert tls.Certificate

	if !noTLS {
		controllerCert, err = FindControllerCertificate(dataDir)
		if err != nil {
			return err
		}

		controllerOpts = append(controllerOpts, controller.WithTLSConfig(&tls.Config{
			MinVersion: tls.VersionTLS12,
			Certificates: []tls.Certificate{
				controllerCert,
			},
			// Since gRPC clients started enforcing ALPN at some point, we need to advertise it
			//
			// See https://github.com/grpc/grpc-go/issues/7922 for more details.
			NextProtos: []string{"http/1.1", "h2"},
		}))
	}

	if addressSSH != "" {
		signer, err := FindSSHHostKey(dataDir)
		if err != nil {
			return err
		}

		controllerOpts = append(controllerOpts, controller.WithSSHServer(addressSSH, signer, sshNoClientAuth))
	}

	if experimentalRPCV2 && noExperimentalRPCV2 {
		return fmt.Errorf("--experimental-rpc-v2 and --no-experimental-rpc-v2 flags are mutually exclusive")
	}

	if experimentalRPCV2 {
		logger.Warn("--experimental-rpc-v2 flag is deprecated: experimental RPC v2 is now enabled by default")
	}

	if !noExperimentalRPCV2 {
		controllerOpts = append(controllerOpts, controller.WithExperimentalRPCV2())
	}

	if experimentalPingInterval != 0 {
		if experimentalPingInterval < 5*time.Second {
			return fmt.Errorf("--experimental-ping-interval's value cannot be less than 5 seconds")
		}

		controllerOpts = append(controllerOpts, controller.WithPingInterval(experimentalPingInterval))
	}

	if experimentalDisableDBCompression {
		controllerOpts = append(controllerOpts, controller.WithDisableDBCompression())
	}

	switch controller.StoreBackend(experimentalStoreBackend) {
	case controller.StoreBackendBadger:
		// Default store backend.
	case controller.StoreBackendEtcd:
		controllerOpts = append(controllerOpts, controller.WithEtcdStore(
			splitEtcdEndpoints(experimentalEtcdEndpoints), experimentalEtcdKeyPrefix,
		))
	default:
		return fmt.Errorf("unsupported --experimental-store value %q", experimentalStoreBackend)
	}

	if execSSHConnectionKeepaliveInterval < 5*time.Second {
		return fmt.Errorf("--exec-ssh-connection-keepalive-interval's value cannot be less than 5 seconds")
	}

	controllerInstance, err := controller.New(controllerOpts...)
	if err != nil {
		return err
	}

	// Return bootstrap service account credentials, optionally creating it
	serviceAccountName, serviceAccountToken, err := Bootstrap(controllerInstance, controllerCert)
	if err != nil {
		return err
	}

	// If bootstrap service account still exists, update a context for it,
	// optionally making this context the default one
	if serviceAccountName != "" && serviceAccountToken != "" {
		if err := createBootstrapContext(controllerInstance.Address(), controllerCert,
			serviceAccountName, serviceAccountToken); err != nil {
			return err
		}
	}

	return controllerInstance.Run(cmd.Context())
}

func splitEtcdEndpoints(rawEndpoints string) []string {
	var endpoints []string
	for _, endpoint := range strings.Split(rawEndpoints, ",") {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" {
			continue
		}

		endpoints = append(endpoints, endpoint)
	}

	return endpoints
}

func createBootstrapContext(
	controllerAddress string,
	controllerCert tls.Certificate,
	serviceAccountName string,
	serviceAccountToken string,
) error {
	configHandle, err := configpkg.NewHandle()
	if err != nil {
		return err
	}

	context := configpkg.Context{
		URL:                 controllerAddress,
		ServiceAccountName:  serviceAccountName,
		ServiceAccountToken: serviceAccountToken,
	}

	if !noTLS {
		certificatePEMBytes := pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: controllerCert.Certificate[0],
		})

		context.Certificate = certificatePEMBytes
	}

	if err := configHandle.CreateContext(BootstrapContextName, context, true); err != nil {
		return err
	}

	config, err := configHandle.Config()
	if err != nil {
		return err
	}

	if config.DefaultContext == "" {
		if err := configHandle.SetDefaultContext(BootstrapContextName); err != nil {
			return err
		}
	}

	return nil
}
