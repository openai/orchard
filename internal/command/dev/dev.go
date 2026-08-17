//go:build unix

package dev

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"slices"

	"github.com/cirruslabs/orchard/internal/config"
	"github.com/cirruslabs/orchard/internal/controller"
	"github.com/cirruslabs/orchard/internal/dialer"
	"github.com/cirruslabs/orchard/internal/echoserver"
	"github.com/cirruslabs/orchard/internal/netconstants"
	"github.com/cirruslabs/orchard/internal/worker"
	"github.com/cirruslabs/orchard/pkg/client"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var ErrFailed = errors.New("failed to run development controller and worker")

var devDataDirPath string
var apiPrefix string
var stringToStringResources map[string]string
var experimentalRPCV2 bool
var addressPprof string
var synthetic bool
var workers int
var insecureAllowHostDirs bool

func NewCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "dev",
		Short: "Run a controller and a worker for development purposes",
		RunE:  runDev,
	}

	command.Flags().StringVarP(&devDataDirPath, "data-dir", "d", ".dev-data",
		"path to persist data between runs")
	command.Flags().StringVar(&apiPrefix, "api-prefix", "",
		"prefix to prepend to all Orchard Controller API endpoints; useful when exposing Orchard Controller "+
			"behind an HTTP proxy together with other services")
	command.Flags().StringToStringVar(&stringToStringResources, "resources", map[string]string{},
		"resources that the development worker will provide")
	command.Flags().BoolVar(&experimentalRPCV2, "experimental-rpc-v2", false,
		"enable experimental RPC v2 (https://github.com/cirruslabs/orchard/issues/235)")
	command.Flags().StringVar(&addressPprof, "listen-pprof", "",
		"start pprof HTTP server on localhost:6060 for diagnostic purposes (e.g. \"localhost:6060\")")
	command.Flags().BoolVar(&synthetic, "synthetic", false,
		"do not instantiate real Tart VM, use synthetic in-memory VMs suitable for load testing")
	command.Flags().IntVar(&workers, "workers", 1, "number of workers to start")
	command.Flags().BoolVar(&insecureAllowHostDirs, "insecure-allow-host-dirs", false,
		"allow unsafe path-based local host directory sharing")

	return command
}

func runDev(cmd *cobra.Command, args []string) error {
	// Initialize the logger
	logger, err := zap.NewDevelopment()
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

	if !filepath.IsAbs(devDataDirPath) {
		pwd, err := os.Getwd()
		if err != nil {
			return err
		}
		devDataDirPath = path.Join(pwd, devDataDirPath)
	}

	// Convert resources
	resources, err := v1.NewResourcesFromStringToString(stringToStringResources)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrFailed, err)
	}

	var additionalControllerOpts []controller.Option

	if apiPrefix != "" {
		additionalControllerOpts = append(additionalControllerOpts, controller.WithAPIPrefix(apiPrefix))
	}

	if experimentalRPCV2 {
		additionalControllerOpts = append(additionalControllerOpts, controller.WithExperimentalRPCV2())
	}

	if insecureAllowHostDirs {
		additionalControllerOpts = append(additionalControllerOpts, controller.WithInsecureAllowHostDirs())
	}

	group, ctx := errgroup.WithContext(cmd.Context())

	var additionalWorkerOpts []worker.Option

	if synthetic {
		// Use TCP echo server to partially emulate VM's TCP/IP stack,
		// this way we get port-forwarding working when running in
		// synthetic mode
		echoServer, err := echoserver.New()
		if err != nil {
			return err
		}

		group.Go(func() error {
			return echoServer.Run(ctx)
		})

		dialer := dialer.DialFunc(func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialer := net.Dialer{}

			return dialer.DialContext(ctx, "tcp", echoServer.Addr())
		})

		additionalControllerOpts = append(additionalControllerOpts, controller.WithSynthetic())
		additionalWorkerOpts = append(additionalWorkerOpts, worker.WithSynthetic(),
			worker.WithDialer(dialer))
	}

	devController, devClient, err := CreateDevController(devDataDirPath, fmt.Sprintf(":%d",
		netconstants.DefaultControllerPort), additionalControllerOpts, logger)
	if err != nil {
		return err
	}

	group.Go(func() error {
		return devController.Run(ctx)
	})

	for i := range workers {
		group.Go(func() error {
			workerOptsLocal := slices.Clone(additionalWorkerOpts)

			if workers > 1 {
				workerOptsLocal = append(workerOptsLocal, worker.WithNameSuffix(fmt.Sprintf("-%d", i+1)))
			}

			devWorker, err := CreateDevWorker(devClient, resources, workerOptsLocal, logger)
			if err != nil {
				return err
			}
			defer devWorker.Close()

			return devWorker.Run(ctx)
		})
	}

	return group.Wait()
}

func CreateDevControllerAndWorker(
	devDataDirPath string,
	controllerListenAddr string,
	resources v1.Resources,
	additionalControllerOpts []controller.Option,
	additionalWorkerOpts []worker.Option,
) (*controller.Controller, *worker.Worker, error) {
	// Initialize the logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if syncErr := logger.Sync(); syncErr != nil && err == nil {
			err = syncErr
		}
	}()

	devController, defaultClient, err := CreateDevController(devDataDirPath, controllerListenAddr,
		additionalControllerOpts, logger)
	if err != nil {
		return nil, nil, err
	}

	devWorker, err := CreateDevWorker(defaultClient, resources, additionalWorkerOpts, logger)
	if err != nil {
		return nil, nil, err
	}

	return devController, devWorker, nil
}

func CreateDevController(
	devDataDirPath string,
	controllerListenAddr string,
	additionalControllerOpts []controller.Option,
	logger *zap.Logger,
) (*controller.Controller, *client.Client, error) {
	dataDir, err := controller.NewDataDir(devDataDirPath)
	if err != nil {
		return nil, nil, err
	}

	controllerOpts := []controller.Option{
		controller.WithDataDir(dataDir),
		controller.WithListenAddr(controllerListenAddr),
		controller.WithInsecureAuthDisabled(),
		controller.WithSwaggerDocs(),
		controller.WithLogger(logger),
	}

	controllerOpts = append(controllerOpts, additionalControllerOpts...)

	devController, err := controller.New(controllerOpts...)
	if err != nil {
		return nil, nil, err
	}

	defaultClient, err := client.New(client.WithAddress(devController.Address()))
	if err != nil {
		return nil, nil, err
	}

	// set local-dev context as active
	configHandle, err := config.NewHandle()
	if err != nil {
		return nil, nil, err
	}
	localContext := config.Context{URL: devController.Address()}
	err = configHandle.CreateContext("local-dev", localContext, true)
	if err != nil {
		return nil, nil, err
	}
	err = configHandle.SetDefaultContext("local-dev")
	if err != nil {
		return nil, nil, err
	}

	return devController, defaultClient, nil
}

func CreateDevWorker(
	client *client.Client,
	resources v1.Resources,
	additionalWorkerOpts []worker.Option,
	logger *zap.Logger,
) (*worker.Worker, error) {
	workerOpts := []worker.Option{
		worker.WithResources(resources),
		worker.WithLogger(logger),
	}

	workerOpts = append(workerOpts, additionalWorkerOpts...)

	return worker.New(client, workerOpts...)
}
