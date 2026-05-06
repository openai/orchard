//go:build unix

package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"slices"
	"strings"

	"github.com/cirruslabs/chacha/pkg/localnetworkhelper"
	"github.com/cirruslabs/chacha/pkg/privdrop"
	"github.com/cirruslabs/orchard/internal/bootstraptoken"
	"github.com/cirruslabs/orchard/internal/dialer"
	"github.com/cirruslabs/orchard/internal/echoserver"
	"github.com/cirruslabs/orchard/internal/netconstants"
	"github.com/cirruslabs/orchard/internal/worker"
	"github.com/cirruslabs/orchard/pkg/client"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	ErrRunFailed                   = errors.New("failed to run worker")
	ErrNoBootstrapTokenProvided    = errors.New("no bootstrap token was provided")
	ErrEmptyBootstrapTokenProvided = errors.New("empty bootstrap token was provided")
)

var name string
var bootstrapTokenRaw string
var bootstrapTokenStdin bool
var logFilePath string
var stringToStringResources map[string]string
var labels map[string]string
var noPKI bool
var defaultCPU uint64
var defaultMemory uint64
var username string
var addressPprof string
var debug bool

// Hidden flags
var synthetic bool
var workers int

func newRunCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run CONTROLLER_URL",
		Short: "Run worker",
		RunE:  runWorker,
		Args:  cobra.ExactArgs(1),
	}

	cmd.Flags().StringVar(&name, "name", "",
		"name of the worker (defaults to the hostname)")
	cmd.Flags().StringVar(&bootstrapTokenRaw, "bootstrap-token", "",
		"a bootstrap token retrieved via \"orchard get bootstrap-token <service-account-name-for-workers>\"")
	cmd.Flags().BoolVar(&bootstrapTokenStdin, "bootstrap-token-stdin", false,
		"use this flag to provide a bootstrap token via the standard input")
	cmd.Flags().StringVar(&logFilePath, "log-file", "",
		"optional path to a file where logs (up to 100 Mb) will be written.")
	cmd.Flags().StringToStringVar(&stringToStringResources, "resources", map[string]string{},
		"resources that this worker provides")
	cmd.Flags().StringToStringVar(&labels, "labels", map[string]string{},
		"labels that this worker supports")
	cmd.Flags().BoolVar(&noPKI, "no-pki", false,
		"do not use the host's root CA set and instead validate the Controller's presented "+
			"certificate using a bootstrap token (or manually via fingerprint, "+
			"if no bootstrap token is provided)")
	cmd.Flags().Uint64Var(&defaultCPU, "default-cpu", 4, "number of CPUs to use for VMs "+
		"that do not explicitly specify a value")
	cmd.Flags().Uint64Var(&defaultMemory, "default-memory", 8*1024, "megabytes of memory "+
		"to use for VMs that do not explicitly specify a value")
	cmd.Flags().StringVar(&username, "user", "", "username to drop privileges to "+
		"(\"Local Network\" permission workaround: requires starting \"orchard worker run\" as \"root\", "+
		"the privileges will be then dropped to the specified user after starting the \"orchard localnetworkhelper\" "+
		"helper process)")
	cmd.Flags().StringVar(&addressPprof, "listen-pprof", "",
		"start pprof HTTP server on localhost:6060 for diagnostic purposes (e.g. \"localhost:6060\")")
	cmd.Flags().BoolVar(&debug, "debug", false, "enable debug logging")

	// Hidden flags
	cmd.Flags().BoolVar(&synthetic, "synthetic", false,
		"do not instantiate real Tart VM, use synthetic in-memory VMs suitable for load testing")
	cmd.Flags().MarkHidden("synthetic")

	cmd.Flags().IntVar(&workers, "workers", 1, "number of workers to start")
	cmd.Flags().MarkHidden("workers")

	return cmd
}

func runWorker(cmd *cobra.Command, args []string) (err error) {
	var clientOpts []client.Option

	workerOpts := []worker.Option{
		worker.WithName(name),
		worker.WithLabels(labels),
		worker.WithDefaultCPUAndMemory(defaultCPU, defaultMemory),
	}

	// Run the macOS "Local Network" permission helper
	// when privilege dropping is requested
	if username != "" {
		localNetworkHelper, err := localnetworkhelper.New(cmd.Context())
		if err != nil {
			return err
		}

		dialer := dialer.DialFunc(func(ctx context.Context, network, addr string) (net.Conn, error) {
			return localNetworkHelper.PrivilegedDialContext(ctx, network, addr)
		})

		clientOpts = append(clientOpts, client.WithDialer(dialer))
		workerOpts = append(workerOpts, worker.WithDialer(dialer))

		if err := privdrop.Drop(username); err != nil {
			return err
		}
	}

	// Parse controller URL
	controllerURL, err := netconstants.NormalizeAddress(args[0])
	if err != nil {
		return err
	}
	clientOpts = append(clientOpts, client.WithAddress(controllerURL.String()))

	// Parse bootstrap token
	bootstrapTokenRaw, err := readBootstrapToken()
	if err != nil {
		return err
	}
	if bootstrapTokenRaw == "" {
		return ErrEmptyBootstrapTokenProvided
	}
	bootstrapToken, err := bootstraptoken.NewFromString(bootstrapTokenRaw)
	if err != nil {
		return err
	}
	clientOpts = append(clientOpts, client.WithCredentials(bootstrapToken.ServiceAccountName(),
		bootstrapToken.ServiceAccountToken()))

	// Convert resources
	resources, err := v1.NewResourcesFromStringToString(stringToStringResources)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrRunFailed, err)
	}
	workerOpts = append(workerOpts, worker.WithResources(resources))

	if trustedCertificate := bootstrapToken.Certificate(); trustedCertificate != nil {
		clientOpts = append(clientOpts, client.WithTrustedCertificate(trustedCertificate))
	} else if noPKI {
		return fmt.Errorf("%w: --no-pki was specified, but not trusted certificate was provided "+
			"in the bootstrap token", ErrRunFailed)
	}

	// Initialize the logger
	logger, err := createLogger()
	if err != nil {
		return err
	}
	defer func() {
		if syncErr := logger.Sync(); syncErr != nil && err == nil {
			err = syncErr
		}
	}()
	workerOpts = append(workerOpts, worker.WithLogger(logger))

	if addressPprof != "" {
		go func() {
			if err := http.ListenAndServe(addressPprof, nil); err != nil {
				logger.Sugar().Errorf("pprof server failed: %v", err)
			}
		}()
	}

	group, ctx := errgroup.WithContext(cmd.Context())

	if synthetic {
		// Use TCP echo server to partially emulate VM's TCP/IP stack,
		// this way we get port-forwarding working when running in
		// synthetic mode
		echoServerOpts := []echoserver.Option{
			echoserver.WithLogger(logger.Sugar().With("component", "echoserver")),
		}

		echoServer, err := echoserver.New(echoServerOpts...)
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

		workerOpts = append(workerOpts, worker.WithSynthetic(), worker.WithDialer(dialer))
	}

	for i := range workers {
		group.Go(func() error {
			workerOptsLocal := slices.Clone(workerOpts)

			if workers > 1 {
				workerOptsLocal = append(workerOptsLocal, worker.WithNameSuffix(fmt.Sprintf("-%d", i+1)))
			}

			controllerClient, err := client.New(clientOpts...)
			if err != nil {
				return err
			}

			workerInstance, err := worker.New(controllerClient, workerOptsLocal...)
			if err != nil {
				return err
			}
			defer workerInstance.Close()

			return workerInstance.Run(ctx)
		})
	}

	return group.Wait()
}

func readBootstrapToken() (string, error) {
	if bootstrapTokenRaw != "" && bootstrapTokenStdin {
		return "", fmt.Errorf("--bootstrap-token and --bootstrap-token-stdin are mutually exclusive")
	}

	if bootstrapTokenRaw != "" {
		return bootstrapTokenRaw, nil
	}

	if bootstrapTokenStdin {
		stdinBytes, err := io.ReadAll(os.Stdin)
		if err != nil {
			return "", fmt.Errorf("failed to read the bootstrap token from the standard input: %w", err)
		}

		return strings.TrimSuffix(string(stdinBytes), "\n"), nil
	}

	return "", ErrNoBootstrapTokenProvided
}

func createLogger() (*zap.Logger, error) {
	const (
		maxLogSizeMB  = 100
		maxLogBackups = 5
		maxLogAgeDays = 14
	)

	level := zap.InfoLevel
	if debug {
		level = zap.DebugLevel
	}

	if logFilePath == "" {
		zapConfig := zap.NewProductionConfig()
		zapConfig.Level = zap.NewAtomicLevelAt(level)

		return zapConfig.Build()
	}

	logFileWriter := zapcore.AddSync(&lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    maxLogSizeMB,
		MaxBackups: maxLogBackups,
		MaxAge:     maxLogAgeDays,
		Compress:   true,
	})
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		logFileWriter,
		level,
	)
	return zap.New(core), nil
}
