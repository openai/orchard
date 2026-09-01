package devcontroller

import (
	"context"
	"errors"
	"github.com/cirruslabs/orchard/internal/command/dev"
	"github.com/cirruslabs/orchard/internal/controller"
	"github.com/cirruslabs/orchard/internal/worker"
	"github.com/cirruslabs/orchard/pkg/client"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
	"time"
)

func StartIntegrationTestEnvironment(t *testing.T) (*client.Client, *controller.Controller, *worker.Worker) {
	return StartIntegrationTestEnvironmentWithAdditionalOpts(t,
		false, nil,
		false, nil,
	)
}

func StartIntegrationTestEnvironmentWithAdditionalOpts(
	t *testing.T,
	noController bool,
	additionalControllerOpts []controller.Option,
	noWorker bool,
	additionalWorkerOpts []worker.Option,
) (*client.Client, *controller.Controller, *worker.Worker) {
	t.Setenv("ORCHARD_HOME", t.TempDir())

	devController, devWorker, err := dev.CreateDevControllerAndWorker(t.TempDir(),
		":0", nil, additionalControllerOpts, additionalWorkerOpts)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = devWorker.Close()
	})

	devContext, cancelDevFunc := context.WithCancel(context.Background())
	t.Cleanup(cancelDevFunc)

	if !noController {
		go func() {
			err := devController.Run(devContext)
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("dev controller failed: %v", err)
			}
		}()
	}

	if !noWorker {
		go func() {
			err := devWorker.Run(devContext)
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("dev worker failed: %v", err)
			}
		}()
	}

	time.Sleep(5 * time.Second)

	devClient, err := client.New(client.WithAddress(devController.Address()))
	require.NoError(t, err)

	return devClient, devController, devWorker
}
