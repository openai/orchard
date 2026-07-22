package runtime

import (
	"context"
	"runtime"

	"github.com/cirruslabs/orchard/internal/dialer"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager"
	syntheticpkg "github.com/cirruslabs/orchard/internal/worker/vmmanager/synthetic"
	"github.com/cirruslabs/orchard/pkg/client"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type Synthetic struct{}

func NewSynthetic() *Synthetic {
	return &Synthetic{}
}

func (synthetic *Synthetic) ID() v1.Runtime {
	// Fake runtime depending on the OS
	if runtime.GOOS == "linux" {
		return v1.RuntimeVetu
	} else {
		return v1.RuntimeTart
	}
}

func (synthetic *Synthetic) Synthetic() bool {
	return true
}

func (synthetic *Synthetic) NewVM(
	vmResource v1.VM,
	eventStreamer *client.EventStreamer,
	vmPullTimeHistogram metric.Float64Histogram,
	_ dialer.Dialer,
	_ bool,
	logger *zap.SugaredLogger,
) vmmanager.VM {
	return syntheticpkg.NewVM(vmResource, eventStreamer, vmPullTimeHistogram, logger)
}

func (synthetic *Synthetic) ListVMs(ctx context.Context, logger *zap.SugaredLogger) ([]vmmanager.VMInfo, error) {
	return nil, nil
}

func (synthetic *Synthetic) Cmd(_ context.Context, _ *zap.SugaredLogger, _ ...string) (string, string, error) {
	return "", "", nil
}
