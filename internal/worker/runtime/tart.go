package runtime

import (
	"context"

	"github.com/cirruslabs/orchard/internal/dialer"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager"
	tartpkg "github.com/cirruslabs/orchard/internal/worker/vmmanager/tart"
	"github.com/cirruslabs/orchard/pkg/client"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type Tart struct{}

func NewTart() *Tart {
	return &Tart{}
}

func (tart *Tart) ID() v1.Runtime {
	return v1.RuntimeTart
}

func (tart *Tart) Synthetic() bool {
	return false
}

func (tart *Tart) NewVM(
	vmResource v1.VM,
	eventStreamer *client.EventStreamer,
	vmPullTimeHistogram metric.Float64Histogram,
	dialer dialer.Dialer,
	softnetPolicyUpdates bool,
	logger *zap.SugaredLogger,
) vmmanager.VM {
	return tartpkg.NewVM(vmResource, eventStreamer, vmPullTimeHistogram, dialer, softnetPolicyUpdates, logger)
}

func (tart *Tart) ListVMs(ctx context.Context, logger *zap.SugaredLogger) ([]vmmanager.VMInfo, error) {
	return tartpkg.List(ctx, logger)
}

func (tart *Tart) Cmd(ctx context.Context, logger *zap.SugaredLogger, args ...string) (string, string, error) {
	return tartpkg.Tart(ctx, logger, args...)
}
