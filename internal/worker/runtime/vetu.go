package runtime

import (
	"context"

	"github.com/cirruslabs/orchard/internal/dialer"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager"
	vetupkg "github.com/cirruslabs/orchard/internal/worker/vmmanager/vetu"
	"github.com/cirruslabs/orchard/pkg/client"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type Vetu struct{}

func NewVetu() *Vetu {
	return &Vetu{}
}

func (vetu *Vetu) ID() v1.Runtime {
	return v1.RuntimeVetu
}

func (vetu *Vetu) Synthetic() bool {
	return false
}

func (vetu *Vetu) NewVM(
	vmResource v1.VM,
	eventStreamer *client.EventStreamer,
	vmPullTimeHistogram metric.Float64Histogram,
	dialer dialer.Dialer,
	_ bool,
	logger *zap.SugaredLogger,
) vmmanager.VM {
	return vetupkg.NewVM(vmResource, eventStreamer, vmPullTimeHistogram, dialer, logger)
}

func (vetu *Vetu) ListVMs(ctx context.Context, logger *zap.SugaredLogger) ([]vmmanager.VMInfo, error) {
	return vetupkg.List(ctx, logger)
}

func (vetu *Vetu) Cmd(ctx context.Context, logger *zap.SugaredLogger, args ...string) (string, string, error) {
	return vetupkg.Vetu(ctx, logger, args...)
}
