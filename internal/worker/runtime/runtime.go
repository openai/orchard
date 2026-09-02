package runtime

import (
	"context"

	"github.com/cirruslabs/orchard/internal/dialer"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager"
	"github.com/cirruslabs/orchard/pkg/client"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type Runtime interface {
	ID() v1.Runtime
	Synthetic() bool
	NewVM(
		vmResource v1.VM,
		eventStreamer *client.EventStreamer,
		vmPullTimeHistogram metric.Float64Histogram,
		dialer dialer.Dialer,
		softnetPolicyUpdates bool,
		logger *zap.SugaredLogger,
	) vmmanager.VM
	ListVMs(ctx context.Context, logger *zap.SugaredLogger) ([]vmmanager.VMInfo, error)
	Cmd(ctx context.Context, logger *zap.SugaredLogger, args ...string) (string, string, error)
}
