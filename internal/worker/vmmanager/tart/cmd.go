package tart

import (
	"context"
	"encoding/json"

	"github.com/cirruslabs/orchard/internal/worker/vmmanager"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager/base"
	"go.uber.org/zap"
)

const tartCommandName = "tart"

func Tart(ctx context.Context, logger *zap.SugaredLogger, args ...string) (string, string, error) {
	return base.Cmd(ctx, logger, tartCommandName, args...)
}

func List(ctx context.Context, logger *zap.SugaredLogger) ([]vmmanager.VMInfo, error) {
	return base.List(ctx, logger, tartCommandName)
}

func Info(ctx context.Context, logger *zap.SugaredLogger, name string) (*vmmanager.VMInfo, error) {
	output, _, err := Tart(ctx, logger, "get", name, "--format", "json")
	if err != nil {
		return nil, err
	}

	info := &vmmanager.VMInfo{
		Name:    name,
		Source:  "local",
		State:   "",
		Running: false,
	}
	if err := json.Unmarshal([]byte(output), info); err != nil {
		return nil, err
	}

	return info, nil
}
