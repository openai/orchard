package tart

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/cirruslabs/orchard/internal/worker/vmmanager"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager/base"
	goversion "github.com/hashicorp/go-version"
	"go.uber.org/zap"
)

const tartCommandName = "tart"

func Tart(ctx context.Context, logger *zap.SugaredLogger, args ...string) (string, string, error) {
	return TartWithExtraFiles(ctx, logger, nil, args...)
}

func Version(ctx context.Context, logger *zap.SugaredLogger) (*goversion.Version, error) {
	stdout, _, err := Tart(ctx, logger, "--version")
	if err != nil {
		return nil, err
	}

	tartVersion, err := goversion.NewSemver(strings.TrimSpace(stdout))
	if err != nil {
		return nil, fmt.Errorf("failed to parse Tart version: %w", err)
	}

	return tartVersion, nil
}

func TartWithExtraFiles(
	ctx context.Context,
	logger *zap.SugaredLogger,
	extraFiles []*os.File,
	args ...string,
) (string, string, error) {
	return base.CmdWithExtraFiles(ctx, logger, tartCommandName, extraFiles, args...)
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
