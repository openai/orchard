package base

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"

	"github.com/cirruslabs/orchard/internal/worker/vmmanager"
	"go.uber.org/zap"
)

func Cmd(
	ctx context.Context,
	logger *zap.SugaredLogger,
	commandName string,
	args ...string,
) (string, string, error) {
	cmd := exec.CommandContext(ctx, commandName, args...)

	var stdout, stderr bytes.Buffer

	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	logger.Debugf("running '%s %s'", commandName, strings.Join(args, " "))
	err := cmd.Run()
	if err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return "", "", fmt.Errorf("%s command not found in PATH, make sure %s is installed: %w",
				commandName, strings.ToTitle(commandName), err)
		}

		if exitErr, ok := err.(*exec.ExitError); ok {
			select {
			case <-ctx.Done():
				// Do not log an error because it's the user's intent to cancel this VM operation
			default:
				logger.Warnf(
					"'%s %s' failed with exit code %d: %s",
					commandName, strings.Join(args, " "),
					exitErr.ExitCode(), firstNonEmptyLine(stderr.String(), stdout.String()),
				)
			}

			// Preserve the exit status while adding the command-specific output.
			err = fmt.Errorf("%s command failed: %q: %w", commandName,
				firstNonEmptyLine(stderr.String(), stdout.String()), exitErr)
		}
	}

	return stdout.String(), stderr.String(), err
}

func List(ctx context.Context, logger *zap.SugaredLogger, commandName string) ([]vmmanager.VMInfo, error) {
	output, _, err := Cmd(ctx, logger, commandName, "list", "--format", "json")
	if err != nil {
		return nil, err
	}

	var entries []vmmanager.VMInfo

	if err := json.Unmarshal([]byte(output), &entries); err != nil {
		return nil, err
	}

	return entries, nil
}

func firstNonEmptyLine(outputs ...string) string {
	for _, output := range outputs {
		for _, line := range strings.Split(output, "\n") {
			if line != "" {
				return line
			}
		}
	}

	return ""
}
