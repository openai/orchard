package base_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/cirruslabs/orchard/internal/worker/vmmanager/base"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestCmdPreservesExitStatus(t *testing.T) {
	for _, commandName := range []string{"tart", "vetu"} {
		t.Run(commandName, func(t *testing.T) {
			commandPath := filepath.Join(t.TempDir(), commandName)
			script := `#!/bin/sh
printf 'command output\n'
printf '\ncommand failed\nmore detail\n' >&2
exit "$1"
`
			require.NoError(t, os.WriteFile(commandPath, []byte(script), 0o600))
			require.NoError(t, os.Chmod(commandPath, 0o700)) //nolint:gosec // The fake command must be executable.

			for _, exitCode := range []int{1, 2, 64} {
				t.Run("exit-"+strconv.Itoa(exitCode), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()

					stdout, stderr, err := base.Cmd(ctx, zap.NewNop().Sugar(), commandPath, strconv.Itoa(exitCode))

					require.Equal(t, "command output\n", stdout)
					require.Equal(t, "\ncommand failed\nmore detail\n", stderr)
					require.ErrorContains(t, err, "command failed")
					var exitErr *exec.ExitError
					require.ErrorAs(t, err, &exitErr)
					require.Equal(t, exitCode, exitErr.ExitCode())
				})
			}
		})
	}
}

func TestCmdPreservesSignalStatus(t *testing.T) {
	commandPath := filepath.Join(t.TempDir(), "signalled-command")
	require.NoError(t, os.WriteFile(commandPath, []byte("#!/bin/sh\nkill -TERM $$\n"), 0o600))
	require.NoError(t, os.Chmod(commandPath, 0o700)) //nolint:gosec // The fake command must be executable.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_, _, err := base.Cmd(ctx, zap.NewNop().Sugar(), commandPath)

	require.NoError(t, ctx.Err(), "the command must terminate before the test deadline")
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, -1, exitErr.ExitCode())
}
