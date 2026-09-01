package tart //nolint:testpackage // VM fixtures require private state without starting clone or run goroutines.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/cirruslabs/orchard/internal/worker/ondiskname"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager/base"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestDeleteIsIdempotent(t *testing.T) {
	vm, vmPath := newVMForDelete(t, `#!/bin/sh
[ "$#" -eq 2 ] && [ "$1" = delete ] || exit 64
vm_dir="$TART_HOME/vms/$2"
if [ ! -e "$vm_dir" ]; then
  printf 'the specified VM "%s" does not exist\n' "$2" >&2
  exit 2
fi
/bin/rm -r "$vm_dir"
`)
	require.NoError(t, os.MkdirAll(vmPath, 0o700))

	require.NoError(t, vm.Delete())
	require.NoDirExists(t, vmPath)
	require.NoError(t, vm.Delete(), "deleting the same VM again must succeed")

	select {
	case <-vm.ctx.Done():
	default:
		t.Fatal("Delete did not cancel the VM context")
	}
}

func TestDeletePreservesCommandFailures(t *testing.T) {
	for _, exitCode := range []int{1, 64, 126} {
		t.Run(fmt.Sprintf("exit-%d", exitCode), func(t *testing.T) {
			vm, _ := newVMForDelete(t, fmt.Sprintf(
				"#!/bin/sh\nprintf 'the specified VM does not exist: permission denied\\n' >&2\nexit %d\n",
				exitCode,
			))

			err := vm.Delete()

			require.ErrorIs(t, err, base.ErrVMFailed)
			require.ErrorContains(t, err, "permission denied")
			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr)
			require.Equal(t, exitCode, exitErr.ExitCode())
		})
	}
}

func TestDeletePreservesProcessStartFailure(t *testing.T) {
	vm, _ := newVMForDelete(t, "#!/nonexistent/tart-interpreter\n")

	err := vm.Delete()

	require.ErrorIs(t, err, base.ErrVMFailed)
	require.ErrorIs(t, err, os.ErrNotExist)
	var pathErr *os.PathError
	require.ErrorAs(t, err, &pathErr)
}

func TestDeletePreservesMissingExecutable(t *testing.T) {
	vm, _ := newVMForDelete(t, "#!/bin/sh\nexit 0\n")
	t.Setenv("PATH", t.TempDir())

	err := vm.Delete()

	require.ErrorIs(t, err, base.ErrVMFailed)
	require.ErrorIs(t, err, exec.ErrNotFound)
	require.ErrorContains(t, err, "tart command not found in PATH")
}

func newVMForDelete(t *testing.T, script string) (*VM, string) {
	t.Helper()

	binDir := t.TempDir()
	commandPath := filepath.Join(binDir, "tart")
	require.NoError(t, os.WriteFile(commandPath, []byte(script), 0o600))
	require.NoError(t, os.Chmod(commandPath, 0o700)) //nolint:gosec // Fake Tart must be executable.
	t.Setenv("PATH", binDir)
	tartHome := t.TempDir()
	t.Setenv("TART_HOME", tartHome)

	logger := zap.NewNop().Sugar()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	vm := &VM{ //nolint:exhaustruct_v5 // The fixture initializes only the state used by Delete.
		onDiskName: ondiskname.New("delete-test", "11111111-2222-4333-8444-555555555555", 0),
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
		VM:         base.NewVM(logger),
	}
	vm.ConditionsSet().Remove(v1.ConditionTypeCloning)

	return vm, filepath.Join(tartHome, "vms", vm.id())
}
