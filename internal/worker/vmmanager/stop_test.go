package vmmanager_test

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cirruslabs/orchard/internal/worker/vmmanager"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager/tart"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager/vetu"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const stopTestTimeout = 15 * time.Second

func TestStopWaitsForCommandAndRun(t *testing.T) {
	runtimes := []struct {
		name string
		new  func(v1.VM) vmmanager.VM
	}{
		{name: "tart", new: func(resource v1.VM) vmmanager.VM {
			return tart.NewVM(resource, nil, nil, nil, false, zap.NewNop().Sugar())
		}},
		{name: "vetu", new: func(resource v1.VM) vmmanager.VM {
			return vetu.NewVM(resource, nil, nil, nil, zap.NewNop().Sugar())
		}},
	}

	for _, runtime := range runtimes {
		for _, commandFirst := range []bool{true, false} {
			order := "run finishes first"
			if commandFirst {
				order = "stop command finishes first"
			}
			t.Run(runtime.name+"/"+order, func(t *testing.T) {
				checkStopCompletion(t, runtime.name, runtime.new, commandFirst)
			})
		}
	}
}

//nolint:exhaustruct_v5 // VM settings unrelated to lifecycle transitions use their defaults.
func checkStopCompletion(t *testing.T, commandName string, newVM func(v1.VM) vmmanager.VM, commandFirst bool) {
	t.Helper()

	dir := installStopTestCommand(t, commandName)
	runGate := filepath.Join(dir, "run.release")
	stopGate := filepath.Join(dir, "stop.release")
	release := func(path string) { require.NoError(t, os.WriteFile(path, nil, 0o600)) }
	waitForFile := func(path string) {
		require.Eventually(t, func() bool {
			_, err := os.Stat(path)
			return err == nil
		}, stopTestTimeout, 5*time.Millisecond, "command did not reach %s", path)
	}
	waitForStop := func(done <-chan error) {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(stopTestTimeout):
			t.Fatal("Stop did not complete after the commands finished")
		}
	}

	resource := v1.VM{Name: "stop-test", UID: "test-uid", Image: "source-image"}
	vm := newVM(resource)
	var first <-chan error
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("VM status before fixture cleanup: %s; error: %v", vm.StatusMessage(), vm.Err())
		}
		release(runGate)
		release(stopGate)
		if first != nil {
			waitForStop(first)
		}
		waitForStop(vm.Stop())
	})

	// A restart must create a new Stop completion for the new run.
	for run := range 2 {
		if run != 0 {
			for _, name := range []string{"run.release", "stop.release", "run.started", "stop.started", "stop.finished"} {
				require.NoError(t, os.Remove(filepath.Join(dir, name)))
			}
			vm.Start(nil)
		}
		waitForFile(filepath.Join(dir, "run.started"))
		first = vm.Stop()
		waitForFile(filepath.Join(dir, "stop.started"))

		if commandFirst {
			release(stopGate)
			waitForFile(filepath.Join(dir, "stop.finished"))
			pidText, err := os.ReadFile(filepath.Join(dir, "run.started")) //nolint:gosec // Read a t.TempDir fixture.
			require.NoError(t, err)
			pid, err := strconv.Atoi(strings.TrimSpace(string(pidText)))
			require.NoError(t, err)
			// Cancellation kills the run process, but its child still holds
			// stdout open, so Cmd.Run and the VM goroutine cannot finish yet.
			require.Eventually(t, func() bool {
				return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
			}, stopTestTimeout, 5*time.Millisecond, "Stop did not cancel the run process")
		} else {
			release(runGate)
			require.Eventually(t, func() bool {
				return v1.ConditionIsFalse(vm.Conditions(), v1.ConditionTypeRunning)
			}, stopTestTimeout, 5*time.Millisecond, "the run did not finish")
		}

		callers := make(chan (<-chan error), 8)
		for range cap(callers) {
			go func() { callers <- vm.Stop() }()
		}
		stops := []<-chan error{first}
		for range cap(callers) {
			select {
			case done := <-callers:
				stops = append(stops, done)
			case <-time.After(stopTestTimeout):
				t.Fatal("concurrent Stop call did not return its completion channel")
			}
		}
		for _, done := range stops {
			select {
			case <-done:
				t.Fatal("Stop completed while shutdown was still in progress")
			default:
			}
		}
		// Reconciliation must still see Stopping when the run has ended
		// but its stop command has not returned.
		require.True(t, v1.ConditionIsTrue(vm.Conditions(), v1.ConditionTypeStopping))

		release(runGate)
		release(stopGate)
		for _, done := range stops {
			waitForStop(done)
		}
		waitForStop(vm.Stop())
		require.False(t, v1.ConditionIsTrue(vm.Conditions(), v1.ConditionTypeRunning))
		require.False(t, v1.ConditionIsTrue(vm.Conditions(), v1.ConditionTypeStopping))
		require.NoError(t, vm.Err())
	}
	commands, err := os.ReadFile(filepath.Join(dir, "commands.log")) //nolint:gosec // Read a t.TempDir fixture.
	require.NoError(t, err)
	require.Equal(t, 2, strings.Count(string(commands), "stop --timeout 5 "),
		"concurrent callers must share one stop command per run")
}

func installStopTestCommand(t *testing.T, name string) string {
	t.Helper()

	dir := t.TempDir()
	const script = `#!/bin/sh
set -eu
wait_for_file() {
  attempts=0
  while [ ! -f "$1" ]; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge "$ORCHARD_STOP_TEST_WAIT_ATTEMPTS" ]; then exit 1; fi
    /bin/sleep 0.01
  done
}
printf '%s\n' "$*" >> "$ORCHARD_STOP_TEST_DIR/commands.log"
case "$1" in
  get)
    printf '%s\n' '{"Running":false,"State":"stopped"}'
    ;;
  run)
    # Keep the child's output pipes open after cancellation kills this shell.
    wait_for_file "$ORCHARD_STOP_TEST_DIR/run.release" &
    printf '%s\n' "$$" > "$ORCHARD_STOP_TEST_DIR/run.started"
    wait
    ;;
  stop)
    : > "$ORCHARD_STOP_TEST_DIR/stop.started"
    wait_for_file "$ORCHARD_STOP_TEST_DIR/stop.release"
    : > "$ORCHARD_STOP_TEST_DIR/stop.finished"
    ;;
esac
`
	commandPath := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(commandPath, []byte(script), 0o600))
	require.NoError(t, os.Chmod(commandPath, 0o700)) //nolint:gosec // Fake commands must be executable.
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("ORCHARD_STOP_TEST_DIR", dir)
	// A command can wait through several Go-side milestones before its gate opens.
	// Keep it bounded, with enough time for every milestone under process load.
	t.Setenv("ORCHARD_STOP_TEST_WAIT_ATTEMPTS", strconv.Itoa(int(8*stopTestTimeout/(10*time.Millisecond))))
	return dir
}
