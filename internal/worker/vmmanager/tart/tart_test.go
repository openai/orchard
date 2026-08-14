//nolint:exhaustruct_v5,goconst // Fixtures omit unrelated fields and keep expected command strings explicit.
package tart //nolint:testpackage // Exercise clone and configuration through the real command runner.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cirruslabs/orchard/internal/worker/ondiskname"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager/base"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestCloneAndConfigurePreservesSuspendedVM(t *testing.T) {
	for _, failedCommand := range []string{"", "fqn"} {
		t.Run("failed command="+failedCommand, func(t *testing.T) {
			commandLog := installCloneFakeTart(t,
				`{"OS":"darwin","CPU":4,"Memory":8192,"Disk":50,"Running":false,"State":"suspended"}`,
				failedCommand)
			vm := newCloneTestVM(v1.VM{
				Name:           "test-vm",
				UID:            "00112233-4455-6677-8899-aabbccddeeff",
				Image:          "source-image",
				CPU:            2,
				AssignedCPU:    6,
				Memory:         4096,
				AssignedMemory: 12288,
				DiskSize:       100,
				RandomSerial:   true,
			})
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			require.NoError(t, vm.cloneAndConfigure(ctx))
			requireCloneCommands(t, commandLog, []string{
				"clone source-image " + vm.id(),
				"fqn source-image",
				"get " + vm.id() + " --format json",
			})
			require.False(t, vm.ConditionsSet().ContainsOne(v1.ConditionTypeCloning))
			if failedCommand == "fqn" {
				require.Nil(t, vm.ImageFQN())
			} else {
				require.NotNil(t, vm.ImageFQN())
				require.Equal(t, "registry.example/source@sha256:abc", *vm.ImageFQN())
			}
		})
	}
}

func TestCloneAndConfigureConfiguresStoppedVM(t *testing.T) {
	tests := []struct {
		name     string
		resource v1.VM
		setArgs  []string
	}{
		{
			name: "requested resources",
			resource: v1.VM{
				CPU:          2,
				Memory:       4096,
				DiskSize:     100,
				RandomSerial: true,
			},
			setArgs: []string{"--memory 4096", "--cpu 2", "--disk-size 100", "--random-mac", "--random-serial"},
		},
		{
			name: "assigned resources override requested resources",
			resource: v1.VM{
				CPU:            2,
				AssignedCPU:    6,
				Memory:         4096,
				AssignedMemory: 12288,
			},
			setArgs: []string{"--memory 12288", "--cpu 6", "--random-mac"},
		},
		{
			name:    "image resource defaults",
			setArgs: []string{"--random-mac"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			commandLog := installCloneFakeTart(t, `{"Running":false,"State":"stopped"}`, "")
			test.resource.Name = "test-vm"
			test.resource.UID = "00112233-4455-6677-8899-aabbccddeeff"
			test.resource.Image = "source-image"
			vm := newCloneTestVM(test.resource)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			require.NoError(t, vm.cloneAndConfigure(ctx))
			commands := []string{
				"clone source-image " + vm.id(),
				"fqn source-image",
				"get " + vm.id() + " --format json",
			}
			for _, args := range test.setArgs {
				commands = append(commands, "set "+args+" "+vm.id())
			}
			requireCloneCommands(t, commandLog, commands)
		})
	}
}

func TestCloneAndConfigureStopsOnMetadataError(t *testing.T) {
	tests := []struct {
		name          string
		output        string
		failedCommand string
		wantError     string
	}{
		{
			name:          "get command fails",
			failedCommand: "get",
			wantError:     "injected get failure",
		},
		{
			name:      "invalid JSON",
			output:    `{"State":`,
			wantError: "unexpected end of JSON input",
		},
		{
			name:      "invalid state type",
			output:    `{"State":123}`,
			wantError: "cannot unmarshal number",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			commandLog := installCloneFakeTart(t, test.output, test.failedCommand)
			vm := newCloneTestVM(v1.VM{
				Name:   "test-vm",
				UID:    "00112233-4455-6677-8899-aabbccddeeff",
				Image:  "source-image",
				Memory: 4096,
			})
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			err := vm.cloneAndConfigure(ctx)
			require.ErrorContains(t, err, test.wantError)
			if test.name == "invalid JSON" {
				var syntaxError *json.SyntaxError
				require.ErrorAs(t, err, &syntaxError)
			}
			if test.name == "invalid state type" {
				var typeError *json.UnmarshalTypeError
				require.ErrorAs(t, err, &typeError)
			}
			requireCloneCommands(t, commandLog, []string{
				"clone source-image " + vm.id(),
				"fqn source-image",
				"get " + vm.id() + " --format json",
			})
		})
	}
}

func newCloneTestVM(resource v1.VM) *VM {
	logger := zap.NewNop().Sugar()

	return &VM{
		onDiskName: ondiskname.NewFromResource(resource),
		resource:   resource,
		logger:     logger,
		VM:         base.NewVM(resource, ondiskname.NewFromResource(resource), logger),
	}
}

func requireCloneCommands(t *testing.T, commandLog string, commands []string) {
	t.Helper()

	logged, err := os.ReadFile(commandLog) //nolint:gosec // The command log is created in t.TempDir.
	require.NoError(t, err)
	require.Equal(t, commands, strings.Split(strings.TrimSpace(string(logged)), "\n"))
}

func installCloneFakeTart(t *testing.T, infoOutput string, failedCommand string) string {
	t.Helper()

	dir := t.TempDir()
	commandLog := filepath.Join(dir, "commands.log")
	script := `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$ORCHARD_TEST_TART_COMMAND_LOG"
if [ "$1" = "$ORCHARD_TEST_TART_FAILED_COMMAND" ]; then
    printf 'injected %s failure\n' "$1" >&2
    exit 1
fi
case "$1" in
    clone|set) ;;
    fqn) printf 'registry.example/source@sha256:abc\n' ;;
    get) printf '%s\n' "$ORCHARD_TEST_TART_INFO" ;;
    *) printf 'unexpected command: %s\n' "$*" >&2; exit 1 ;;
esac
`
	commandPath := filepath.Join(dir, tartCommandName)
	require.NoError(t, os.WriteFile(commandPath, []byte(script), 0o600))
	require.NoError(t, os.Chmod(commandPath, 0o700)) //nolint:gosec // The fake Tart command must be executable.
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("ORCHARD_TEST_TART_COMMAND_LOG", commandLog)
	t.Setenv("ORCHARD_TEST_TART_INFO", infoOutput)
	t.Setenv("ORCHARD_TEST_TART_FAILED_COMMAND", failedCommand)

	return commandLog
}
