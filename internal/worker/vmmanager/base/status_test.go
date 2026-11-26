package base_test

import (
	"testing"

	"github.com/cirruslabs/orchard/internal/worker/ondiskname"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager/base"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestVMStatusTransitionsAreVisibleAtDefaultLogLevel(t *testing.T) {
	core, observed := observer.New(zap.InfoLevel)
	id := ondiskname.New("test-vm", "00112233-4455-6677-8899-aabbccddeeff", 2)
	vm := base.NewVM(v1.VM{}, id, zap.New(core).Sugar())
	require.Empty(t, vm.StatusMessage())

	statuses := []string{
		"pulling VM image...",
		"cloning VM...",
		"configuring VM...",
		"VM started",
	}
	for _, status := range statuses {
		vm.SetStatusMessage(status)
		vm.SetStatusMessage(status)
		require.Equal(t, status, vm.StatusMessage())
	}

	entries := observed.All()
	require.Len(t, entries, len(statuses))
	previous := ""
	for i, entry := range entries {
		require.Equal(t, "VM status changed", entry.Message)
		require.Equal(t, zap.InfoLevel, entry.Level)
		require.Equal(t, map[string]interface{}{
			"vm":              "test-vm",
			"on_disk_name":    id.String(),
			"previous_status": previous,
			"status":          statuses[i],
		}, entry.ContextMap())
		previous = statuses[i]
	}
}
