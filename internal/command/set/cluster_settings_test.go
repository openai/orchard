//nolint:testpackage // Exercise the cluster-settings command without the root command's global flags.
package set

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetWorkersStartPaused(t *testing.T) {
	var mu sync.Mutex

	settings := v1.ClusterSettings{
		SchedulerProfile: v1.SchedulerProfileDistributeLoad,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/cluster-settings", func(writer http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		current := settings
		mu.Unlock()

		assert.NoError(t, json.NewEncoder(writer).Encode(current))
	})
	mux.HandleFunc("PUT /v1/cluster-settings", func(_ http.ResponseWriter, request *http.Request) {
		var updated v1.ClusterSettings
		assert.NoError(t, json.NewDecoder(request.Body).Decode(&updated))

		mu.Lock()
		settings = updated
		mu.Unlock()
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	t.Setenv("ORCHARD_HOME", t.TempDir())
	t.Setenv("ORCHARD_URL", server.URL)

	for _, paused := range []bool{true, false} {
		command := newSetClusterSettingsCommand()
		command.SetArgs([]string{"--workers-start-paused=" + strconv.FormatBool(paused)})

		require.NoError(t, command.ExecuteContext(t.Context()))

		mu.Lock()
		updated := settings
		mu.Unlock()

		require.Equal(t, paused, updated.WorkersStartPaused)
		require.Equal(t, v1.SchedulerProfileDistributeLoad, updated.SchedulerProfile)
	}
}
