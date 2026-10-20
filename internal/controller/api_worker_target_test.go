//nolint:testpackage // Exercise the worker handler with the real transactional store.
package controller

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	storepkg "github.com/cirruslabs/orchard/internal/controller/store"
	"github.com/cirruslabs/orchard/internal/controller/store/badger"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	workerTargetFixture = "target"
	workerOtherFixture  = "other"
)

func TestWorkerUpdateUsesPathIdentity(t *testing.T) {
	for _, bodyName := range []string{"", workerTargetFixture, workerOtherFixture, "unknown"} {
		t.Run(bodyName, func(t *testing.T) {
			logger := zap.NewNop().Sugar()
			store, err := badger.NewBadgerStore(t.TempDir(), true, logger)
			require.NoError(t, err)
			var target, other v1.Worker
			target.Name, target.MachineID = workerTargetFixture, "target-machine"
			other.Name, other.MachineID = workerOtherFixture, "other-machine"
			target.CreatedAt = time.Unix(100, 0).UTC()
			other.CreatedAt = target.CreatedAt
			require.NoError(t, store.Update(func(txn storepkg.Transaction) error {
				if err := txn.SetWorker(target); err != nil {
					return err
				}
				return txn.SetWorker(other)
			}))
			controller := Controller{store: store, logger: logger, insecureAuthDisabled: true}
			engine := gin.New()
			engine.PUT("/workers/:name", func(ctx *gin.Context) { controller.updateWorker(ctx).Respond(ctx) })
			lastSeen := time.Unix(200, 0).UTC()
			body, err := json.Marshal(map[string]any{"name": bodyName, "scheduling_paused": true, "last_seen": lastSeen})
			require.NoError(t, err)
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/workers/target", bytes.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			engine.ServeHTTP(response, request)
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			var updated v1.Worker
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &updated))
			require.Equal(t, workerTargetFixture, updated.Name)
			require.True(t, updated.SchedulingPaused)
			require.Equal(t, target.MachineID, updated.MachineID)
			require.NoError(t, store.View(func(txn storepkg.Transaction) error {
				actual, err := txn.GetWorker(workerTargetFixture)
				require.NoError(t, err)
				require.True(t, actual.SchedulingPaused)
				require.Equal(t, lastSeen, actual.LastSeen)
				require.Equal(t, target.CreatedAt, actual.CreatedAt)
				unrelated, err := txn.GetWorker(workerOtherFixture)
				require.NoError(t, err)
				require.False(t, unrelated.SchedulingPaused)
				require.Equal(t, other.MachineID, unrelated.MachineID)
				return nil
			}))
		})
	}
}

func TestMissingWorkerPathDoesNotUpdateBodyIdentity(t *testing.T) {
	logger := zap.NewNop().Sugar()
	store, err := badger.NewBadgerStore(t.TempDir(), true, logger)
	require.NoError(t, err)
	var worker v1.Worker
	worker.Name = workerOtherFixture
	require.NoError(t, store.Update(func(txn storepkg.Transaction) error { return txn.SetWorker(worker) }))
	controller := Controller{store: store, logger: logger, insecureAuthDisabled: true}
	engine := gin.New()
	engine.PUT("/workers/:name", func(ctx *gin.Context) { controller.updateWorker(ctx).Respond(ctx) })
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/workers/missing",
		bytes.NewBufferString(`{"name":"other","scheduling_paused":true}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)
	require.Equal(t, http.StatusNotFound, response.Code)
	require.NoError(t, store.View(func(txn storepkg.Transaction) error {
		actual, err := txn.GetWorker(workerOtherFixture)
		require.NoError(t, err)
		require.False(t, actual.SchedulingPaused)
		return nil
	}))
}
