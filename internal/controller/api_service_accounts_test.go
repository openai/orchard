//nolint:testpackage // we need to test unexported API handlers
package controller

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	storepkg "github.com/cirruslabs/orchard/internal/controller/store"
	"github.com/cirruslabs/orchard/internal/controller/store/badger"
	v1pkg "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestUpdateServiceAccountRejectsBodyNameMismatch(t *testing.T) {
	controller := newServiceAccountTestController(t)

	require.NoError(t, controller.EnsureServiceAccount(&v1pkg.ServiceAccount{
		Meta:  v1pkg.Meta{Name: "alice"},
		Token: "alice-token",
		Roles: []v1pkg.ServiceAccountRole{v1pkg.ServiceAccountRoleAdminRead},
	}))
	require.NoError(t, controller.EnsureServiceAccount(&v1pkg.ServiceAccount{
		Meta:  v1pkg.Meta{Name: "bob"},
		Token: "bob-token",
		Roles: []v1pkg.ServiceAccountRole{v1pkg.ServiceAccountRoleAdminRead},
	}))

	ctx, recorder := serviceAccountUpdateContext(t, "alice", v1pkg.ServiceAccount{
		Meta:  v1pkg.Meta{Name: "bob"},
		Token: "new-token",
		Roles: []v1pkg.ServiceAccountRole{v1pkg.ServiceAccountRoleAdminWrite},
	})

	controller.updateServiceAccount(ctx).Respond(ctx)

	require.Equal(t, http.StatusPreconditionFailed, recorder.Code)
	require.Equal(t, "alice-token", getServiceAccount(t, controller, "alice").Token)
	require.Equal(t, "bob-token", getServiceAccount(t, controller, "bob").Token)
}

func TestUpdateServiceAccountUsesPathName(t *testing.T) {
	controller := newServiceAccountTestController(t)

	require.NoError(t, controller.EnsureServiceAccount(&v1pkg.ServiceAccount{
		Meta:  v1pkg.Meta{Name: "alice"},
		Token: "old-token",
		Roles: []v1pkg.ServiceAccountRole{v1pkg.ServiceAccountRoleAdminRead},
	}))

	ctx, recorder := serviceAccountUpdateContext(t, "alice", v1pkg.ServiceAccount{
		Meta:  v1pkg.Meta{Name: "alice"},
		Token: "new-token",
		Roles: []v1pkg.ServiceAccountRole{v1pkg.ServiceAccountRoleAdminWrite},
	})

	controller.updateServiceAccount(ctx).Respond(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)

	serviceAccount := getServiceAccount(t, controller, "alice")
	require.Equal(t, "new-token", serviceAccount.Token)
	require.Equal(t, []v1pkg.ServiceAccountRole{v1pkg.ServiceAccountRoleAdminWrite}, serviceAccount.Roles)
}

func newServiceAccountTestController(t *testing.T) *Controller {
	t.Helper()

	logger := zap.NewNop().Sugar()
	store, err := badger.NewBadgerStore(t.TempDir(), true, logger)
	require.NoError(t, err)

	return &Controller{
		store:                store,
		logger:               logger,
		insecureAuthDisabled: true,
	}
}

func serviceAccountUpdateContext(
	t *testing.T,
	name string,
	serviceAccount v1pkg.ServiceAccount,
) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()

	body, err := json.Marshal(serviceAccount)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "name", Value: name}}
	ctx.Request = httptest.NewRequest(http.MethodPut, "/service-accounts/"+name, bytes.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")

	return ctx, recorder
}

func getServiceAccount(t *testing.T, controller *Controller, name string) *v1pkg.ServiceAccount {
	t.Helper()

	var serviceAccount *v1pkg.ServiceAccount
	err := controller.store.View(func(txn storepkg.Transaction) error {
		var err error
		serviceAccount, err = txn.GetServiceAccount(name)

		return err
	})
	require.NoError(t, err)

	return serviceAccount
}
