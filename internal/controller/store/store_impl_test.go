package store_test

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	storepkg "github.com/cirruslabs/orchard/internal/controller/store"
	"github.com/cirruslabs/orchard/internal/controller/store/badger"
	etcdstore "github.com/cirruslabs/orchard/internal/controller/store/etcd"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type storeImpl struct {
	Name string
	Init func(t *testing.T) storepkg.Store
}

func testStores(logger *zap.SugaredLogger) []storeImpl {
	storeImpls := []storeImpl{
		{
			Name: "badger",
			Init: func(t *testing.T) storepkg.Store {
				store, err := badger.NewBadgerStore(t.TempDir(), true, logger)
				require.NoError(t, err)

				return store
			},
		},
	}

	etcdEndpointsRaw := os.Getenv("ORCHARD_TEST_ETCD_ENDPOINTS")
	if etcdEndpointsRaw != "" {
		storeImpls = append(storeImpls, storeImpl{
			Name: "etcd",
			Init: func(t *testing.T) storepkg.Store {
				prefix := fmt.Sprintf("/orchard-tests/%s-%d", strings.ReplaceAll(t.Name(), "/", "-"), time.Now().UnixNano())
				store, err := etcdstore.NewEtcdStore(splitEndpoints(etcdEndpointsRaw), prefix, logger)
				require.NoError(t, err)

				return store
			},
		})
	}

	return storeImpls
}

func splitEndpoints(rawEndpoints string) []string {
	var endpoints []string
	for _, endpoint := range strings.Split(rawEndpoints, ",") {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" {
			continue
		}

		endpoints = append(endpoints, endpoint)
	}

	return endpoints
}
