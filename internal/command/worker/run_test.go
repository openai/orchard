//go:build unix

package worker

import (
	"strings"
	"testing"

	"github.com/cirruslabs/orchard/internal/bootstraptoken"
	"github.com/stretchr/testify/require"
)

func TestWorkerBootstrapTokenGuidance(t *testing.T) {
	for _, raw := range []string{"bootstrap-admin", "synthetic-raw-token"} {
		_, err := parseWorkerBootstrapToken(raw)
		require.ErrorIs(t, err, bootstraptoken.ErrInvalidBootstrapTokenFormat)
		require.ErrorContains(t, err, "orchard get bootstrap-token <worker-service-account>")
		require.ErrorContains(t, err, "not a service account name or raw service account token")
		require.NotContains(t, err.Error(), raw)
	}
	token, err := bootstraptoken.New(nil, "test-worker", "synthetic-token")
	require.NoError(t, err)
	parsed, err := parseWorkerBootstrapToken(token.String())
	require.NoError(t, err)
	require.Equal(t, token.ServiceAccountName(), parsed.ServiceAccountName())
	require.Equal(t, token.ServiceAccountToken(), parsed.ServiceAccountToken())

	flag := newRunCommand().Flags().Lookup("bootstrap-token")
	require.NotNil(t, flag)
	require.True(t, strings.Contains(flag.Usage, "not the account name"))
}
