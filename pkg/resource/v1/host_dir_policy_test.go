package v1_test

import (
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewHostDirPolicyFromString(t *testing.T) {
	policy, err := v1.NewHostDirPolicyFromString("/Users/ci/src:ro")
	require.NoError(t, err)
	require.EqualValues(t, v1.HostDirPolicy{
		PathPrefix: "/Users/ci/src",
		ReadOnly:   true,
	}, policy)

	_, err = v1.NewHostDirPolicyFromString("/Users/ci/src:ro:something")
	require.Error(t, err)

	_, err = v1.NewHostDirPolicyFromString("/Users/ci/src:rw")
	require.Error(t, err)
}

func TestHostDirPolicyValidate(t *testing.T) {
	policy := &v1.HostDirPolicy{PathPrefix: "/Users/ci/src"}

	// Valid uses
	require.True(t, policy.Validate("/Users/ci/src", true))
	require.True(t, policy.Validate("/Users/ci/src/", true))
	require.True(t, policy.Validate("/Users/ci/src/website", true))

	// Invalid uses
	require.False(t, policy.Validate("/Users/ci/", true))
	require.False(t, policy.Validate("/Users", true))
	require.False(t, policy.Validate("/tmp", true))
	require.False(t, policy.Validate("/", true))

	// No path traversal, even within the path prefix
	require.False(t, policy.Validate("/Users/ci/src/website/../../../../../../etc/passwd", true))
	require.False(t, policy.Validate("/Users/ci/src/website/..", true))
	require.False(t, policy.Validate("/Users/ci/src/..", true))
	require.False(t, policy.Validate("/Users/ci/..", true))
	require.False(t, policy.Validate("/Users/..", true))
	require.False(t, policy.Validate("/..", true))
}

func TestHostDirPolicyValidatePathBoundary(t *testing.T) {
	testCases := []struct {
		name       string
		pathPrefix string
		path       string
		allowed    bool
	}{
		{
			name:       "local policy allows its exact path",
			pathPrefix: "/src/",
			path:       "/src",
			allowed:    true,
		},
		{
			name:       "local policy allows descendants",
			pathPrefix: "/src/",
			path:       "/src/project",
			allowed:    true,
		},
		{
			name:       "local policy without trailing slash rejects sibling sharing its prefix",
			pathPrefix: "/src",
			path:       "/src-private",
		},
		{
			name:       "local policy rejects sibling sharing its prefix",
			pathPrefix: "/src/",
			path:       "/src-private",
		},
		{
			name:       "local root policy allows descendants",
			pathPrefix: "/",
			path:       "/src/project",
			allowed:    true,
		},
		{
			name:       "local root policy rejects remote URLs",
			pathPrefix: "/",
			path:       "https://github.com/archive.tar.gz",
		},
		{
			name:       "URL policy allows its exact host",
			pathPrefix: "https://github.com/",
			path:       "https://github.com",
			allowed:    true,
		},
		{
			name:       "URL policy allows paths on its host",
			pathPrefix: "https://github.com",
			path:       "https://github.com/actions/archive.tar.gz",
			allowed:    true,
		},
		{
			name:       "URL policy rejects lookalike host",
			pathPrefix: "https://github.com",
			path:       "https://github.com.attacker.com/archive.tar.gz",
		},
		{
			name:       "URL policy with trailing slash rejects lookalike host",
			pathPrefix: "https://github.com/",
			path:       "https://github.com.attacker.com/archive.tar.gz",
		},
		{
			name:       "URL policy rejects host concealed by userinfo",
			pathPrefix: "https://github.com",
			path:       "https://github.com@attacker.example/archive.tar.gz",
		},
		{
			name:       "URL path policy allows descendants",
			pathPrefix: "https://github.com/actions",
			path:       "https://github.com/actions/runner/archive.tar.gz",
			allowed:    true,
		},
		{
			name:       "URL path policy rejects sibling sharing its prefix",
			pathPrefix: "https://github.com/actions",
			path:       "https://github.com/actions-private/archive.tar.gz",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			policy := v1.HostDirPolicy{PathPrefix: testCase.pathPrefix}

			require.Equal(t, testCase.allowed, policy.Validate(testCase.path, false))
		})
	}
}

func TestHostDirPolicyValidateReadOnly(t *testing.T) {
	policy := &v1.HostDirPolicy{PathPrefix: "/Users/ci/src", ReadOnly: true}

	const desiredPath = "/Users/ci/src/website"

	// Only read-only is allowed
	require.True(t, policy.Validate(desiredPath, true))
	require.False(t, policy.Validate(desiredPath, false))
}

func TestHostDirPolicyString(t *testing.T) {
	policyRw := &v1.HostDirPolicy{PathPrefix: "/Users/ci/src"}
	require.EqualValues(t, "/Users/ci/src", policyRw.String())

	policyRo := &v1.HostDirPolicy{PathPrefix: "/Users/ci/src", ReadOnly: true}
	require.EqualValues(t, "/Users/ci/src:ro", policyRo.String())
}

func TestHTTPHostDirPolicyString(t *testing.T) {
	policy, err := v1.NewHostDirPolicyFromString("https://github.com/actions/runner/releases/download")
	require.NoError(t, err)
	require.EqualValues(t, v1.HostDirPolicy{
		PathPrefix: "https://github.com/actions/runner/releases/download",
		ReadOnly:   false,
	}, policy)
	//nolint: lll
	require.True(t, policy.Validate("https://github.com/actions/runner/releases/download/v2.309.0/actions-runner-osx-arm64-2.309.0.tar.gz", false))
}
