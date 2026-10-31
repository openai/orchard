//nolint:testpackage // we need to test the unexported VNC URL helper
package vnc

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewVNCURL(t *testing.T) {
	const host = "127.0.0.1:5900"

	require.Equal(t, "vnc://127.0.0.1:5900", newVNCURL(host, "", ""))
	require.Equal(t, "vnc://admin@127.0.0.1:5900", newVNCURL(host, "admin", ""))
	require.Equal(t, "vnc://admin:admin@127.0.0.1:5900", newVNCURL(host, "admin", "admin"))
}

func TestNewVNCURLEscapesCredentials(t *testing.T) {
	const host = "127.0.0.1:5900"

	for _, password := range []string{"p@ss", "p:ss", "p/ss", "p?ss", "p#ss", "p%ss"} {
		parsedURL, err := url.Parse(newVNCURL(host, "admin", password))
		require.NoError(t, err, password)
		require.Equal(t, host, parsedURL.Host, password)
		require.Equal(t, "admin", parsedURL.User.Username(), password)

		parsedPassword, ok := parsedURL.User.Password()
		require.True(t, ok, password)
		require.Equal(t, password, parsedPassword, password)
	}
}
