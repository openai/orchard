package client_test

import (
	"context"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cirruslabs/orchard/internal/config"
	"github.com/cirruslabs/orchard/pkg/client"
	"github.com/cirruslabs/orchard/rpc"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const requestTimeout = 5 * time.Second

func TestWithCredentialsAndDefaultContext(t *testing.T) {
	const (
		explicitName  = "alice"
		explicitToken = "alice-token"
		defaultName   = "bob"
		defaultToken  = "bob-token"
	)

	for _, tc := range []struct {
		name                string
		contextName         string
		contextToken        string
		envCredentials      bool
		opts                []client.Option
		wantName, wantToken string
	}{
		{
			name:     "explicit credentials with address-only context",
			opts:     []client.Option{client.WithCredentials(explicitName, explicitToken)},
			wantName: explicitName, wantToken: explicitToken,
		},
		{
			name:        "explicit credentials override context",
			contextName: defaultName, contextToken: defaultToken,
			opts:     []client.Option{client.WithCredentials(explicitName, explicitToken)},
			wantName: explicitName, wantToken: explicitToken,
		},
		{
			name:        "explicit credentials override environment",
			contextName: defaultName, contextToken: defaultToken, envCredentials: true,
			opts:     []client.Option{client.WithCredentials(explicitName, explicitToken)},
			wantName: explicitName, wantToken: explicitToken,
		},
		{
			name:        "context credentials are defaults",
			contextName: defaultName, contextToken: defaultToken,
			wantName: defaultName, wantToken: defaultToken,
		},
		{
			name:        "environment credentials are defaults",
			contextName: defaultName, contextToken: defaultToken, envCredentials: true,
			wantName: "env-account", wantToken: "env-token",
		},
		{
			name:        "incomplete explicit credentials are not mixed with defaults",
			contextName: defaultName, contextToken: defaultToken,
			opts: []client.Option{client.WithCredentials(explicitName, "")},
		},
		{
			name:        "token-only explicit credentials are not mixed with defaults",
			contextName: defaultName, contextToken: defaultToken,
			opts: []client.Option{client.WithCredentials("", explicitToken)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ORCHARD_HOME", t.TempDir())
			for _, name := range []string{
				config.OrchardURL,
				config.OrchardServiceAccountName,
				config.OrchardServiceAccountToken,
			} {
				t.Setenv(name, "")
				require.NoError(t, os.Unsetenv(name))
			}
			if tc.envCredentials {
				t.Setenv(config.OrchardServiceAccountName, "env-account")
				t.Setenv(config.OrchardServiceAccountToken, "env-token")
			}

			wantAuth := ""
			if tc.wantName != "" {
				wantAuth = "Basic " + base64.StdEncoding.EncodeToString([]byte(tc.wantName+":"+tc.wantToken))
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, wantAuth, r.Header.Get("Authorization"))
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(server.Close)
			handle, err := config.NewHandle()
			require.NoError(t, err)
			require.NoError(t, handle.CreateContext("default", config.Context{
				URL: server.URL, ServiceAccountName: tc.contextName, ServiceAccountToken: tc.contextToken,
			}, false))

			controller, err := client.New(tc.opts...)
			require.NoError(t, err)
			require.NoError(t, controller.Check(t.Context()))
			metadata := controller.GPRCMetadata()
			if tc.wantName == "" {
				assert.Empty(t, metadata)
			} else {
				assert.Equal(t, []string{tc.wantName}, metadata.Get(rpc.MetadataServiceAccountNameKey))
				assert.Equal(t, []string{tc.wantToken}, metadata.Get(rpc.MetadataServiceAccountTokenKey))
			}
		})
	}
}

func TestWithHTTPClientControlsRedirectsAndConnections(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			http.Redirect(w, r, "/redirected", http.StatusTemporaryRedirect)
		}),
	)
	t.Cleanup(server.Close)

	httpClient := server.Client()
	httpClient.Timeout = requestTimeout
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	t.Cleanup(httpClient.CloseIdleConnections)

	controller, err := client.New(client.WithAddress(server.URL), client.WithHTTPClient(httpClient))
	require.NoError(t, err)

	var connection net.Conn
	ctx := httptrace.WithClientTrace(
		t.Context(),
		&httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) {
				connection = info.Conn
			},
		},
	)
	_, err = controller.Workers().Get(ctx, "worker")
	var apiError *client.APIError
	require.ErrorAs(t, err, &apiError)
	require.Equal(t, http.StatusTemporaryRedirect, apiError.StatusCode)
	require.EqualValues(t, 1, calls.Load())

	require.NotNil(t, connection)
	httpClient.CloseIdleConnections()
	_, err = connection.Write([]byte("closed"))
	require.ErrorIs(t, err, net.ErrClosed)
}

func TestWithHTTPClientSupportsWebSockets(t *testing.T) {
	server := httptest.NewTLSServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			connection, err := websocket.Accept(w, r, nil)
			if !assert.NoError(t, err) {
				return
			}
			defer connection.CloseNow()

			messageType, payload, err := connection.Read(r.Context())
			if !assert.NoError(t, err) {
				return
			}

			assert.NoError(t, connection.Write(r.Context(), messageType, payload))
		}),
	)
	t.Cleanup(server.Close)

	httpClient := server.Client()
	t.Cleanup(httpClient.CloseIdleConnections)

	controller, err := client.New(client.WithAddress(server.URL), client.WithHTTPClient(httpClient))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()

	connection, err := controller.Workers().PortForward(ctx, "worker", 22)
	require.NoError(t, err)
	defer connection.Close()

	_, err = connection.Write([]byte("hello"))
	require.NoError(t, err)

	payload := make([]byte, len("hello"))
	_, err = io.ReadFull(connection, payload)
	require.NoError(t, err)
	require.Equal(t, "hello", string(payload))
}

func TestWithHTTPClientNilKeepsDefaults(t *testing.T) {
	server := httptest.NewTLSServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"name":"worker"}`)
		}),
	)
	t.Cleanup(server.Close)

	controller, err := client.New(
		client.WithAddress(server.URL),
		client.WithHTTPClient(nil),
		client.WithTrustedCertificate(server.Certificate()),
	)
	require.NoError(t, err)

	worker, err := controller.Workers().Get(t.Context(), "worker")
	require.NoError(t, err)
	require.Equal(t, "worker", worker.Name)
}
