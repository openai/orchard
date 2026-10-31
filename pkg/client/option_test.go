package client_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cirruslabs/orchard/pkg/client"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const requestTimeout = 5 * time.Second

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
