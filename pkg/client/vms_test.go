package client

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestHTTPClientForWebSocketHonorsWait(t *testing.T) {
	devClient, err := New(WithAddress("http://localhost"))
	require.NoError(t, err)

	httpClient := devClient.httpClientForWebSocket(map[string]string{waitParameterName: "120"})

	require.Equal(t, 150*time.Second, httpClient.Timeout)
	require.Same(t, devClient.httpClient.Transport, httpClient.Transport)
}

func TestExecSessionBuildsReconnectableQuery(t *testing.T) {
	var query map[string][]string

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			query = request.URL.Query()

			conn, err := websocket.Accept(writer, request, nil)
			if err != nil {
				t.Errorf("failed to accept WebSocket connection: %v", err)

				return
			}

			defer conn.CloseNow()
		}),
	)
	defer server.Close()

	devClient, err := New(WithAddress(server.URL))
	require.NoError(t, err)

	conn, err := devClient.VMs().ExecSession(t.Context(), "vm", ExecSessionOptions{
		Command:     "echo hello",
		Interactive: true,
		TTY:         true,
		Rows:        24,
		Cols:        80,
		Env:         map[string]string{"GREETING": "hello"},
		Workdir:     "/tmp",
		WaitSeconds: 7,
		Session:     "resume-me",
	})
	require.NoError(t, err)
	defer conn.CloseNow()

	require.Equal(t, []string{"echo hello"}, query["command"])
	require.Equal(t, []string{"true"}, query["interactive"])
	require.Equal(t, []string{"true"}, query["tty"])
	require.Equal(t, []string{"24"}, query["rows"])
	require.Equal(t, []string{"80"}, query["cols"])
	require.Equal(t, []string{"hello"}, query["env[GREETING]"])
	require.Equal(t, []string{"/tmp"}, query["workdir"])
	require.Equal(t, []string{"7"}, query[waitParameterName])
	require.Equal(t, []string{"resume-me"}, query["session"])
}
