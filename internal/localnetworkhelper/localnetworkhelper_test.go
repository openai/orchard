//go:build unix

package localnetworkhelper //nolint:testpackage // The cancellation test synchronizes the private request handoff.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const helperTestTimeout = 5 * time.Second

func TestMain(m *testing.M) {
	if len(os.Args) == 2 && os.Args[1] == CommandName {
		if err := Serve(3); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		os.Exit(0)
	}

	os.Exit(m.Run())
}

func TestPrivilegedDialAcrossSocketReads(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), helperTestTimeout)
	t.Cleanup(cancel)

	helper, err := New(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, helper.Close()) })

	// The request spans the old 4 KiB read boundary. An unknown network keeps the error reply short.
	_, err = helper.PrivilegedDialContext(ctx, "unknown", strings.Repeat("x", 8*1024))
	require.ErrorContains(t, err, "unknown network unknown")

	transport := &http.Transport{DialContext: helper.PrivilegedDialContext}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{
		Transport: transport,
		Timeout:   helperTestTimeout,
	}

	for _, body := range []string{"first", "second"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, body)
		}))
		t.Cleanup(server.Close)

		t.Run(body, func(t *testing.T) {
			t.Parallel()

			request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
			require.NoError(t, err)

			response, err := client.Do(request)
			require.NoError(t, err)
			defer response.Body.Close()

			data, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Equal(t, body, string(data))
		})
	}
}

func TestPrivilegedDialCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), helperTestTimeout)
	defer cancel()

	helper := &LocalNetworkHelper{requestsUnprocessed: make(chan dialRequest)}
	done := make(chan error, 1)

	go func() {
		_, err := helper.PrivilegedDialContext(ctx, "tcp", "127.0.0.1:1")
		done <- err
	}()

	// Cancel after the request has been handed off, while the caller is waiting for its reply.
	select {
	case <-helper.requestsUnprocessed:
		cancel()
	case <-ctx.Done():
		t.Fatal("dial request was not dispatched")
	}

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(helperTestTimeout):
		t.Fatal("dial did not return after cancellation")
	}
}
