//nolint:testpackage // Inspect effective deadlines using the configured client transport.
package client

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type ipWaitTransport func(*http.Request) (*http.Response, error)

func (transport ipWaitTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestIPRequestHonorsWaitBudget(t *testing.T) {
	for _, test := range []struct {
		name    string
		wait    uint16
		timeout time.Duration
		want    time.Duration
	}{
		{name: "long-wait", wait: 120, timeout: defaultHTTPTimeout, want: 150 * time.Second},
		{name: "maximum-wait", wait: 65535, timeout: defaultHTTPTimeout, want: 65565 * time.Second},
		{name: "no-wait", timeout: defaultHTTPTimeout, want: defaultHTTPTimeout},
		{name: "longer-client-timeout", wait: 120, timeout: 5 * time.Minute, want: 5 * time.Minute},
		{name: "disabled-client-timeout", wait: 120},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, err := New(WithAddress("http://localhost"))
			require.NoError(t, err)
			client.httpClient.Timeout = test.timeout
			var deadline time.Time
			var hasDeadline bool
			client.httpClient.Transport = ipWaitTransport(func(request *http.Request) (*http.Response, error) {
				require.Equal(t, "/v1/vms/synthetic-vm/ip", request.URL.Path)
				deadline, hasDeadline = request.Context().Deadline()
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(`{"ip":"192.0.2.7"}`)), Header: make(http.Header),
				}, nil
			})
			before := time.Now()
			ip, err := client.VMs().IP(t.Context(), "synthetic-vm", test.wait)
			after := time.Now()
			require.NoError(t, err)
			require.Equal(t, "192.0.2.7", ip)
			require.Equal(t, test.timeout, client.httpClient.Timeout, "per-request waits must not mutate the shared client")
			if test.want == 0 {
				require.False(t, hasDeadline)
			} else {
				require.True(t, hasDeadline)
				require.False(t, deadline.Before(before.Add(test.want)))
				require.False(t, deadline.After(after.Add(test.want)))
			}
		})
	}
}

func TestIPWaitPreservesEarlierCallerDeadline(t *testing.T) {
	client, err := New(WithAddress("http://localhost"))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	want, _ := ctx.Deadline()
	client.httpClient.Transport = ipWaitTransport(func(request *http.Request) (*http.Response, error) {
		got, ok := request.Context().Deadline()
		require.True(t, ok)
		require.Equal(t, want, got)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"ip":"192.0.2.7"}`)), Header: make(http.Header),
		}, nil
	})
	_, err = client.VMs().IP(ctx, "synthetic-vm", 120)
	require.NoError(t, err)
}
