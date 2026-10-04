package client_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/cirruslabs/orchard/pkg/client"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
)

func TestEventStreamerCloseDrainsPendingEvents(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			var mutex sync.Mutex
			var received []v1.Event
			emptyRequests := 0
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var events []v1.Event
				if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
					t.Errorf("decode events: %v", err)
				}
				mutex.Lock()
				requests++
				first := requests == 1
				if len(events) == 0 {
					emptyRequests++
				}
				received = append(received, events...)
				mutex.Unlock()
				if first {
					close(started)
					<-release
				}
				w.WriteHeader(status)
			}))
			defer server.Close()
			defer unblock()
			api, err := client.New(client.WithAddress(server.URL))
			require.NoError(t, err)
			streamer := api.VMs().StreamEvents("vm")
			expected := []v1.Event{
				{Kind: v1.EventKindLogLine, Payload: "first"},
				{Kind: v1.EventKindLogLine, Payload: "second"},
				{Kind: v1.EventKindLogLine, Payload: "last"},
			}
			streamer.Stream(expected[0])
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("first event was not sent")
			}
			streamer.Stream(expected[1])
			streamer.Stream(expected[2])
			closed := make(chan error, 1)
			go func() { closed <- streamer.Close() }()
			select {
			case err := <-closed:
				t.Fatalf("Close returned before the pending request completed: %v", err)
			case <-time.After(25 * time.Millisecond):
			}
			unblock()
			select {
			case err := <-closed:
				if status == http.StatusOK {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, client.ErrAPI)
					require.Contains(t, err.Error(), "2 errors occurred")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not finish after requests completed")
			}
			mutex.Lock()
			defer mutex.Unlock()
			require.Equal(t, expected, received)
			require.Zero(t, emptyRequests)
		})
	}
}

func TestEventStreamerCloseWithoutEvents(t *testing.T) {
	requests := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	api, err := client.New(client.WithAddress(server.URL))
	require.NoError(t, err)
	streamer := api.VMs().StreamEvents("vm")
	require.NoError(t, streamer.Close())
	select {
	case <-requests:
		t.Fatal("an empty event stream sent a request")
	default:
	}
}

func TestEventStreamerConcurrentClose(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	api, err := client.New(client.WithAddress(server.URL))
	require.NoError(t, err)
	streamer := api.VMs().StreamEvents("vm")
	streamer.Stream(v1.Event{Kind: v1.EventKindLogLine, Payload: "last"})
	var group sync.WaitGroup
	for range 4 {
		group.Go(func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Errorf("Close panicked: %v", recovered)
				}
			}()
			if err := streamer.Close(); err == nil {
				t.Error("Close lost the send error")
			}
		})
	}
	group.Wait()
}
