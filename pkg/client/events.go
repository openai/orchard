package client

import (
	"context"
	"io"
	"net/http"
	"sync"

	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/hashicorp/go-multierror"
)

type EventStreamer struct {
	client        *Client
	endpoint      string
	eventsChannel chan v1.Event
	done          chan struct{}
	closeOnce     sync.Once
	sendErr       error
	io.Closer
}

func NewEventStreamer(client *Client, endpoint string) *EventStreamer {
	streamer := &EventStreamer{
		client:        client,
		endpoint:      endpoint,
		eventsChannel: make(chan v1.Event, 64),
		done:          make(chan struct{}),
	}
	go streamer.stream()
	return streamer
}

func (streamer *EventStreamer) Stream(event v1.Event) {
	streamer.eventsChannel <- event
}

func (streamer *EventStreamer) stream() {
	defer close(streamer.done)
	ctx := context.Background()

	for {
		events, finished := streamer.readAvailableEvents()
		if len(events) > 0 {
			err := streamer.client.request(ctx, http.MethodPost, streamer.endpoint, events, nil, nil)
			if err != nil {
				streamer.sendErr = multierror.Append(streamer.sendErr, err)
			}
		}
		if finished {
			break
		}
	}
}

func (streamer *EventStreamer) readAvailableEvents() ([]v1.Event, bool) {
	var result []v1.Event

	// blocking wait for at least one event
	nextEvent, ok := <-streamer.eventsChannel
	if !ok {
		return result, true
	}
	result = append(result, nextEvent)

	// non-blocking wait for more events, if any
	for {
		select {
		case nextEvent, ok := <-streamer.eventsChannel:
			if !ok {
				return result, true
			}
			result = append(result, nextEvent)
		default:
			return result, false
		}
	}
}

// Close waits for queued events to be sent and returns all send errors.
// Callers must stop calling Stream before closing the streamer.
func (streamer *EventStreamer) Close() error {
	streamer.closeOnce.Do(func() { close(streamer.eventsChannel) })
	<-streamer.done
	return streamer.sendErr
}
