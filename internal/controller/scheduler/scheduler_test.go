package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorkerFailureChecksEnabled(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	offlineTimeout := 3 * time.Minute

	testCases := []struct {
		name      string
		startedAt time.Time
		enabled   bool
	}{
		{
			name:    "scheduler-not-started",
			enabled: true,
		},
		{
			name:      "scheduler-just-started",
			startedAt: now,
		},
		{
			name:      "before-offline-timeout",
			startedAt: now.Add(-offlineTimeout + time.Second),
		},
		{
			name:      "at-offline-timeout",
			startedAt: now.Add(-offlineTimeout),
		},
		{
			name:      "after-offline-timeout",
			startedAt: now.Add(-offlineTimeout - time.Second),
			enabled:   true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			scheduler := Scheduler{
				workerOfflineTimeout: offlineTimeout,
				startedAt:            testCase.startedAt,
			}

			require.Equal(t, testCase.enabled, scheduler.workerFailureChecksEnabled(now))
		})
	}
}
