package execstream

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFrameRoundTripsTerminalSize(t *testing.T) {
	frame := Frame{
		Type: FrameTypeResize,
		Terminal: &TerminalSize{
			Rows: 24,
			Cols: 80,
		},
	}

	payload, err := json.Marshal(frame)
	require.NoError(t, err)

	var decoded Frame
	err = json.Unmarshal(payload, &decoded)
	require.NoError(t, err)
	require.Equal(t, frame, decoded)
}
