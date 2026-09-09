//nolint:testpackage // exercise the internal output helper
package base

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScriptOutput(t *testing.T) {
	atLimit := strings.Repeat("x", scriptOutputBufferSize)
	belowLimit := atLimit[:len(atLimit)-1]
	oversized := atLimit + atLimit
	half := atLimit[:len(atLimit)/2]

	testCases := []struct {
		name        string
		chunks      []string
		want        []string
		beforeFlush int // Number of lines emitted before flushing the final tail
		stopped     bool
	}{
		{name: "empty"},
		{
			name: "split lines and blanks", chunks: []string{"hel", "", "lo\n\nwor", "ld\nlast"},
			want: []string{"hello", "", "world", "last"}, beforeFlush: 3,
		},
		{
			name: "split CRLF and trailing CR", chunks: []string{"first\r", "\none\rtwo\r", "\nlast\r"},
			want: []string{"first", "one\rtwo", "last"}, beforeFlush: 2,
		},
		{
			name: "strip only one CR", chunks: []string{"line\r\r\nlast\r\r"},
			want: []string{"line\r", "last\r"}, beforeFlush: 1,
		},
		{name: "only CR", chunks: []string{"\r"}, want: []string{""}},
		{
			name: "binary and split UTF8", chunks: []string{"\x00\xff\x80\nhello \xf0", "\x9f\x8c", "\x8d\n"},
			want: []string{"\x00\xff\x80", "hello 🌍"}, beforeFlush: 2,
		},
		{name: "unfinished below limit", chunks: []string{belowLimit}, want: []string{belowLimit}},
		{
			name: "complete at limit", chunks: []string{atLimit + "\n"},
			want: []string{atLimit}, beforeFlush: 1,
		},
		{
			name: "unfinished reaches limit", chunks: []string{"ok\n", belowLimit, "x", "\nignored\n"},
			want: []string{"ok"}, beforeFlush: 1, stopped: true,
		},
		{name: "unfinished overshoots limit", chunks: []string{oversized}, stopped: true},
		{
			name: "large complete line with tail", chunks: []string{oversized + "\r\nnext\npar", "tial"},
			want: []string{oversized, "next", "partial"}, beforeFlush: 2,
		},
		{
			name: "buffered prefix completes over limit", chunks: []string{belowLimit, "xx\r\nnext\n"},
			want: []string{atLimit + "x", "next"}, beforeFlush: 2,
		},
		{
			name: "large chunk with multiple lines", chunks: []string{strings.Repeat(half+"\n", 3)},
			want: []string{half, half, half}, beforeFlush: 3,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Capture emitted lines
			var got []string
			output := newScriptOutput(func(line string) { got = append(got, line) })

			// Write chunks and simulate input buffer reuse
			for _, chunk := range testCase.chunks {
				data := []byte(chunk)
				output.write(data)
				clear(data)
			}

			// Ensure complete lines are emitted before flushing the final tail
			require.Len(t, got, testCase.beforeFlush)
			require.Equal(t, testCase.stopped, output.stopped)

			// Ensure stopped output ignores further writes
			if testCase.stopped {
				buffered, capacity := output.pending.Len(), output.pending.Cap()
				output.write([]byte("ignored\n"))
				require.Equal(t, buffered, output.pending.Len())
				require.Equal(t, capacity, output.pending.Cap())
			}

			// Flush the final tail and verify the result
			output.flush()
			require.Equal(t, testCase.want, got)
			require.Zero(t, output.pending.Len())
			require.Equal(t, testCase.stopped, output.stopped)

			// Ensure flushing again does not duplicate output
			output.flush()
			require.Equal(t, testCase.want, got, "flushing twice must not duplicate output")
		})
	}
}
