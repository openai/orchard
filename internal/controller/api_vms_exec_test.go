//nolint:testpackage // we need to test unexported exec helpers
package controller

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cirruslabs/orchard/internal/controller/sshexec"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestParseExecInteractive(t *testing.T) {
	for _, test := range []struct {
		name        string
		query       string
		interactive bool
		errContains string
	}{
		{
			name:        "default false",
			interactive: false,
		},
		{
			name:        "interactive true",
			query:       "interactive=true",
			interactive: true,
		},
		{
			name:        "stdin alias true",
			query:       "stdin=true",
			interactive: true,
		},
		{
			name:        "matching values accepted",
			query:       "interactive=true&stdin=true",
			interactive: true,
		},
		{
			name:        "conflicting values rejected",
			query:       "interactive=true&stdin=false",
			errContains: "cannot conflict",
		},
		{
			name:        "invalid interactive rejected",
			query:       "interactive=maybe",
			errContains: "interactive",
		},
		{
			name:        "invalid stdin rejected",
			query:       "stdin=maybe",
			errContains: "stdin",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			interactive, err := parseExecInteractive(execQueryContext(test.query))
			if test.errContains != "" {
				require.ErrorContains(t, err, test.errContains)

				return
			}

			require.NoError(t, err)
			require.Equal(t, test.interactive, interactive)
		})
	}
}

func TestParseExecOptions(t *testing.T) {
	options, runCommand, err := parseExecOptions(
		execQueryContext("interactive=true&tty=true&rows=24&cols=80&env[GREETING]=hello&workdir=/tmp"),
		"printf '%s' \"$GREETING\"",
	)
	require.NoError(t, err)
	require.Equal(t, sshexec.Options{
		Interactive: true,
		TTY:         true,
		Rows:        24,
		Cols:        80,
		Env:         map[string]string{"GREETING": "hello"},
		Workdir:     "/tmp",
	}, options)
	require.Equal(t, "cd '/tmp' || exit $?\nexport GREETING='hello'\nprintf '%s' \"$GREETING\"", runCommand)
}

func TestParseExecOptionsRejectsPartialTTYSize(t *testing.T) {
	_, _, err := parseExecOptions(execQueryContext("tty=true&rows=24"), "echo hello")
	require.ErrorContains(t, err, "provided together")
}

func execQueryContext(query string) *gin.Context {
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = httptest.NewRequest(http.MethodGet, "/?"+query, nil)

	return ctx
}
