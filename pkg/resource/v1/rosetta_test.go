package v1_test

import (
	"encoding/json"
	"testing"

	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
)

func TestRosettaSpecification(t *testing.T) {
	for _, tc := range []struct {
		name      string
		input     string
		wantError string
	}{
		{name: "omitted", input: `{}`},
		{name: "empty", input: `{"runtime":"vetu","rosetta":""}`},
		{name: "linux tart", input: `{"os":"linux","runtime":"tart","arch":"arm64","rosetta":"translation"}`},
		{name: "default runtime and arch", input: `{"os":"linux","rosetta":"translation"}`},
		{name: "macOS", input: `{"os":"darwin","rosetta":"translation"}`, wantError: "requires a Linux guest"},
		{name: "missing OS", input: `{"rosetta":"translation"}`, wantError: "requires a Linux guest"},
		{name: "vetu", input: `{"os":"linux","runtime":"vetu","rosetta":"translation"}`, wantError: "does not support field"},
		{name: "amd64", input: `{"os":"linux","arch":"amd64","rosetta":"translation"}`, wantError: "requires an arm64 guest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var vm v1.VM
			require.NoError(t, json.Unmarshal([]byte(tc.input), &vm))
			if tc.wantError != "" {
				require.ErrorContains(t, vm.Validate(), tc.wantError)
				return
			}
			require.NoError(t, vm.Validate())
			encoded, err := json.Marshal(vm)
			require.NoError(t, err)
			var reopened v1.VM
			require.NoError(t, json.Unmarshal(encoded, &reopened))
			require.Equal(t, vm.Rosetta, reopened.Rosetta)
			if vm.Rosetta == "" {
				require.NotContains(t, string(encoded), `"rosetta"`)
			}
		})
	}
}
