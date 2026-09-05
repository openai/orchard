package v1_test

import (
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestVMValidateNetSoftnetExpose(t *testing.T) {
	for _, test := range []struct {
		name        string
		runtime     v1.Runtime
		expose      []string
		errIs       error
		errContains string
	}{
		{name: "tart valid", runtime: v1.RuntimeTart, expose: []string{"2222:22", "08080:80"}},
		{name: "tart nil", runtime: v1.RuntimeTart},
		{name: "tart same internal port", runtime: v1.RuntimeTart, expose: []string{"2222:22", "2223:22"}},
		{name: "empty runtime valid", expose: []string{"2222:22"}},
		{
			name:        "tart malformed",
			runtime:     v1.RuntimeTart,
			expose:      []string{"2222:22", "2222"},
			errIs:       v1.ErrInvalidExposedPort,
			errContains: "netSoftnetExpose",
		},
		{
			name:        "tart duplicate external port",
			runtime:     v1.RuntimeTart,
			expose:      []string{"2222:22", "02222:80"},
			errIs:       v1.ErrInvalidExposedPort,
			errContains: "external port 2222 is exposed more than once",
		},
		{
			name:        "vetu unsupported",
			runtime:     v1.RuntimeVetu,
			expose:      []string{"2222:22"},
			errContains: "does not support field \"netSoftnetExpose\"",
		},
		{
			name:        "vetu malformed reports unsupported field",
			runtime:     v1.RuntimeVetu,
			expose:      []string{"2222"},
			errContains: "does not support field \"netSoftnetExpose\"",
		},
		{name: "vetu nil", runtime: v1.RuntimeVetu},
	} {
		t.Run(test.name, func(t *testing.T) {
			var vm v1.VM
			vm.Runtime = test.runtime
			vm.NetSoftnetExpose = test.expose

			err := vm.Validate()

			if test.errContains == "" {
				require.NoError(t, err)

				return
			}

			if test.errIs != nil {
				require.ErrorIs(t, err, test.errIs)
			} else {
				// Unsupported field errors take precedence over parsing the entries
				require.NotErrorIs(t, err, v1.ErrInvalidExposedPort)
			}

			require.ErrorContains(t, err, test.errContains)
		})
	}
}

// Tart binds the Softnet ports while the worker binds one port per endpoint listener,
// so a VM cannot ask for more of them than its ranges hold.
func TestVMValidateEndpointReusesSoftnetPort(t *testing.T) {
	endpoint := func(name string, min uint16, max uint16) v1.EndpointSpec {
		return v1.EndpointSpec{
			Name:            name,
			Target:          v1.ConnectionTarget{VM: &v1.ConnectionTargetVM{Port: 22}},
			WorkerPortRange: &v1.PortRange{Min: min, Max: max},
		}
	}

	for _, test := range []struct {
		name        string
		expose      []string
		endpoints   []v1.EndpointSpec
		errContains string
	}{
		{name: "same port", endpoints: []v1.EndpointSpec{endpoint("ssh", 2222, 2222)},
			errContains: "worker port 2222 is taken by netSoftnetExpose or another endpoint"},
		{name: "other port", endpoints: []v1.EndpointSpec{endpoint("ssh", 2223, 2223)}},
		// a range with a port left over lets the worker avoid the Softnet ones
		{name: "range", endpoints: []v1.EndpointSpec{endpoint("ssh", 2222, 2299)}},
		{
			name:      "range fully exposed",
			expose:    []string{"2222:22", "2223:23"},
			endpoints: []v1.EndpointSpec{endpoint("ssh", 2222, 2223)},
			errContains: "every worker port in 2222-2223 is taken by netSoftnetExpose " +
				"or the other endpoints",
		},
		{name: "range with one port left", expose: []string{"2222:22", "2223:23"},
			endpoints: []v1.EndpointSpec{endpoint("ssh", 2222, 2224)}},
		// two listeners need two ports between them, wherever their ranges overlap
		{
			name:        "two endpoints, one port left",
			endpoints:   []v1.EndpointSpec{endpoint("ssh", 2222, 2223), endpoint("http", 2222, 2223)},
			errContains: "is taken by netSoftnetExpose or the other endpoints",
		},
		{name: "two endpoints, two ports left",
			endpoints: []v1.EndpointSpec{endpoint("ssh", 2222, 2224), endpoint("http", 2222, 2224)}},
		{name: "two endpoints, one pinned", expose: []string{"2299:22"},
			endpoints: []v1.EndpointSpec{endpoint("ssh", 2222, 2222), endpoint("http", 2222, 2223)}},
		// the worker works down the endpoints in the order they are declared, so a wide
		// range ahead of a pinned one takes the port that pinned one has to have
		{
			name: "wide range ahead of a pinned one",
			endpoints: []v1.EndpointSpec{endpoint("http", 2222, 2224),
				endpoint("ssh", 2223, 2223)},
			errContains: "worker port 2223 is taken by netSoftnetExpose or another endpoint",
		},
		{name: "pinned range ahead of a wide one", endpoints: []v1.EndpointSpec{
			endpoint("ssh", 2223, 2223), endpoint("http", 2222, 2224)}},
		{name: "no range", endpoints: []v1.EndpointSpec{{Name: "ssh",
			Target: v1.ConnectionTarget{VM: &v1.ConnectionTargetVM{Port: 22}}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			expose := test.expose
			if expose == nil {
				expose = []string{"2222:22"}
			}

			var vm v1.VM
			vm.Runtime = v1.RuntimeTart
			vm.NetSoftnetExpose = expose
			vm.Endpoints = test.endpoints

			err := vm.Validate()

			if test.errContains == "" {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, v1.ErrInvalidExposedPort)
			require.ErrorContains(t, err, test.errContains)
		})
	}
}
