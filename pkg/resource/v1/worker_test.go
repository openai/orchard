package v1_test

import (
	"testing"

	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
)

func TestValidateWorkerAddress(t *testing.T) {
	validAddresses := []string{
		"192.168.1.10", "fd00::1", "::1", "worker-1", "worker-1.example.com", "worker-1.example.com.",
	}

	for _, address := range validAddresses {
		require.NoError(t, v1.ValidateWorkerAddress(address), address)
	}

	invalidAddresses := []string{
		"", "192.168.1.10:22", "[fd00::1]", "[fd00::1]:22", "worker-1:22", "http://worker-1", "worker 1",
		"user@worker-1", "foo%bar", "-worker", "worker-", "worker..example.com", ".",
	}

	for _, address := range invalidAddresses {
		require.ErrorIs(t, v1.ValidateWorkerAddress(address), v1.ErrInvalidWorkerAddress, address)
	}
}
