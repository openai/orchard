package imageconstant_test

import (
	"testing"

	"github.com/cirruslabs/orchard/internal/imageconstant"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
)

func TestDefaultImage(t *testing.T) {
	tests := []struct {
		name         string
		os           v1.OS
		architecture v1.Architecture
		expected     string
	}{
		{
			name:         "macOS ARM64",
			os:           v1.OSDarwin,
			architecture: v1.ArchitectureARM64,
			expected:     imageconstant.DefaultMacosImage,
		},
		{
			name:         "Linux AMD64",
			os:           v1.OSLinux,
			architecture: v1.ArchitectureAMD64,
			expected:     imageconstant.DefaultLinuxAMD64Image,
		},
		{
			name:         "Linux ARM64",
			os:           v1.OSLinux,
			architecture: v1.ArchitectureARM64,
			expected:     imageconstant.DefaultLinuxARM64Image,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := imageconstant.DefaultImage(test.os, test.architecture)

			require.NoError(t, err)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestDefaultImageRejectsUnsupportedPlatforms(t *testing.T) {
	tests := []struct {
		name         string
		os           v1.OS
		architecture v1.Architecture
	}{
		{
			name:         "unsupported OS",
			os:           v1.OS("windows"),
			architecture: v1.ArchitectureAMD64,
		},
		{
			name:         "unsupported Linux architecture",
			os:           v1.OSLinux,
			architecture: v1.Architecture("riscv64"),
		},
		{
			name:         "unsupported macOS architecture",
			os:           v1.OSDarwin,
			architecture: v1.ArchitectureAMD64,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := imageconstant.DefaultImage(test.os, test.architecture)

			require.Error(t, err)
		})
	}
}
