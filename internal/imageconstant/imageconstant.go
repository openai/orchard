package imageconstant

import (
	"errors"
	"fmt"

	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
)

var ErrUnsupportedPlatform = errors.New("unsupported platform")

const (
	DefaultMacosImage      = "ghcr.io/cirruslabs/macos-tahoe-base:latest"
	DefaultLinuxAMD64Image = "ghcr.io/cirruslabs/ubuntu-amd64:24.04"
	DefaultLinuxARM64Image = "ghcr.io/cirruslabs/ubuntu:24.04"
)

func DefaultImage(operatingSystem v1.OS, architecture v1.Architecture) (string, error) {
	switch operatingSystem {
	case v1.OSDarwin:
		if architecture != v1.ArchitectureARM64 {
			return "", fmt.Errorf(
				"%w: %s/%s",
				ErrUnsupportedPlatform,
				operatingSystem,
				architecture,
			)
		}

		return DefaultMacosImage, nil
	case v1.OSLinux:
		switch architecture {
		case v1.ArchitectureAMD64:
			return DefaultLinuxAMD64Image, nil
		case v1.ArchitectureARM64:
			return DefaultLinuxARM64Image, nil
		default:
			return "", fmt.Errorf(
				"%w: %s/%s",
				ErrUnsupportedPlatform,
				operatingSystem,
				architecture,
			)
		}
	default:
		return "", fmt.Errorf("%w: %s/%s", ErrUnsupportedPlatform, operatingSystem, architecture)
	}
}
