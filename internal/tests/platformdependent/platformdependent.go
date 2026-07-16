package platformdependent

import (
	"context"
	"fmt"
	"runtime"

	"github.com/cirruslabs/orchard/internal/imageconstant"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager/tart"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager/vetu"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"go.uber.org/zap"
)

func VM(name string) *v1.VM {
	hostOS, hostArchitecture, err := hostPlatform()
	if err != nil {
		panic(err)
	}

	image, err := imageconstant.DefaultImage(hostOS, hostArchitecture)
	if err != nil {
		panic(err)
	}

	vm := &v1.VM{
		Meta: v1.Meta{
			Name: name,
		},
		Image:    image,
		CPU:      4,
		Memory:   8 * 1024,
		Headless: true,
	}

	if hostOS == v1.OSLinux {
		vm.OS = hostOS
		vm.Arch = hostArchitecture
		vm.Runtime = v1.RuntimeVetu
	}

	return vm
}

func CloneDefaultImage(ctx context.Context, logger *zap.SugaredLogger, destination string) error {
	hostOS, hostArchitecture, err := hostPlatform()
	if err != nil {
		return err
	}

	image, err := imageconstant.DefaultImage(hostOS, hostArchitecture)
	if err != nil {
		return err
	}

	switch hostOS {
	case v1.OSLinux:
		_, _, err = vetu.Vetu(ctx, logger, "clone", image, destination)
	case v1.OSDarwin:
		_, _, err = tart.Tart(ctx, logger, "clone", image, destination)
	default:
		return fmt.Errorf("%w: %q", imageconstant.ErrUnsupportedPlatform, hostOS)
	}

	return err
}

func ListVMs(ctx context.Context, logger *zap.SugaredLogger) ([]vmmanager.VMInfo, error) {
	hostOS, _, err := hostPlatform()
	if err != nil {
		return nil, err
	}

	switch hostOS {
	case v1.OSLinux:
		return vetu.List(ctx, logger)
	case v1.OSDarwin:
		return tart.List(ctx, logger)
	default:
		return nil, fmt.Errorf("%w: %q", imageconstant.ErrUnsupportedPlatform, hostOS)
	}
}

func hostPlatform() (v1.OS, v1.Architecture, error) {
	hostOS, err := v1.NewOSFromString(runtime.GOOS)
	if err != nil {
		return "", "", err
	}

	hostArchitecture, err := v1.NewArchitectureFromString(runtime.GOARCH)
	if err != nil {
		return "", "", err
	}

	return hostOS, hostArchitecture, nil
}
