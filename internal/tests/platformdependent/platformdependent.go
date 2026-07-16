package platformdependent

import (
	"context"
	"runtime"

	"github.com/cirruslabs/orchard/internal/imageconstant"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager/tart"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager/vetu"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"go.uber.org/zap"
)

func VM(name string) *v1.VM {
	vm := &v1.VM{
		Meta: v1.Meta{
			Name: name,
		},
		Image:    imageconstant.DefaultMacosImage,
		CPU:      4,
		Memory:   8 * 1024,
		Headless: true,
	}

	if runtime.GOOS == "linux" {
		vm.Image = imageconstant.DefaultLinuxImage
		vm.OS = v1.OSLinux
		vm.Arch = v1.Architecture(runtime.GOARCH)
		vm.Runtime = v1.RuntimeVetu
	}

	return vm
}

func CloneDefaultImage(ctx context.Context, logger *zap.SugaredLogger, destination string) error {
	var err error

	if runtime.GOOS == "linux" {
		_, _, err = vetu.Vetu(ctx, logger, "clone", imageconstant.DefaultLinuxImage, destination)
	} else {
		_, _, err = tart.Tart(ctx, logger, "clone", imageconstant.DefaultMacosImage, destination)
	}

	return err
}

func ListVMs(ctx context.Context, logger *zap.SugaredLogger) ([]vmmanager.VMInfo, error) {
	var vms []vmmanager.VMInfo
	var err error

	if runtime.GOOS == "linux" {
		vms, err = vetu.List(ctx, logger)
	} else {
		vms, err = tart.List(ctx, logger)
	}

	return vms, err
}
