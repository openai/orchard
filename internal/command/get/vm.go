package get

import (
	"fmt"
	"strings"
	"time"

	"github.com/cirruslabs/orchard/internal/structpath"
	"github.com/cirruslabs/orchard/pkg/client"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/dustin/go-humanize"
	"github.com/gosuri/uitable"
	"github.com/samber/lo"
	"github.com/spf13/cobra"
)

func newGetVMCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "vm NAME",
		Short: "Retrieve a VM and it's fields",
		RunE:  runGetVM,
		Args:  cobra.ExactArgs(1),
	}

	return command
}

func runGetVM(cmd *cobra.Command, args []string) error {
	name := args[0]

	client, err := client.New()
	if err != nil {
		return err
	}

	// Ability to retrieve resource fields (e.g. "orchard get vm macos/status")
	splits := strings.Split(name, "/")
	var path []string
	if len(splits) > 1 {
		name = splits[0]
		path = splits[1:]
	}

	vm, err := client.VMs().Get(cmd.Context(), name)
	if err != nil {
		return err
	}

	// Ability to retrieve resource fields (e.g. "orchard get vm macos/status")
	if len(path) != 0 {
		result, ok := structpath.Lookup(*vm, path)
		if !ok {
			return fmt.Errorf("%w: failed to find the specified field \"%s\" or the field is not a string",
				ErrGetFailed, strings.Join(path, "/"))
		}

		fmt.Println(result)

		return nil
	}

	table := uitable.New()
	table.Wrap = true

	table.AddRow("Name", vm.Name)
	createdAtInfo := humanize.RelTime(vm.CreatedAt, time.Now(), "ago", "in the future")
	table.AddRow("Created", createdAtInfo)
	table.AddRow("Image", vm.Image)
	table.AddRow("Image pull policy", vm.ImagePullPolicy)

	cpu := vm.CPU
	if cpu == 0 {
		// Implicit CPU assignment, CPU will always be 0
		cpu = vm.AssignedCPU
	}
	table.AddRow("CPU", cpu)

	memory := vm.Memory
	if memory == 0 {
		// Implicit memory assignment, memory will always be 0
		memory = vm.AssignedMemory
	}
	table.AddRow("Memory", memory)

	var diskSize string

	if vm.DiskSize != 0 {
		diskSize = fmt.Sprintf("%d GB", vm.DiskSize)
	} else {
		diskSize = "VM image's default"
	}

	table.AddRow("Disk size", diskSize)

	table.AddRow("Softnet enabled", vm.NetSoftnetDeprecated || vm.NetSoftnet)
	table.AddRow("Softnet allowed CIDRs", strings.Join(vm.NetSoftnetAllow, "\n"))
	table.AddRow("Softnet blocked CIDRs", strings.Join(vm.NetSoftnetBlock, "\n"))
	table.AddRow("Bridged networking interface", nonEmptyOrNone(vm.NetBridged))
	table.AddRow("Headless mode", vm.Headless)
	table.AddRow("Nested virtualization", vm.Nested)
	table.AddRow("Audio disabled", vm.NoAudio)
	table.AddRow("Clipboard sharing disabled", vm.NoClipboard)
	table.AddRow("Status", vm.Status)
	table.AddRow("Status message", vm.StatusMessage)
	table.AddRow("Assigned worker", nonEmptyOrNone(vm.Worker))

	table.AddRow("Restart policy", vm.RestartPolicy)
	restartedAtInfo := "never"
	if !vm.RestartedAt.IsZero() {
		restartedAtInfo = humanize.RelTime(vm.RestartedAt, time.Now(), "ago", "in the future")
	}
	table.AddRow("Restarted", restartedAtInfo)
	table.AddRow("Restart count", vm.RestartCount)

	resourcesInfo := strings.Join(lo.MapToSlice(vm.Resources, func(key string, value uint64) string {
		return fmt.Sprintf("%s: %d", key, value)
	}), "\n")
	table.AddRow("Resources", nonEmptyOrNone(resourcesInfo))

	labelsInfo := strings.Join(lo.MapToSlice(vm.Labels, func(key string, value string) string {
		return fmt.Sprintf("%s: %s", key, value)
	}), "\n")
	table.AddRow("Labels", nonEmptyOrNone(labelsInfo))

	table.AddRow("Random serial", vm.RandomSerial)

	var hostDirsInfo string
	if len(vm.HostDirs) != 0 {
		hostDirsDescriptions := lo.Map(vm.HostDirs, func(hostDir v1.HostDir, index int) string {
			return hostDir.String()
		})
		hostDirsInfo = strings.Join(hostDirsDescriptions, "\n")
	}
	table.AddRow("Host directories", nonEmptyOrNone(hostDirsInfo))

	fmt.Println(table)

	return nil
}
