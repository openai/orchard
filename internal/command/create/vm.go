package create

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/cirruslabs/orchard/internal/imageconstant"
	"github.com/cirruslabs/orchard/internal/simplename"
	"github.com/cirruslabs/orchard/pkg/client"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/spf13/cobra"
)

var ErrVMFailed = errors.New("failed to create VM")

var image string
var vmOSRaw string
var vmArchRaw string
var vmRuntimeRaw string
var cpu uint64
var memory uint64
var diskSize uint64
var netSoftnet bool
var netSoftnetAllow []string
var netSoftnetBlock []string
var netBridged string
var headless bool
var nested bool
var noAudio bool
var suspendable bool
var username string
var password string
var resources map[string]string
var labels map[string]string
var randomSerial bool
var restartPolicy string
var startupScript string
var hostDirsRaw []string
var imagePullPolicy string

func newCreateVMCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "vm NAME",
		Short: "Create a VM",
		RunE:  runCreateVM,
		Args:  cobra.ExactArgs(1),
	}

	command.Flags().StringVar(&image, "image", imageconstant.DefaultMacosImage, "image to use")
	command.Flags().StringVar(&vmOSRaw, "os", string(v1.OSDarwin), fmt.Sprintf("operating system of this "+
		"VM: %q or %q; set to \"linux\" to work around the Apple's limitation of 2 macOS VMs per host when using "+
		"\"tart\" runtime", v1.OSDarwin, v1.OSLinux))
	command.Flags().StringVar(&vmArchRaw, "arch", string(v1.ArchitectureARM64), fmt.Sprintf("architecture "+
		"of this VM: %q or %q; ensures the VM is scheduled on an architecture-compatible machine in mixed-architecture "+
		"clusters", v1.ArchitectureARM64, v1.ArchitectureAMD64))
	command.Flags().StringVar(&vmRuntimeRaw, "runtime", string(v1.RuntimeTart), fmt.Sprintf("runtime to use "+
		"for this VM: %q or %q; ensures the VM is scheduled on a runtime-compatible node", v1.RuntimeTart, v1.RuntimeVetu))
	command.Flags().Uint64Var(&cpu, "cpu", 4, "number of CPUs to use")
	command.Flags().Uint64Var(&memory, "memory", 8*1024, "megabytes of memory to use")
	command.Flags().Uint64Var(&diskSize, "disk-size", 0, "resize the VMs disk to the specified size in GB "+
		"(no resizing is done by default and VM's image default size is used)")
	command.Flags().BoolVar(&netSoftnet, "net-softnet", false, "whether to use Softnet network isolation")
	command.Flags().StringSliceVar(&netSoftnetAllow, "net-softnet-allow", []string{},
		"comma-separated list of CIDRs to allow the traffic to when using Softnet isolation, see "+
			"\"tart run\"'s help for \"--net-softnet-block\" for more details; automatically enables --net-softnet")
	command.Flags().StringSliceVar(&netSoftnetBlock, "net-softnet-block", []string{},
		"comma-separated list of CIDRs to block the traffic to when using Softnet isolation, see "+
			"\"tart run\"'s help for \"--net-softnet-block\" for more details; automatically enables --net-softnet")
	command.Flags().StringVar(&netBridged, "net-bridged", "", "whether to use Bridged network mode")
	command.Flags().BoolVar(&headless, "headless", true, "whether to run without graphics")
	command.Flags().BoolVar(&nested, "nested", false, "enable nested virtualization")
	command.Flags().BoolVar(&noAudio, "no-audio", false, "disable audio pass-through to the host")
	command.Flags().BoolVar(&suspendable, "suspendable", false, "treat the VM as suspendable, "+
		"disabling certain devices for suspendability support and issuing \"tart suspend\" instead of \"tart stop\" "+
		"when VM's specification is updated, thus preserving the VM's state between specification generations")
	command.Flags().StringVar(&username, "username", "admin",
		"SSH username to use when executing a startup script on the VM")
	command.Flags().StringVar(&password, "password", "admin",
		"SSH password to use when executing a startup script on the VM")
	command.Flags().StringToStringVar(&resources, "resources", map[string]string{},
		"resources to request for this VM")
	command.Flags().StringToStringVar(&labels, "labels", map[string]string{},
		"labels required by this VM")
	command.Flags().BoolVar(&randomSerial, "random-serial", false,
		"generate a new random serial number if this is a macOS VM (no-op for Linux VMs)")
	command.Flags().StringVar(&restartPolicy, "restart-policy", string(v1.RestartPolicyNever),
		fmt.Sprintf("restart policy for this VM: specify %q to never restart or %q "+
			"to only restart when the VM fails", v1.RestartPolicyNever, v1.RestartPolicyOnFailure))
	command.Flags().StringVar(&startupScript, "startup-script", "",
		"startup script (e.g. --startup-script=\"sync\") or a path to a script file prefixed with \"@\" "+
			"(e.g. \"--startup-script=@script.sh\")")
	command.Flags().StringSliceVar(&hostDirsRaw, "host-dirs", []string{},
		"directories on the Orchard Worker host to mount to a VM, can be specified multiple times "+
			"and/or be comma-separated (see \"tart run\"'s --dir argument for syntax)")
	command.Flags().StringVar(&imagePullPolicy, "image-pull-policy", string(v1.ImagePullPolicyIfNotPresent),
		fmt.Sprintf("image pull policy for this VM, by default the image is only pulled if it doesn't "+
			"exist in the cache (%q), specify %q to always try to pull the image",
			v1.ImagePullPolicyIfNotPresent, v1.ImagePullPolicyAlways))

	return command
}

func runCreateVM(cmd *cobra.Command, args []string) error {
	name := args[0]

	// Issue a warning if the name used will be invalid in the future
	if err := simplename.ValidateNext(name); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "WARNING: %v\n", err)
	}

	// Convert arguments
	var hostDirs []v1.HostDir

	vmOS, err := v1.NewOSFromString(vmOSRaw)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrVMFailed, err)
	}

	vmArch, err := v1.NewArchitectureFromString(vmArchRaw)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrVMFailed, err)
	}

	vmRuntime, err := v1.NewRuntimeFromString(vmRuntimeRaw)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrVMFailed, err)
	}

	for _, hostDirRaw := range hostDirsRaw {
		hostDir, err := v1.NewHostDirFromString(hostDirRaw)
		if err != nil {
			return err
		}

		hostDirs = append(hostDirs, hostDir)
	}

	vm := &v1.VM{
		Meta: v1.Meta{
			Name: name,
		},
		Image:    image,
		CPU:      cpu,
		Memory:   memory,
		DiskSize: diskSize,
		VMSpec: v1.VMSpec{
			OS:                   vmOS,
			Arch:                 vmArch,
			Runtime:              vmRuntime,
			NetSoftnetDeprecated: netSoftnet,
			NetSoftnet:           netSoftnet,
			NetSoftnetAllow:      netSoftnetAllow,
			NetSoftnetBlock:      netSoftnetBlock,
			Suspendable:          suspendable,
		},
		NetBridged:   netBridged,
		Headless:     headless,
		Nested:       nested,
		NoAudio:      noAudio,
		Username:     username,
		Password:     password,
		RandomSerial: randomSerial,
		Labels:       labels,
		HostDirs:     hostDirs,
	}

	if err := vm.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrVMFailed, err)
	}

	// Convert resources
	vm.Resources, err = v1.NewResourcesFromStringToString(resources)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrVMFailed, err)
	}

	// Convert image pull policy
	vm.ImagePullPolicy, err = v1.NewImagePullPolicyFromString(imagePullPolicy)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrVMFailed, err)
	}

	// Convert restart policy
	vm.RestartPolicy, err = v1.NewRestartPolicyFromString(restartPolicy)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrVMFailed, err)
	}

	// Convert startup script, optionally reading it from the file system
	const scriptFilePrefix = "@"

	if strings.HasPrefix(startupScript, scriptFilePrefix) {
		startupScriptBytes, err := os.ReadFile(strings.TrimPrefix(startupScript, scriptFilePrefix))
		if err != nil {
			return err
		}

		vm.StartupScript = &v1.VMScript{
			ScriptContent: string(startupScriptBytes),
		}
	} else if startupScript != "" {
		vm.StartupScript = &v1.VMScript{
			ScriptContent: startupScript,
		}
	}

	client, err := client.New()
	if err != nil {
		return err
	}

	return client.VMs().Create(cmd.Context(), vm)
}
